package main

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/e13815332/multiscan/internal/resolver"
	"github.com/e13815332/multiscan/v2/internal/pg"
	"github.com/nats-io/nats.go"
)

//go:embed web/*
var webFS embed.FS

var (
	store      *pg.Store
	nc         *nats.Conn
	mu         sync.Mutex
	sseClients = make(map[chan string]bool)
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	var err error
	pgPassword := os.Getenv("PG_PASSWORD")
	if pgPassword == "" {
		pgPassword = "multiscan" // default
	}
	store, err = pg.Connect("127.0.0.1", 5432, "multiscan", pgPassword, "multiscan")
	if err != nil {
		log.Fatalf("PG: %v", err)
	}

	nc, err = nats.Connect("nats://127.0.0.1:4222")
	if err != nil {
		log.Fatalf("NATS: %v", err)
	}

	// Clean up stale jobs from previous run
	store.DB.Exec(`UPDATE jobs SET status='cancelled' WHERE status='pending'`)
	store.DB.Exec(`DELETE FROM masscan_results WHERE job_id IN (SELECT id FROM jobs WHERE status != 'running')`)
	log.Println("[master] Cleaned up stale jobs and masscan_results")

	// Phase 1→2 watcher
	go watchPhase1To2()

	// Phase 2→done watcher
	go watchPhase2ToDone()

	// PG NOTIFY → SSE
	go pgNotifyListener()

	mux := http.NewServeMux()

	// API
	mux.HandleFunc("/api/job/create", handleCreateJob)
	mux.HandleFunc("/api/job/cancel", handleCancelJob)
	mux.HandleFunc("/api/job/delete", handleDeleteJob)
	mux.HandleFunc("/api/job/status", handleJobStatus)
	mux.HandleFunc("/api/job/results", handleResults)
	mux.HandleFunc("/api/job/results.csv", handleResultsCSV)
	mux.HandleFunc("/api/worker/list", handleWorkerList)
	mux.HandleFunc("/api/job/list", handleJobList)
	mux.HandleFunc("/api/stats", handleStats)

	// Dashboard SSE
	mux.HandleFunc("/api/events", handleSSE)

	// Web UI
	webSub, _ := fs.Sub(webFS, "web")
	mux.Handle("/", http.FileServer(http.FS(webSub)))

	log.Println("[master] Listening on :8801")
	log.Fatal(http.ListenAndServe(":8801", mux))
}

func handleCreateJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "method not allowed", 405)
		return
	}
	asnsStr := r.FormValue("asns")
	portsStr := r.FormValue("ports")
	mode := r.FormValue("mode")
	name := r.FormValue("name")

	var asns []string
	for _, a := range strings.Split(asnsStr, ",") {
		a = strings.TrimSpace(a)
		if a != "" {
			asns = append(asns, a)
		}
	}
	var ports []int
	for _, p := range strings.Split(portsStr, ",") {
		p = strings.TrimSpace(p)
		if port, err := strconv.Atoi(p); err == nil && port > 0 {
			ports = append(ports, port)
		}
	}
	if len(ports) == 0 {
		ports = []int{443}
	}
	if mode == "" {
		mode = "tls"
	}
	if name == "" {
		// Auto-generate: first 3 ASNs + count
		short := strings.Join(asns, "+")
		if len(short) > 40 {
			short = short[:40]
		}
		name = short
		if len(asns) > 3 {
			name = fmt.Sprintf("%s+等%d个", strings.Join(asns[:3], "+"), len(asns))
		}
	}

	jobID := fmt.Sprintf("j_%d", time.Now().UnixNano())
	store.CreateJob(jobID, name, mode, asnsStr, portsStr, 0)

	// Resolve ASN → CIDR async
	go func() {
		prefixMap, err := resolver.ResolveMultipleASNs(asns)
		if err != nil {
			log.Printf("ASN resolution failed: %v", err)
			store.DB.Exec(`UPDATE jobs SET status='failed' WHERE id=$1`, jobID)
			return
		}
		var allCIDRs []string
		for _, p := range prefixMap {
			allCIDRs = append(allCIDRs, p...)
		}
		log.Printf("Resolved %d CIDRs from %d ASNs", len(allCIDRs), len(asns))

		// Get online workers with masscan rates
		rows, _ := store.DB.Query(`SELECT id, masscan_rate FROM workers WHERE status='online'`)
		var workerIDs []string
		var workerRates []int
		if rows != nil {
			for rows.Next() {
				var id string
				var rate int
				rows.Scan(&id, &rate)
				workerIDs = append(workerIDs, id)
				workerRates = append(workerRates, rate)
			}
			rows.Close()
		}
		if len(workerIDs) == 0 {
			log.Printf("No online workers, aborting")
			return
		}

		store.DB.Exec(`UPDATE jobs SET total_workers=$1, status='running' WHERE id=$2`, len(workerIDs), jobID)

		// Split CIDRs weighted by masscan_rate
		// If all rates are 0 (legacy), fall back to equal split
		totalRate := 0
		for _, r := range workerRates {
			totalRate += r
		}
		if totalRate == 0 {
			totalRate = len(workerIDs) // fallback: equal weights
			for i := range workerRates {
				workerRates[i] = 1
			}
		}

		offset := 0
		for i, wid := range workerIDs {
			// Weighted chunk size
			chunk := len(allCIDRs) * workerRates[i] / totalRate
			if i == len(workerIDs)-1 {
				chunk = len(allCIDRs) - offset // last worker gets remainder
			}
			if chunk < 1 {
				chunk = 1
			}
			end := offset + chunk
			if end > len(allCIDRs) {
				end = len(allCIDRs)
			}
			cidrChunk := allCIDRs[offset:end]
			offset = end

			msg := map[string]interface{}{
				"job_id":   jobID,
				"cidrs":    cidrChunk,
				"ports":    ports,
				"max_rate": 0,
			}
			data, _ := json.Marshal(msg)
			nc.Publish("masscan."+jobID+"."+wid, data)
			log.Printf("Published Phase 1 chunk %d/%d (%d CIDRs, %.0f%%) for worker %s (rate=%d)",
				i+1, len(workerIDs), len(cidrChunk), float64(len(cidrChunk))/float64(len(allCIDRs))*100, wid, workerRates[i])
		}
	}()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"job_id": jobID, "status": "resolving"})
}

func watchPhase1To2() {
	for {
		time.Sleep(3 * time.Second)
		rows, _ := store.DB.Query(`SELECT id, done_workers, total_workers, phase FROM jobs WHERE status='running' AND phase='phase1'`)
		if rows == nil {
			continue
		}
		for rows.Next() {
			var id, phase string
			var done, total int
			rows.Scan(&id, &done, &total, &phase)

			if done > 0 && done >= total {
				log.Printf("Job %s Phase 1 → 2: all workers done (%d/%d)", id, done, total)
				store.PopulateWorkQueue(id)
				var itemCount int
				store.DB.QueryRow(`SELECT COUNT(*) FROM work_queue WHERE job_id=$1`, id).Scan(&itemCount)
				store.UpdateJobPhase(id, "phase2", itemCount)
				log.Printf("Job %s Phase 2: %d items in queue", id, itemCount)

				// Notify workers via NATS
				nc.Publish("phase2."+id, []byte(id))
			}
		}
		rows.Close()
	}
}

func watchPhase2ToDone() {
	for {
		time.Sleep(5 * time.Second)

		// Reset processing items from offline workers (crash recovery)
		store.DB.Exec(`UPDATE work_queue SET status='pending', worker=NULL
			WHERE status='processing'
			AND worker NOT IN (SELECT id FROM workers WHERE status='online')`)

		// Find jobs in phase2 that might be done
		rows, _ := store.DB.Query(`SELECT id FROM jobs WHERE status='running' AND phase IN ('phase2','phase2b')`)
		if rows == nil {
			continue
		}
		var jobIDs []string
		for rows.Next() {
			var id string
			rows.Scan(&id)
			jobIDs = append(jobIDs, id)
		}
		rows.Close()

		for _, jid := range jobIDs {
			var undone int
			store.DB.QueryRow(`SELECT COUNT(*) FROM work_queue WHERE job_id=$1 AND status != 'done'`, jid).Scan(&undone)

			// tls-ok with empty colo = unverified, waiting for Phase2b
			var unverified, verified int
			store.DB.QueryRow(`SELECT COUNT(*) FILTER (WHERE status='tls-ok' AND colo='') FROM results WHERE job_id=$1`, jid).Scan(&unverified)
			store.DB.QueryRow(`SELECT COUNT(*) FILTER (WHERE status='verified') FROM results WHERE job_id=$1`, jid).Scan(&verified)

			if undone == 0 && unverified > 0 {
				store.UpdateJobPhase(jid, "phase2b", unverified+verified)
				log.Printf("Job %s → phase2b: verifying %d/%d", jid, verified, unverified+verified)
				continue
			}

			if undone > 0 {
				continue
			}

			// All done: no undone work_queue, no unverified tls-ok
			var itemCount int
			store.DB.QueryRow(`SELECT COUNT(*) FROM work_queue WHERE job_id=$1`, jid).Scan(&itemCount)
			store.UpdateJobPhase(jid, "done", itemCount)
			log.Printf("Job %s fully complete (%d items, %d verified)", jid, itemCount, verified)

			store.DB.Exec(`DELETE FROM results WHERE job_id=$1 AND status='tls-ok'`, jid)
		}
	}
}

func pgNotifyListener() {
	_, err := store.DB.Exec("LISTEN progress")
	if err != nil {
		log.Printf("LISTEN failed: %v", err)
		return
	}
	for {
		// Poll-based notification (simpler than raw PG connection listening)
		time.Sleep(2 * time.Second)
		mu.Lock()
		for ch := range sseClients {
			select {
			case ch <- "ping":
			default:
			}
		}
		mu.Unlock()
	}
}

func handleSSE(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := make(chan string, 10)
	mu.Lock()
	sseClients[ch] = true
	mu.Unlock()

	defer func() {
		mu.Lock()
		delete(sseClients, ch)
		mu.Unlock()
	}()

	for {
		select {
		case <-r.Context().Done():
			return
		case msg := <-ch:
			if msg == "ping" {
				fmt.Fprintf(w, "data: {\"type\":\"heartbeat\"}\n\n")
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
			}
		}
	}
}

func handleCancelJob(w http.ResponseWriter, r *http.Request) {
	jobID := r.URL.Query().Get("id")
	store.CancelJob(jobID)
	nc.Publish("job.cancel."+jobID, []byte("cancel"))
	json.NewEncoder(w).Encode(map[string]string{"status": "cancelled"})
}

func handleDeleteJob(w http.ResponseWriter, r *http.Request) {
	jobID := r.URL.Query().Get("id")
	store.DeleteJob(jobID)
	json.NewEncoder(w).Encode(map[string]string{"status": "deleted"})
}

func handleJobStatus(w http.ResponseWriter, r *http.Request) {
	jobID := r.URL.Query().Get("id")
	done, total, _ := store.GetJobStats(jobID)

	var phase, status string
	store.DB.QueryRow(`SELECT phase, status FROM jobs WHERE id=$1`, jobID).Scan(&phase, &status)

	json.NewEncoder(w).Encode(map[string]interface{}{
		"job_id": jobID, "phase": phase, "status": status,
		"done": done, "total": total,
	})
}

func handleResults(w http.ResponseWriter, r *http.Request) {
	jobID := r.URL.Query().Get("id")
	rows, err := store.GetResults(jobID)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var ip, colo, country, asn string
		var port, delay int
		rows.Scan(&ip, &port, &colo, &country, &asn, &delay)
		results = append(results, map[string]interface{}{
			"ip": ip, "port": port, "colo": colo,
			"country": country, "asn": asn, "delay": delay,
		})
	}
	json.NewEncoder(w).Encode(results)
}

func handleResultsCSV(w http.ResponseWriter, r *http.Request) {
	jobID := r.URL.Query().Get("id")

	// Get job name for filename
	var jobName string
	store.DB.QueryRow(`SELECT name FROM jobs WHERE id=$1`, jobID).Scan(&jobName)
	if jobName == "" {
		jobName = jobID
	}

	rows, err := store.GetResults(jobID)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	defer rows.Close()

	w.Header().Set("Content-Type", "text/csv")
	ts := time.Now().Format("20060102")
	filename := fmt.Sprintf("multiscan_%s_%s.csv", sanitizeFilename(jobName), ts)
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filename))
	w.Write([]byte("ip,port,colo,country,asn,delay_ms\n"))
	for rows.Next() {
		var ip, colo, country, asn string
		var port, delay int
		rows.Scan(&ip, &port, &colo, &country, &asn, &delay)
		fmt.Fprintf(w, "%s,%d,%s,%s,%s,%d\n", ip, port, colo, country, asn, delay)
	}
}

func handleWorkerList(w http.ResponseWriter, r *http.Request) {
	rows, err := store.DB.Query(`SELECT w.id, w.name, w.cpu_count, w.memory_mb, w.max_concurrent, w.status, w.last_seen,
		COALESCE(h.activity, 'idle') as activity
		FROM workers w LEFT JOIN heartbeat h ON w.id = h.worker_id
		ORDER BY w.last_seen DESC`)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	defer rows.Close()

	var workers []map[string]interface{}
	for rows.Next() {
		var id, name, status, activity string
		var cpu, mem, conc int
		var lastSeen time.Time
		rows.Scan(&id, &name, &cpu, &mem, &conc, &status, &lastSeen, &activity)
		workers = append(workers, map[string]interface{}{
			"id":             id,
			"name":           name,
			"cpu_count":      cpu,
			"memory_mb":      mem,
			"max_concurrent": conc,
			"status":         status,
			"last_seen":      lastSeen.Format(time.RFC3339),
			"activity":       activity,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(workers)
}

func handleStats(w http.ResponseWriter, r *http.Request) {
	type stat struct {
		RunningJobs int `json:"running_jobs"`
		DoneJobs    int `json:"done_jobs"`
		TotalIPs    int `json:"total_ips"`
		Scanned     int `json:"scanned"`
		Pending     int `json:"pending"`
		CFHits      int `json:"cf_hits"`
		Verified    int `json:"verified"`
	}
	var s stat
	store.DB.QueryRow(`SELECT COUNT(*) FROM jobs WHERE status='running'`).Scan(&s.RunningJobs)
	store.DB.QueryRow(`SELECT COUNT(*) FROM jobs WHERE status NOT IN ('running','pending')`).Scan(&s.DoneJobs)

	// Only count running jobs' data (not historical)
	if s.RunningJobs > 0 {
		// Total: sum of total_items across running jobs (set at phase1→2 transition)
		store.DB.QueryRow(`SELECT COALESCE(SUM(total_items),0) FROM jobs WHERE status='running'`).Scan(&s.TotalIPs)
		// Phase1: count masscan_results; Phase2: count work_queue
		store.DB.QueryRow(`SELECT COALESCE(
			(SELECT COUNT(*) FROM masscan_results WHERE job_id IN (SELECT id FROM jobs WHERE status='running' AND phase='phase1')),
			0) + COALESCE(
			(SELECT COUNT(*) FROM work_queue WHERE job_id IN (SELECT id FROM jobs WHERE status='running' AND phase != 'phase1')),
			0)`).Scan(&s.Scanned)
		store.DB.QueryRow(`SELECT COALESCE(
			(SELECT COUNT(*) FILTER (WHERE status='pending') FROM work_queue WHERE job_id IN (SELECT id FROM jobs WHERE status='running')),
			0)`).Scan(&s.Pending)
		s.Scanned -= s.Pending
		store.DB.QueryRow(`SELECT COUNT(*) FILTER (WHERE status='tls-ok') FROM results WHERE job_id IN (SELECT id FROM jobs WHERE status='running')`).Scan(&s.CFHits)
		store.DB.QueryRow(`SELECT COUNT(*) FILTER (WHERE status='verified') FROM results WHERE job_id IN (SELECT id FROM jobs WHERE status='running')`).Scan(&s.Verified)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s)
}

func handleJobList(w http.ResponseWriter, r *http.Request) {
	rows, err := store.DB.Query(`SELECT id, name, mode, asns, ports, status, phase, 
		COALESCE((SELECT COUNT(*) FILTER (WHERE status!='pending') FROM work_queue WHERE work_queue.job_id=jobs.id),0) as done,
		COALESCE((SELECT COUNT(*) FROM work_queue WHERE work_queue.job_id=jobs.id),0) as total
		FROM jobs ORDER BY created_at DESC LIMIT 50`)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	defer rows.Close()

	var jobs []map[string]interface{}
	for rows.Next() {
		var id, name, mode, asns, ports, status, phase string
		var done, total int
		rows.Scan(&id, &name, &mode, &asns, &ports, &status, &phase, &done, &total)
		jobs = append(jobs, map[string]interface{}{
			"job_id": id, "name": name, "mode": mode, "asns": asns, "ports": ports,
			"status": status, "phase": phase, "done": done, "total": total,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jobs)
}

// sanitizeFilename replaces chars unsafe for filenames
func sanitizeFilename(s string) string {
	r := strings.NewReplacer(
		"/", "_", "\\", "_", ":", "_", "*", "_", "?", "_",
		"\"", "_", "<", "_", ">", "_", "|", "_", " ", "_",
	)
	return r.Replace(s)
}
