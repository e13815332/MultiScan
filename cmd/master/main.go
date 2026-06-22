package main

import (
	"embed"
	"encoding/json"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/e13815332/multiscan/internal/master"
	"github.com/e13815332/multiscan/internal/protocol"
	"github.com/e13815332/multiscan/internal/resolver"
)

//go:embed web/*
var webFS embed.FS

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.Println("[master] Multiscan Master starting...")

	store := master.NewWorkerStore()
	tasks := master.NewTaskStore()
	events := master.NewEventQueueStore()
	dashboard := master.NewDashboardHub(store, tasks)
	scheduler := master.NewTaskScheduler(store, tasks, events)
	handler := master.NewHandler(store, tasks, scheduler, dashboard)

	mux := http.NewServeMux()

	// Static files (embedded web panel)
	webSub, _ := fs.Sub(webFS, "web")
	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.FS(webSub))))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		data, err := webFS.ReadFile("web/index.html")
		if err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(data)
	})

	// WebSocket for Workers
	mux.HandleFunc("/api/worker/ws", handler.HandleWorkerWS)

	// WebSocket for dashboard
	mux.HandleFunc("/api/dashboard/ws", dashboard.HandleWS)

	// REST API: Worker list
	mux.HandleFunc("/api/worker/list", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(store.List())
	})

	// REST API: single worker
	mux.HandleFunc("/api/worker/get", func(w http.ResponseWriter, r *http.Request) {
		uuid := r.URL.Query().Get("uuid")
		if uuid == "" {
			http.Error(w, `{"error":"missing uuid"}`, http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		info := store.GetByUUID(uuid)
		if info == nil {
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
			return
		}
		json.NewEncoder(w).Encode(info)
	})

	// REST API: create task
	mux.HandleFunc("/api/task/create", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		name := r.FormValue("name")
		asnStr := r.FormValue("asns")
		portStr := r.FormValue("ports")
		maxRate, _ := strconv.Atoi(r.FormValue("max_rate"))
		totalIPs, _ := strconv.Atoi(r.FormValue("total_ips"))

		asns := strings.Split(asnStr, ",")
		var cleanASNs []string
		for _, a := range asns {
			a = strings.TrimSpace(a)
			if a != "" {
				cleanASNs = append(cleanASNs, a)
			}
		}
		portStrs := strings.Split(portStr, ",")
		var ports []int
		for _, p := range portStrs {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			port, _ := strconv.Atoi(p)
			if port > 0 {
				ports = append(ports, port)
			}
		}
		if maxRate == 0 {
			maxRate = 15000
		}
		if totalIPs == 0 {
			totalIPs = 200000
		}

		shardStr := r.FormValue("shards")
		shardsCount, _ := strconv.Atoi(shardStr)
		if shardsCount < 1 {
			shardsCount = 1
		}
		if shardsCount > 20 {
			shardsCount = 20 // cap
		}

		// Master resolves ASNs → CIDRs → distributes across shards
		var shards []*master.Task
		if len(cleanASNs) == 0 {
			// Fallback: no ASNs, create single task
			shards = tasks.CreateShards(name, cleanASNs, ports, maxRate, totalIPs, shardsCount)
		} else {
			log.Printf("[master] Resolving %d ASNs to CIDRs for %d shards...", len(cleanASNs), shardsCount)

			// Step 1: Resolve all ASNs via RIPE API
			prefixMap, err := resolver.ResolveMultipleASNs(cleanASNs)
			if err != nil {
				http.Error(w, `{"error":"ASN resolution failed: `+err.Error()+`"}`, http.StatusInternalServerError)
				return
			}

			// Step 2: Flatten all CIDRs
			var allCIDRs []string
			for _, prefixes := range prefixMap {
				allCIDRs = append(allCIDRs, prefixes...)
			}
			log.Printf("[master] Resolved %d CIDRs from %d ASNs", len(allCIDRs), len(cleanASNs))

			// Step 3: Distribute CIDRs across shards
			cidrGroups := master.DistributeCIDRs(allCIDRs, shardsCount)
			log.Printf("[master] Distributed %d CIDRs into %d shards", len(allCIDRs), len(cidrGroups))

			// Step 4: Create shard tasks
			shards = tasks.CreateCIDRShards(name, cleanASNs, cidrGroups, ports, maxRate)
		}
		for _, task := range shards {
			dashboard.BroadcastTaskCreated(task)
		}

		w.Header().Set("Content-Type", "application/json")
		if len(shards) == 1 {
			json.NewEncoder(w).Encode(shards[0])
		} else {
			json.NewEncoder(w).Encode(map[string]any{
				"group_id": shards[0].GroupID,
				"total":    len(shards),
				"shards":   shards,
			})
		}
	})

	// REST API: task list
	mux.HandleFunc("/api/task/list", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(tasks.List())
	})

	// REST API: task get
	mux.HandleFunc("/api/task/get", func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("id")
		if id == "" {
			http.Error(w, `{"error":"missing id"}`, http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		task := tasks.GetByID(id)
		if task == nil {
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
			return
		}
		json.NewEncoder(w).Encode(task)
	})

	// REST API: task counts
	mux.HandleFunc("/api/task/counts", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(tasks.TaskCounts())
	})

	// REST API: cancel task (or group)
	mux.HandleFunc("/api/task/cancel", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		taskID := r.FormValue("task_id")
		groupID := r.FormValue("group_id")

		var cancelled []*master.Task
		if groupID != "" {
			cancelled = tasks.CancelGroup(groupID)
		} else if taskID != "" {
			if t := tasks.CancelTask(taskID); t != nil {
				cancelled = append(cancelled, t)
			}
		} else {
			http.Error(w, `{"error":"provide task_id or group_id"}`, http.StatusBadRequest)
			return
		}

		// Notify assigned workers
		for _, t := range cancelled {
			if t.AssignedTo != "" && t.Status == "cancelled" {
				// Decrement running task count
				store.DecrementRunningTasks(t.AssignedTo)

				conn := store.GetConn(t.AssignedTo)
				if conn != nil {
					cancelReq, _ := protocol.NewRequest(protocol.MethodMasterTaskCancel,
						protocol.TaskCancelParams{TaskID: t.ID, Reason: "cancelled_by_user"}, 0)
					data, _ := json.Marshal(cancelReq)
					conn.Write(data)
				}
			}
			dashboard.BroadcastTaskUpdate(t)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"cancelled": len(cancelled),
			"tasks":     cancelled,
		})
	})

	// Health check
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8800"
	}

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("[master] Shutting down...")
		os.Exit(0)
	}()

	log.Printf("[master] Listening on :%s", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatalf("[master] Listen error: %v", err)
	}
}
