package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/e13815332/multiscan/internal/masscan"
	"github.com/e13815332/multiscan/internal/scanner"
	"github.com/e13815332/multiscan/internal/verifier"
	"github.com/e13815332/multiscan/v2/internal/pg"
	"github.com/nats-io/nats.go"
)

type MasscanJob struct {
	JobID   string   `json:"job_id"`
	CIDRs   []string `json:"cidrs"`
	Ports   []int    `json:"ports"`
	MaxRate int      `json:"max_rate"`
}

var (
	workerID    = flag.String("id", "worker-1", "worker ID")
	workerName  = flag.String("name", "worker", "worker name")
	pgHost      = flag.String("pg-host", "127.0.0.1", "PG host")
	pgPort      = flag.Int("pg-port", 5432, "PG port")
	natsURL     = flag.String("nats", "nats://127.0.0.1:4222", "NATS URL")
	scanConc    = flag.Int("s-conc", 0, "HTTP HEAD scan concurrency (0=auto)")
	verifyConc  = flag.Int("v-conc", 0, "API verify concurrency (0=auto)")
	masscanRate = flag.Int("masscan-rate", 0, "masscan pps (0=auto)")
	reprobe     = flag.Bool("reprobe", false, "force hardware re-detection")
)

// ── 网卡探测 ──
func findIface() string {
	conn, err := net.Dial("udp", "1.1.1.1:53")
	if err == nil {
		addr := conn.LocalAddr().(*net.UDPAddr)
		conn.Close()
		ifaces, _ := net.Interfaces()
		for _, iface := range ifaces {
			addrs, _ := iface.Addrs()
			for _, a := range addrs {
				if ipnet, ok := a.(*net.IPNet); ok && ipnet.IP.Equal(addr.IP) {
					return iface.Name
				}
			}
		}
	}
	for _, name := range []string{"eth0", "ens3", "enp0s3", "enp1s0", "ens5", "enp0s1"} {
		if _, err := os.Stat("/sys/class/net/" + name + "/statistics/tx_packets"); err == nil {
			return name
		}
	}
	return "eth0"
}

func readTxPackets(iface string) (int64, error) {
	data, err := os.ReadFile("/sys/class/net/" + iface + "/statistics/tx_packets")
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
}

// ── 实测 masscan 发包上限（参考 ASNIPtest run.py 的 probe_masscan_rate） ──
// 首次探测后缓存到 /var/lib/multiscan/hardware.json，后续启动直接读取。
// 只有缓存不存在或 --reprobe 时才会重新探测。
const hardwareCache = "/var/lib/multiscan/hardware.json"

type hardwareInfo struct {
	MasscanRate int `json:"masscan_rate"`
	CacheTS     string `json:"cached_at"`
}

func probeMasscanRate() int {
	iface := findIface()
	log.Printf("[probe] iface=%s", iface)

	bestRate := 2000
	testRate := 1000
	maxTest := 200000
	probeSec := 5

	for testRate <= maxTest {
		txBefore, err := readTxPackets(iface)
		if err != nil {
			break
		}

		cmd := exec.Command("masscan", "1.0.0.0/8", "-p", "443",
			"--rate", strconv.Itoa(testRate), "-oX", "/dev/null")
		cmd.Stdout = nil
		cmd.Stderr = nil
		cmd.Start()

		time.Sleep(time.Duration(probeSec) * time.Second)
		cmd.Process.Kill()
		cmd.Wait()

		txAfter, _ := readTxPackets(iface)
		actualPPS := float64(txAfter-txBefore) / float64(probeSec)
		ratio := actualPPS / float64(testRate)

		log.Printf("[probe] rate=%d actual=%.0f ratio=%.2f", testRate, actualPPS, ratio)

		if ratio >= 0.7 {
			bestRate = testRate
			testRate *= 2
		} else if ratio >= 0.3 {
			bestRate = max(2000, int(actualPPS*0.8))
			break
		} else {
			break
		}
	}
	log.Printf("[probe] masscan best=%d pps", bestRate)

	// 写缓存
	if err := os.MkdirAll("/var/lib/multiscan", 0755); err == nil {
		info := hardwareInfo{MasscanRate: bestRate, CacheTS: time.Now().Format(time.RFC3339)}
		if data, err := json.Marshal(info); err == nil {
			os.WriteFile(hardwareCache, data, 0644)
		}
	}
	return bestRate
}

func autoDetect() (cpu, memMB, sConc, vConc, mRate int) {
	cpu = runtime.NumCPU()
	memMB = 512
	if f, err := os.Open("/proc/meminfo"); err == nil {
		var kb int64
		fmt.Fscanf(f, "MemTotal: %d kB", &kb)
		f.Close()
		memMB = int(kb / 1024)
	}

	// 1. masscan 发包速率 — 优先读缓存，避免每次启动都探测 15-30s
	mRate = 2000 // 最低保底
	if !*reprobe {
		if data, err := os.ReadFile(hardwareCache); err == nil {
			var info hardwareInfo
			if json.Unmarshal(data, &info) == nil && info.MasscanRate > 0 {
				mRate = info.MasscanRate
				log.Printf("[probe] using cached masscan rate=%d (from %s)", mRate, info.CacheTS)
			}
		}
	}
	if mRate == 2000 || *reprobe {
		mRate = probeMasscanRate()
	}

	// 2. Phase 2a 扫描并发 — 固定 500（对齐 cf-scanner）
	sConc = 500

	// 3. Phase 2b 验证并发 — 固定 32
	vConc = 32

	return
}

func main() {
	flag.Parse()
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	// 先连 PG 和 NATS
	pgPassword := os.Getenv("PG_PASSWORD")
	if pgPassword == "" {
		pgPassword = "multiscan" // default
	}
	store, err := pg.Connect(*pgHost, *pgPort, "multiscan", pgPassword, "multiscan")
	if err != nil {
		log.Fatalf("PG connect: %v", err)
	}

	nc, err := nats.Connect(*natsURL)
	if err != nil {
		log.Fatalf("NATS: %v", err)
	}
	defer nc.Close()

	// ── 第一时间订阅 NATS，再探测硬件，避免消息丢失 ──
	phase2Jobs := sync.Map{}

	_, _ = nc.Subscribe("masscan.*."+*workerID, func(msg *nats.Msg) {
		var job MasscanJob
		if err := json.Unmarshal(msg.Data, &job); err != nil {
			log.Printf("bad masscan msg: %v", err)
			return
		}
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("PANIC in masscan goroutine: %v", r)
					store.MarkPhase1Done(job.JobID)
					store.SetActivity(*workerID, "idle")
				}
			}()
			log.Printf("Phase1 job=%s cidrs=%d ports=%v", job.JobID, len(job.CIDRs), job.Ports)
			store.SetActivity(*workerID, "masscan")

			cfg := masscan.Config{
				CIDRs: job.CIDRs,
				Ports: job.Ports,
				Rate:  job.MaxRate,
			}
			if cfg.Rate == 0 {
				cfg.Rate = *masscanRate
			}

			ch, cmd, err := masscan.RunStreaming(cfg, 500)
			if err != nil {
				log.Printf("masscan start failed: %v", err)
				store.MarkPhase1Done(job.JobID)
				store.SetActivity(*workerID, "idle")
				return
			}

			total := 0
			for batch := range ch {
				if batch.Err != nil {
					log.Printf("masscan stream err: %v", batch.Err)
					continue
				}
				if batch.Done {
					break
				}
				ips := make([]string, len(batch.Results))
				for j, r := range batch.Results {
					ips[j] = r.IP
				}
				if len(batch.Results) > 0 {
					if err := store.InsertMasscanResults(job.JobID, ips, batch.Results[0].Port); err != nil {
						log.Printf("insert err: %v", err)
					}
				}
				total += len(batch.Results)
				if total%10000 == 0 {
					log.Printf("Phase1 progress: %d open ports so far", total)
				}
			}
			_ = cmd.Wait()
			log.Printf("masscan done: %d open ports", total)
			store.MarkPhase1Done(job.JobID)
			store.SetActivity(*workerID, "idle")
		}()
	})

	// 硬件探测
	cpu, memMB, autoScanConc, autoVerifyConc, autoMasscanRate := autoDetect()
	if *scanConc == 0 {
		*scanConc = autoScanConc
	}
	if *verifyConc == 0 {
		*verifyConc = autoVerifyConc
	}
	if *masscanRate == 0 {
		*masscanRate = autoMasscanRate
	}
	store.RegisterWorker(*workerID, *workerName, cpu, memMB, *scanConc, *masscanRate)

	log.Printf("[worker] %s CPU=%d MEM=%dMB scan=%d verify=%d masscan=%dpps",
		*workerID, cpu, memMB, *scanConc, *verifyConc, *masscanRate)

	go func() {
		for {
			store.Heartbeat(*workerID, "")
			time.Sleep(15 * time.Second)
		}
	}()

	log.Printf("[worker] %s ready", *workerID)

	// Subscribe to Phase2 NATS notifications from Master
	_, _ = nc.Subscribe("phase2.*", func(msg *nats.Msg) {
		jid := strings.TrimPrefix(msg.Subject, "phase2.")
		if jid == "" || jid == msg.Subject {
			return
		}
		if _, loaded := phase2Jobs.LoadOrStore(jid, true); !loaded {
			log.Printf("[worker] Phase2 triggered via NATS: %s", jid)
			go processPhase2(store, *workerID, jid)
		}
	})

	// One-time startup: pick up any Phase2 jobs we might have missed
	go func() {
		time.Sleep(3 * time.Second)
		rows, err := store.DB.Query(`SELECT DISTINCT job_id FROM work_queue WHERE status='pending' LIMIT 20`)
		if err != nil || rows == nil {
			return
		}
		for rows.Next() {
			var jid string
			rows.Scan(&jid)
			if _, loaded := phase2Jobs.LoadOrStore(jid, true); !loaded {
				log.Printf("[worker] Phase2 picked up at startup: %s", jid)
				go processPhase2(store, *workerID, jid)
			}
		}
		rows.Close()
	}()

	// Phase2b reconciliation: pick up unverified tls-ok results
	go func() {
		time.Sleep(5 * time.Second)
		for {
			rows, err := store.DB.Query(`
				SELECT DISTINCT r.job_id FROM results r
				JOIN jobs j ON j.id = r.job_id
				WHERE r.status='tls-ok' AND r.colo='' AND j.phase IN ('phase2b','phase2')
				AND j.status='running' LIMIT 10`)
			if err != nil || rows == nil {
				time.Sleep(10 * time.Second)
				continue
			}
			var jids []string
			for rows.Next() {
				var jid string
				rows.Scan(&jid)
				jids = append(jids, jid)
			}
			rows.Close()
			for _, jid := range jids {
				if _, loaded := phase2Jobs.LoadOrStore(jid, true); !loaded {
					log.Printf("[worker] Phase2b reconciliation: verifying %s", jid)
					go processPhase2(store, *workerID, jid)
				}
			}
			time.Sleep(15 * time.Second)
		}
	}()

	select {}
}

type cfHit struct {
	IP    string
	Port  int
	Delay int
}

func processPhase2(store *pg.Store, wid, jobID string) {
	cfg := scanner.DefaultConfig()

	log.Printf("[%s] Phase2a scan starting...", jobID)
	store.SetActivity(wid, "scanning")
	var cfHits []cfHit
	var scanned int
	sem := make(chan struct{}, *scanConc)
	var mu sync.Mutex

	const flushAt = 250
	var buf []pg.ScannedItem
	flush := func() {
		if len(buf) == 0 {
			return
		}
		batch := buf
		buf = nil
		store.MarkWorkScannedBatch(jobID, batch)
	}

	for {
		if store.IsJobCancelled(jobID) {
			flush()
			return
		}
		items, err := store.FetchWorkBatch(jobID, wid, *scanConc)
		if err != nil || len(items) == 0 {
			var unfinished int
			store.DB.QueryRow(`SELECT COUNT(*) FROM work_queue WHERE job_id=$1 AND status NOT IN ('done')`, jobID).Scan(&unfinished)
			if unfinished > 0 {
				time.Sleep(2 * time.Second)
				continue
			}
			flush()
			break
		}

		var wg sync.WaitGroup
		for _, item := range items {
			wg.Add(1)
			sem <- struct{}{}
			go func(item pg.WorkItem) {
				defer wg.Done()
				defer func() { <-sem }()
				defer func() {
					if r := recover(); r != nil {
						log.Printf("PANIC scan %s:%d: %v", item.IP, item.Port, r)
					}
				}()

				result := scanner.CheckProxy(item.IP, item.Port, cfg)
				if result.IsCF {
					mu.Lock()
					cfHits = append(cfHits, cfHit{item.IP, item.Port, result.Delay})
					mu.Unlock()
					store.InsertResult(jobID, item.IP, item.Port,
						"tls-ok", "", "", "", result.Delay, "", "")
				}
				mu.Lock()
				buf = append(buf, pg.ScannedItem{IP: item.IP, Port: item.Port})
				if len(buf) >= flushAt {
					batch := buf
					buf = nil
					mu.Unlock()
					store.MarkWorkScannedBatch(jobID, batch)
				} else {
					mu.Unlock()
				}
			}(item)
		}
		wg.Wait()

		scanned += len(items)
		flush()

		if scanned%10000 == 0 {
			log.Printf("[%s] Phase2a scanned=%d cf_hits=%d", jobID, scanned, len(cfHits))
		}
	}

	log.Printf("[%s] Phase2a done: scanned=%d cf_hits=%d", jobID, scanned, len(cfHits))

	// Even if no local cf_Hits, check DB for unverified tls-ok results (reconciliation)
	if len(cfHits) == 0 {
		var unverified int
		store.DB.QueryRow(`SELECT COUNT(*) FROM results WHERE job_id=$1 AND status='tls-ok' AND colo=''`, jobID).Scan(&unverified)
		if unverified == 0 {
			log.Printf("[%s] Phase2a done: scanned=%d cf_hits=0 (waiting for master)", jobID, scanned)
			store.SetActivity(wid, "idle")
			return
		}
		log.Printf("[%s] Phase2a done: no new hits, but %d unverified tls-ok in DB", jobID, unverified)
	}

	// Phase2b: continuous verification loop — fetch tls-ok from DB, verify, update
	runPhase2b(store, wid, jobID)
}

func runPhase2b(store *pg.Store, wid, jobID string) {
	log.Printf("[%s] Phase2b verifying CF hits...", jobID)
	store.SetActivity(wid, "verifying")

	vsem := make(chan struct{}, *verifyConc)
	var mu sync.Mutex
	var totalVerified int

	for {
		if store.IsJobCancelled(jobID) {
			return
		}

		// Fetch unverified tls-ok results from DB
		rows, err := store.DB.Query(`
			SELECT ip, port, delay FROM results
			WHERE job_id=$1 AND status='tls-ok' AND colo=''
			LIMIT $2`, jobID, *verifyConc*2)
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}
		var hits []cfHit
		for rows.Next() {
			var h cfHit
			rows.Scan(&h.IP, &h.Port, &h.Delay)
			hits = append(hits, h)
		}
		rows.Close()

		if len(hits) == 0 {
			break
		}

		var wg sync.WaitGroup
		for _, h := range hits {
			wg.Add(1)
			vsem <- struct{}{}
			go func(h cfHit) {
				defer wg.Done()
				defer func() { <-vsem }()
				defer func() {
					if r := recover(); r != nil {
						log.Printf("PANIC verify %s:%d: %v", h.IP, h.Port, r)
					}
				}()
				vResult, err := verifier.Verify(h.IP, h.Port)
				if err != nil {
					// Mark as non-CF so it doesn't get retried
					store.DB.Exec(`UPDATE results SET colo='error' WHERE job_id=$1 AND ip=$2 AND port=$3 AND status='tls-ok'`, jobID, h.IP, h.Port)
					return
				}
				if vResult == nil || !vResult.Success {
					store.DB.Exec(`UPDATE results SET colo='fail' WHERE job_id=$1 AND ip=$2 AND port=$3 AND status='tls-ok'`, jobID, h.IP, h.Port)
					return
				}
				store.InsertResult(jobID, h.IP, h.Port,
					"verified", vResult.Colo, vResult.Country,
					vResult.ASN, h.Delay, "", "")
				mu.Lock()
				totalVerified++
				mu.Unlock()
			}(h)
		}
		wg.Wait()
	}

	// Drain semaphore
	for i := 0; i < cap(vsem); i++ {
		vsem <- struct{}{}
	}

	log.Printf("[%s] Phase2b done: verified=%d", jobID, totalVerified)
	store.SetActivity(wid, "idle")
}
