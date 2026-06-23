package worker

import (
	"log"
	"sync"
	"time"

	"github.com/e13815332/multiscan/internal/masscan"
	"github.com/e13815332/multiscan/internal/protocol"
	"github.com/e13815332/multiscan/internal/resolver"
	"github.com/e13815332/multiscan/internal/scanner"
	"github.com/e13815332/multiscan/internal/verifier"
)

// TaskRunner executes a scanning task with the full pipeline:
// ASN→CIDR → masscan → cf-scanner → 090227 verify.
type TaskRunner struct {
	TaskID     string
	ASNs       []string
	CIDRs      []string // pre-resolved CIDRs (master-side), skips ASN resolution + IP generation
	Ports      []int
	MaxRate    int
	Status     string
	Phase      string

	// MaxConcurrency caps cf-scanner goroutine count.
	// Set from Worker hardware detection (default 200).
	MaxConcurrency int

	ips        []string // pre-provided IP list (bypasses ASN resolution)
	shardIndex int
	shardTotal int
	progressCh chan protocol.ReportParams
	resultCh   chan protocol.TaskResult
	stopCh     chan struct{}
}

// NewTaskRunner creates a new task runner.
func NewTaskRunner(taskID string, asns []string, ports []int, maxRate int) *TaskRunner {
	return &TaskRunner{
		TaskID:     taskID,
		ASNs:       asns,
		Ports:      ports,
		MaxRate:    maxRate,
		Status:     "running",
		progressCh: make(chan protocol.ReportParams, 10),
		resultCh:   make(chan protocol.TaskResult, 1),
		stopCh:     make(chan struct{}),
	}
}

// SetIPs sets a pre-resolved IP list. Bypasses ASN resolution and masscan.
func (r *TaskRunner) SetIPs(ips []string) {
	r.ips = ips
}

// SetShard sets the shard range for IP-count-based splitting.
func (r *TaskRunner) SetShard(index, total int) {
	r.shardIndex = index
	r.shardTotal = total
}

// Run starts the task pipeline.
func (r *TaskRunner) Run() {
	go r.run()
}

func (r *TaskRunner) Progress() <-chan protocol.ReportParams { return r.progressCh }
func (r *TaskRunner) Result() <-chan protocol.TaskResult     { return r.resultCh }
func (r *TaskRunner) Stop()                                   { close(r.stopCh) }

func (r *TaskRunner) run() {
	startTime := time.Now()
	var finalResults []protocol.ScanEntry

	// ── Phase 1: Get targets (CIDRs or ASN→IP) ──
	var liveIPs []string

	if len(r.CIDRs) > 0 {
		// New path: pre-resolved CIDRs from master, skip ASN resolve + IP generation
		r.sendProgress("resolve", "skipped", 0, len(r.CIDRs), 0)
		log.Printf("[runner] Using %d pre-resolved CIDRs, skipping ASN resolve", len(r.CIDRs))
		r.sendProgress("masscan", "0%", 0, len(r.CIDRs), 0)
		liveIPs = r.runMasscanCIDRs(r.CIDRs)
		if liveIPs == nil {
			return // failed or cancelled
		}
	} else if len(r.ips) > 0 {
		liveIPs = r.ips
		r.sendProgress("resolve", "skipped", 0, len(liveIPs), 0)
		r.sendProgress("masscan", "skipped", len(liveIPs), len(liveIPs), 0)
		log.Printf("[runner] Using %d pre-provided IPs, skipping ASN resolve and masscan", len(liveIPs))
	} else {
		// Legacy path: resolve ASNs and generate IP sample
		r.sendProgress("resolve", "0%", 0, 0, 0)
		log.Printf("[runner] Resolving ASNs %v...", r.ASNs)
		prefixes, err := resolver.ResolveMultipleASNs(r.ASNs)
		if err != nil {
			r.fail("ASN resolution failed: " + err.Error())
			return
		}

		// Flatten all CIDRs
		var allCIDRs []string
		for _, p := range prefixes {
			allCIDRs = append(allCIDRs, p...)
		}
		log.Printf("[runner] Resolved %d CIDRs from %d ASNs", len(allCIDRs), len(r.ASNs))

		// Generate random IPs from CIDRs
		sampleSize := 50000 // default sample for masscan
		if r.MaxRate > 0 {
			sampleSize = r.MaxRate * 3 // 3 seconds worth of scanning
		}
		generatedIPs := resolver.ExpandCIDR(allCIDRs, sampleSize)
		log.Printf("[runner] Generated %d sample IPs from CIDRs", len(generatedIPs))
		r.sendProgress("resolve", "100%", len(generatedIPs), len(allCIDRs), 0)

		if len(generatedIPs) == 0 {
			r.fail("no IPs generated from CIDRs")
			return
		}

		// ── Phase 1b: masscan ──
		liveIPs = r.runMasscan(generatedIPs)
		if liveIPs == nil {
			return // failed or cancelled
		}
	}

	// ── Phase 2: cf-scanner (TLS/HTTP check) ──
	r.sendProgress("cf-scanner", "0%", 0, len(liveIPs), 0)
	log.Printf("[runner] Scanning %d IPs on ports %v...", len(liveIPs), r.Ports)

	cfg := scanner.DefaultConfig()
	cfg.Concurrency = r.MaxRate / 50
	if cfg.Concurrency < 50 {
		cfg.Concurrency = 50
	}
	// Cap to hardware limit
	if r.MaxConcurrency > 0 && cfg.Concurrency > r.MaxConcurrency {
		cfg.Concurrency = r.MaxConcurrency
	}

	scanned := 0
	hits := 0

	type scanJob struct {
		ip   string
		port int
	}
	jobs := make(chan scanJob, len(liveIPs)*len(r.Ports))
	go func() {
		for _, ip := range liveIPs {
			for _, port := range r.Ports {
				select {
				case <-r.stopCh:
					return
				case jobs <- scanJob{ip, port}:
				}
			}
		}
		close(jobs)
	}()

	results := make(chan scanner.ScanResult, cfg.Concurrency)

	workerDone := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		sem := make(chan struct{}, cfg.Concurrency)
		for job := range jobs {
			select {
			case <-r.stopCh:
				break
			default:
			}
			sem <- struct{}{}
			wg.Add(1)
			go func(j scanJob) {
				defer wg.Done()
				defer func() { <-sem }()
				result := scanner.CheckProxy(j.ip, j.port, cfg)
				select {
				case results <- result:
				case <-r.stopCh:
				}
			}(job)
		}
		wg.Wait()
		close(results)
		close(workerDone)
	}()

	progressTick := time.NewTicker(2 * time.Second)
	defer progressTick.Stop()

	totalJobs := len(liveIPs) * len(r.Ports)
	var cfHits []scanner.ScanResult
	done := false

	for !done {
		select {
		case <-r.stopCh:
			r.resultCh <- protocol.TaskResult{
				TaskID: r.TaskID, Status: "cancelled",
				Duration: time.Since(startTime).Round(time.Second).String(),
			}
			return
		case result, ok := <-results:
			if !ok {
				done = true
				break
			}
			scanned++
			if result.IsCF {
				hits++
				cfHits = append(cfHits, result)
				finalResults = append(finalResults, protocol.ScanEntry{
					IP: result.IP, Port: result.Port,
					Status: "tls-ok", Delay: result.Delay,
				})
			}
		case <-progressTick.C:
			pct := int(float64(scanned) / float64(totalJobs) * 100)
			r.sendProgress("cf-scanner", pctStr(pct), scanned, totalJobs, hits)
		}
	}

	// ── Phase 3: 090227 API verify ──
	if len(cfHits) > 0 {
		r.sendProgress("verify", "0%", 0, len(cfHits), hits)
		log.Printf("[runner] Verifying %d CF hits via 090227 API...", len(cfHits))
		r.verifyHits(cfHits, &finalResults, startTime)
	}

	duration := time.Since(startTime).Round(time.Second).String()

	select {
	case r.resultCh <- protocol.TaskResult{
		TaskID: r.TaskID, Status: "completed",
		Hits: hits, TotalIPs: scanned,
		Duration: duration, Results: finalResults,
	}:
	case <-r.stopCh:
	}
}

func (r *TaskRunner) runMasscan(ips []string) []string {
	// Check if masscan is available — skip if not
	mc := masscan.DefaultConfig()
	mc.IPs = ips
	mc.Ports = r.Ports
	mc.Rate = r.MaxRate
	if mc.Rate < 1000 {
		mc.Rate = 1000
	}

	r.sendProgress("masscan", "0%", 0, len(ips), 0)
	log.Printf("[runner] Running masscan on %d IPs (rate=%d)...", len(ips), mc.Rate)

	results, err := masscan.Run(mc)
	if err != nil {
		if mnErr, ok := err.(*masscan.MasscanError); ok && mnErr.NotFound {
			log.Printf("[runner] masscan not found, using all IPs directly")
			r.sendProgress("masscan", "skipped", len(ips), len(ips), 0)
			return ips
		}
		log.Printf("[runner] masscan error: %v, using all IPs directly", err)
		r.sendProgress("masscan", "skipped", len(ips), len(ips), 0)
		return ips
	}

	// Deduplicate by IP
	ipSet := make(map[string]bool)
	for _, res := range results {
		ipSet[res.IP] = true
	}
	liveIPs := make([]string, 0, len(ipSet))
	for ip := range ipSet {
		liveIPs = append(liveIPs, ip)
	}
	log.Printf("[runner] masscan found %d live hosts (from %d IPs)", len(liveIPs), len(ips))
	r.sendProgress("masscan", "100%", len(liveIPs), len(ips), 0)

	return liveIPs
}

// runMasscanCIDRs feeds CIDR ranges directly to masscan (no IP generation).
// Returns discovered live hosts.
func (r *TaskRunner) runMasscanCIDRs(cidrs []string) []string {
	mc := masscan.DefaultConfig()
	mc.CIDRs = cidrs
	mc.Ports = r.Ports
	mc.Rate = r.MaxRate
	if mc.Rate < 1000 {
		mc.Rate = 1000
	}
	mc.Timeout = 0 // no timeout for large scans

	r.sendProgress("masscan", "0%", 0, len(cidrs), 0)
	log.Printf("[runner] Running masscan on %d CIDRs (rate=%d)...", len(cidrs), mc.Rate)

	results, err := masscan.Run(mc)
	if err != nil {
		if mnErr, ok := err.(*masscan.MasscanError); ok && mnErr.NotFound {
			log.Printf("[runner] masscan not found, cannot scan CIDRs directly")
			r.fail("masscan not installed")
			return nil
		}
		log.Printf("[runner] masscan error: %v, using CIDRs directly as live hosts", err)
		// Fallback: feed CIDR ranges directly (masscan may have partial results)
		if len(results) == 0 {
			// No results at all, mark as empty
			r.sendProgress("masscan", "100%", 0, len(cidrs), 0)
			return nil
		}
	}

	// Deduplicate by IP
	ipSet := make(map[string]bool)
	for _, res := range results {
		ipSet[res.IP] = true
	}
	liveIPs := make([]string, 0, len(ipSet))
	for ip := range ipSet {
		liveIPs = append(liveIPs, ip)
	}
	log.Printf("[runner] masscan found %d live hosts from %d CIDRs", len(liveIPs), len(cidrs))
	r.sendProgress("masscan", "100%", len(liveIPs), len(cidrs), 0)

	return liveIPs
}

func (r *TaskRunner) verifyHits(cfHits []scanner.ScanResult, finalResults *[]protocol.ScanEntry, startTime time.Time) {
	// Build target list
	targets := make([]string, len(cfHits))
	hitMap := make(map[string]*protocol.ScanEntry)
	for i, h := range cfHits {
		target := h.IP + ":" + itoa(h.Port)
		targets[i] = target
		hitMap[target] = &(*finalResults)[i]
	}

	// Run verification
	concurrency := r.MaxRate / 100
	if concurrency < 8 {
		concurrency = 8
	}
	if concurrency > 100 {
		concurrency = 100
	}
	batch := verifier.VerifyBatch(targets, concurrency)

	verified := 0
	for _, v := range batch {
		target := v.IP + ":" + itoa(v.Port)
		if entry, ok := hitMap[target]; ok {
			if v.Valid {
				entry.Colo = v.Colo
				entry.ASN = v.ASN
				entry.Status = "verified"
				verified++
			} else {
				entry.Status = "failed-verify"
			}
		}
	}

	log.Printf("[runner] 090227 verified %d/%d CF hits", verified, len(cfHits))
	r.sendProgress("verify", "100%", verified, len(cfHits), verified)
}

func (r *TaskRunner) sendProgress(phase, progress string, scanned, total, hits int) {
	select {
	case r.progressCh <- protocol.ReportParams{
		TaskID: r.TaskID, Phase: phase,
		Progress: progress, ScannedIPs: scanned,
		TotalIPs: total, Hits: hits,
	}:
	default:
	}
}

func (r *TaskRunner) fail(msg string) {
	log.Printf("[runner] Task %s failed: %s", r.TaskID, msg)
	r.resultCh <- protocol.TaskResult{
		TaskID: r.TaskID, Status: "failed",
	}
}

func capConcurrency(rate int) int {
	c := rate / 150
	if c < 10 {
		return 10
	}
	if c > 500 {
		return 500
	}
	return c
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + (n % 10))
		n /= 10
	}
	return string(buf[i:])
}

func pctStr(n int) string {
	if n > 100 {
		n = 100
	}
	return itoa(n) + "%"
}
