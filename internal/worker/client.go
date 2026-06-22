package worker

import (
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/e13815332/multiscan/internal/protocol"
	"github.com/gorilla/websocket"
)

// detectHardware reads CPU count and total memory.
func detectHardware() (cpuCount, memoryMB int) {
	cpuCount = runtime.NumCPU()

	// Read /proc/meminfo
	data, err := os.ReadFile("/proc/meminfo")
	if err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "MemTotal:") {
				fields := strings.Fields(line)
				if len(fields) >= 2 {
					kb, err := strconv.Atoi(fields[1])
					if err == nil {
						memoryMB = kb / 1024
					}
				}
				break
			}
		}
	}
	if memoryMB <= 0 {
		memoryMB = 512 // safe default
	}
	return
}

// calculateLimits computes safe concurrency caps from hardware specs.
// maxTasks=1: each worker processes one shard at a time (user request for pull-based dispatch).
func calculateLimits(cpuCount, memoryMB int) (maxTasks, maxConcurrent, maxRate int) {
	maxTasks = 1 // one shard at a time, pull next when done

	// cf-scanner goroutines: 50 per core, capped [50, 200]
	maxConcurrent = cpuCount * 50
	if maxConcurrent < 50 {
		maxConcurrent = 50
	}
	if maxConcurrent > 200 {
		maxConcurrent = 200
	}

	// masscan hard cap: 14K pps
	maxRate = 14000

	return
}

// Client handles the Worker ↔ Master WebSocket connection.
type Client struct {
	MasterURL string
	Name      string
	UUID      string

	capabilities *protocol.WorkerCapabilities

	conn        *websocket.Conn
	nextID      int64
	stopCh      chan struct{}
	stopOnce    sync.Once
	tasks       map[string]*TaskRunner // taskID → runner
	tasksMu     sync.Mutex
}

// NewClient creates a new Worker client.
func NewClient(masterURL, name, uuid string) *Client {
	return &Client{
		MasterURL: masterURL,
		Name:      name,
		UUID:      uuid,
		stopCh:    make(chan struct{}),
		tasks:     make(map[string]*TaskRunner),
	}
}

// RunningTasks returns how many tasks are currently active.
func (c *Client) RunningTasks() int {
	c.tasksMu.Lock()
	defer c.tasksMu.Unlock()
	return len(c.tasks)
}

// CanAcceptTask returns true if the worker has capacity for another task.
func (c *Client) CanAcceptTask() bool {
	if c.capabilities == nil {
		return true
	}
	return c.RunningTasks() < c.capabilities.MaxTasks
}

// Run connects to Master and starts the communication loops.
func (c *Client) Run() error {
	// Detect hardware and calculate limits
	cpuCount, memoryMB := detectHardware()
	maxTasks, maxConcurrent, maxRate := calculateLimits(cpuCount, memoryMB)
	c.capabilities = &protocol.WorkerCapabilities{
		CPUCount:      cpuCount,
		MemoryMB:      memoryMB,
		MaxTasks:      maxTasks,
		MaxConcurrent: maxConcurrent,
		MaxRate:       maxRate,
	}
	log.Printf("[worker] Hardware: %d CPU, %dMB RAM → max_tasks=%d max_concurrent=%d max_rate=%d",
		cpuCount, memoryMB, maxTasks, maxConcurrent, maxRate)

	u := c.MasterURL
	log.Printf("[worker] Connecting to %s ...", u)

	conn, _, err := websocket.DefaultDialer.Dial(u, nil)
	if err != nil {
		return err
	}
	c.conn = conn
	log.Printf("[worker] Connected to %s", u)

	// 1. Register (with capabilities)
	if err := c.register(); err != nil {
		conn.Close()
		return err
	}

	// 2. Signal Master we're ready for a task
	c.pull()

	// 3. Start concurrent loops
	go c.heartbeatLoop()
	go c.readLoop()
	go c.reportLoop()

	return nil
}

func (c *Client) Stop() {
	c.stopOnce.Do(func() {
		close(c.stopCh)
		// Stop all running tasks
		c.tasksMu.Lock()
		for _, runner := range c.tasks {
			runner.Stop()
		}
		c.tasksMu.Unlock()
		if c.conn != nil {
			c.conn.Close()
		}
	})
}

// --- RPC methods ---

func (c *Client) register() error {
	params := protocol.RegisterParams{
		UUID:         c.UUID,
		Name:         c.Name,
		Capabilities: c.capabilities,
	}
	req, _ := protocol.NewRequest(protocol.MethodWorkerRegister, params, c.nextID)
	c.nextID++
	c.conn.WriteJSON(req)

	var resp protocol.Response
	c.conn.ReadJSON(&resp)
	if resp.Error != nil {
		log.Printf("[worker] Registration error: %v", resp.Error)
		return nil
	}

	var result protocol.RegisterResult
	if resp.Result != nil {
		json.Unmarshal(resp.Result, &result)
	}
	if result.UUID != "" {
		c.UUID = result.UUID
	}
	log.Printf("[worker] Registered as %s (%s)", c.Name, c.UUID)
	return nil
}

func (c *Client) pull() {
	params := protocol.PullParams{Status: "idle"}
	req, _ := protocol.NewRequest(protocol.MethodWorkerPull, params, c.nextID)
	c.nextID++
	c.conn.WriteJSON(req)
}

func (c *Client) sendReport(rp protocol.ReportParams) {
	req, _ := protocol.NewRequest(protocol.MethodWorkerReport, rp, c.nextID)
	c.nextID++
	c.conn.WriteJSON(req)
}

func (c *Client) sendResult(tr protocol.TaskResult) {
	req, _ := protocol.NewRequest(protocol.MethodWorkerResult, tr, c.nextID)
	c.nextID++
	c.conn.WriteJSON(req)
}

// --- Loops ---

func (c *Client) heartbeatLoop() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			hb := protocol.HeartbeatParams{
				CPUPercent:  float64(10 + rand.Intn(40)),
				MemPercent:  float64(30 + rand.Intn(50)),
				DiskPercent: float64(40 + rand.Intn(20)),
				CurrentTask: "",
				Phase:       "",
				Progress:    "",
			}
			// Include current running tasks info
			c.tasksMu.Lock()
			for _, r := range c.tasks {
				hb.CurrentTask = r.TaskID
				hb.Phase = r.Phase
				hb.Progress = ""
				break // heartbeat only reports first task
			}
			runningTasks := len(c.tasks)
			c.tasksMu.Unlock()
			hb.Progress = strconv.Itoa(runningTasks) + " running"

			req, _ := protocol.NewRequest(protocol.MethodWorkerHeartbeat, hb, c.nextID)
			c.nextID++
			c.conn.WriteJSON(req)
		}
	}
}

// reportLoop forwards TaskRunner progress → Master via worker.report.
// Handles multiple concurrent tasks.
func (c *Client) reportLoop() {
	for {
		select {
		case <-c.stopCh:
			return
		default:
		}

		// Snapshot current runners
		c.tasksMu.Lock()
		runners := make([]*TaskRunner, 0, len(c.tasks))
		for _, r := range c.tasks {
			runners = append(runners, r)
		}
		c.tasksMu.Unlock()

		if len(runners) == 0 {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		// Drain progress/result from all runners
		hadWork := false
		for _, r := range runners {
			select {
			case report := <-r.Progress():
				c.sendReport(report)
				hadWork = true
			default:
			}
			select {
			case result := <-r.Result():
				c.sendResult(result)
				log.Printf("[worker] Task %s completed: %d hits in %s", result.TaskID, result.Hits, result.Duration)
				c.tasksMu.Lock()
				delete(c.tasks, result.TaskID)
				c.tasksMu.Unlock()
				// Signal master we have capacity
				c.pull()
				hadWork = true
			default:
			}
		}

		if !hadWork {
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func (c *Client) readLoop() {
	for {
		select {
		case <-c.stopCh:
			return
		default:
		}

		_, msgBytes, err := c.conn.ReadMessage()
		if err != nil {
			log.Printf("[worker] Read error: %v", err)
			c.Stop()
			return
		}

		// Try parsing as Response (ack from Master) — skip silently
		var resp struct {
			JSONRPC string          `json:"jsonrpc"`
			ID      int64           `json:"id"`
			Result  json.RawMessage `json:"result,omitempty"`
			Error   *protocol.Error `json:"error,omitempty"`
		}
		if err := json.Unmarshal(msgBytes, &resp); err == nil && resp.JSONRPC == "2.0" && (resp.Result != nil || resp.Error != nil) {
			continue // this is a response to our request, not an event
		}

		// Parse as Request (Master → Worker event)
		var req protocol.Request
		if err := json.Unmarshal(msgBytes, &req); err != nil || req.Method == "" {
			continue
		}

		c.handleMasterMessage(req)
	}
}

func (c *Client) handleMasterMessage(req protocol.Request) {
	switch req.Method {
	case protocol.MethodMasterPing:
		pong, _ := protocol.NewRequest(protocol.MethodWorkerPong, nil, c.nextID)
		c.nextID++
		c.conn.WriteJSON(pong)

	case protocol.MethodMasterTaskAssign:
		var params protocol.TaskAssignParams
		if err := json.Unmarshal(req.Params, &params); err != nil {
			log.Printf("[worker] Invalid task assign: %v", err)
			return
		}

		// Cap MaxRate to hardware limit
		if c.capabilities != nil && params.MaxRate > c.capabilities.MaxRate {
			params.MaxRate = c.capabilities.MaxRate
		}

		log.Printf("[worker] Task assigned: %s (ASNs=%v ports=%v shard=%d/%d)",
			params.TaskID, params.ASNs, params.Ports, params.ShardIndex+1, params.ShardTotal)

		c.tasksMu.Lock()
		// Check capacity
		if c.capabilities != nil && len(c.tasks) >= c.capabilities.MaxTasks {
			log.Printf("[worker] REJECTING task %s: at capacity %d/%d",
				params.TaskID, len(c.tasks), c.capabilities.MaxTasks)
			c.tasksMu.Unlock()
			// Send failure immediately so master reassigns
			c.sendResult(protocol.TaskResult{
				TaskID:   params.TaskID,
				Status:   "failed",
				Duration: "0s",
			})
			return
		}
		c.tasksMu.Unlock()

		// Start new task
		runner := NewTaskRunner(params.TaskID, params.ASNs, params.Ports, params.MaxRate)
		runner.CIDRs = params.CIDRs
		runner.MaxConcurrency = c.capabilities.MaxConcurrent
		runner.Phase = "assigned"
		if params.ShardTotal > 1 {
			runner.SetShard(params.ShardIndex, params.ShardTotal)
		}
		if len(params.IPs) > 0 {
			runner.SetIPs(params.IPs)
		} else if len(params.ASNs) == 0 || (len(params.ASNs) == 1 && params.ASNs[0] == "") {
			runner.SetIPs(getDefaultTestIPs())
		}

		c.tasksMu.Lock()
		c.tasks[params.TaskID] = runner
		c.tasksMu.Unlock()
		runner.Run()

	case protocol.MethodMasterTaskCancel:
		c.tasksMu.Lock()
		runner, ok := c.tasks[req.Method] // need TaskID from params
		c.tasksMu.Unlock()
		// Parse cancel params
		var cancelParams protocol.TaskCancelParams
		if err := json.Unmarshal(req.Params, &cancelParams); err == nil {
			c.tasksMu.Lock()
			runner, ok = c.tasks[cancelParams.TaskID]
			c.tasksMu.Unlock()
			if ok {
				log.Printf("[worker] Task %s cancelled by master", cancelParams.TaskID)
				runner.Stop()
				c.tasksMu.Lock()
				delete(c.tasks, cancelParams.TaskID)
				c.tasksMu.Unlock()
			}
		} else if ok {
			log.Printf("[worker] Task cancelled by master")
			runner.Stop()
			c.tasksMu.Lock()
			delete(c.tasks, runner.TaskID)
			c.tasksMu.Unlock()
		}

	case protocol.MethodMasterNotify:
		log.Printf("[worker] Notification from master")

	default:
		log.Printf("[worker] Unknown method: %s", req.Method)
	}
}
