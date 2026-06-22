package master

import (
	"log"
	"sync"
	"time"

	"github.com/e13815332/multiscan/internal/protocol"
)

// WorkerInfo holds persistent information about a Worker.
type WorkerInfo struct {
	UUID        string    `json:"uuid"`
	Name        string    `json:"name"`
	Status      string    `json:"status"` // "online", "offline", "idle", "running"
	Online      bool      `json:"online"`
	CurrentTask string    `json:"current_task,omitempty"`
	Phase       string    `json:"phase,omitempty"`
	Progress    string    `json:"progress,omitempty"`
	CPUPercent  float64   `json:"cpu_percent"`
	MemPercent  float64   `json:"memory_percent"`
	DiskPercent float64   `json:"disk_percent"`
	Addr        string    `json:"addr"`
	ConnectedAt time.Time `json:"connected_at"`
	LastBeat    time.Time `json:"last_heartbeat"`

	// Hardware limits and current load
	Capabilities *protocol.WorkerCapabilities `json:"capabilities,omitempty"`
	RunningTasks int                          `json:"running_tasks"`
}

// SafeConn wraps a WebSocket connection for concurrent writes.
type SafeConn struct {
	mu     sync.Mutex
	write  func(data []byte) error
	closed bool
}

func NewSafeConn(writeFn func(data []byte) error) *SafeConn {
	return &SafeConn{write: writeFn}
}

func (c *SafeConn) Write(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	return c.write(data)
}

func (c *SafeConn) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
}

// WorkerStore manages all connected and known Workers.
type WorkerStore struct {
	mu       sync.RWMutex
	workers  map[string]*WorkerInfo // uuid → info
	conns    map[string]*SafeConn   // uuid → WebSocket connection
	lastBeat map[string]time.Time   // uuid → last heartbeat time
}

func NewWorkerStore() *WorkerStore {
	ws := &WorkerStore{
		workers:  make(map[string]*WorkerInfo),
		conns:    make(map[string]*SafeConn),
		lastBeat: make(map[string]time.Time),
	}
	go ws.offlineDetector()
	return ws
}

// Register adds a new Worker or updates an existing one.
func (s *WorkerStore) Register(uuid, name, addr string, caps *protocol.WorkerCapabilities) *WorkerInfo {
	s.mu.Lock()
	defer s.mu.Unlock()

	info, exists := s.workers[uuid]
	if !exists {
		info = &WorkerInfo{
			UUID: uuid,
			Name: name,
		}
		s.workers[uuid] = info
	}

	info.Status = "idle"
	info.Online = true
	info.Addr = addr
	info.Capabilities = caps
	info.RunningTasks = 0
	now := time.Now()
	info.ConnectedAt = now
	info.LastBeat = now
	s.lastBeat[uuid] = now
	return info
}

// SetConn associates a WebSocket connection with a Worker.
func (s *WorkerStore) SetConn(uuid string, conn *SafeConn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.conns[uuid] = conn
}

// GetConn returns the WebSocket connection for a Worker.
func (s *WorkerStore) GetConn(uuid string) *SafeConn {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.conns[uuid]
}

// Disconnect marks a Worker as offline and removes its connection.
func (s *WorkerStore) Disconnect(uuid string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if info, ok := s.workers[uuid]; ok {
		info.Online = false
		info.Status = "offline"
	}
	if conn, ok := s.conns[uuid]; ok {
		conn.Close()
		delete(s.conns, uuid)
	}
}

// Heartbeat updates the last heartbeat time for a Worker.
func (s *WorkerStore) Heartbeat(uuid string, hb protocol.HeartbeatParams) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if info, ok := s.workers[uuid]; ok {
		now := time.Now()
		info.LastBeat = now
		info.CPUPercent = hb.CPUPercent
		info.MemPercent = hb.MemPercent
		info.DiskPercent = hb.DiskPercent
		info.CurrentTask = hb.CurrentTask
		info.Phase = hb.Phase
		info.Progress = hb.Progress
		s.lastBeat[uuid] = now
	}
}

// SetWorkerStatus updates a Worker's status string.
func (s *WorkerStore) SetWorkerStatus(uuid, status string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if info, ok := s.workers[uuid]; ok {
		info.Status = status
	}
}

// setWorkerStatus is the unexported version for internal use.
func (s *WorkerStore) setWorkerStatus(uuid, status string) {
	s.SetWorkerStatus(uuid, status)
}
func (s *WorkerStore) UpdateProgress(uuid string, report protocol.ReportParams) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if info, ok := s.workers[uuid]; ok {
		info.Phase = report.Phase
		info.Progress = report.Progress
		info.CurrentTask = report.TaskID
	}
}

// IncrementRunningTasks increases the Worker's running task count.
func (s *WorkerStore) IncrementRunningTasks(uuid string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if info, ok := s.workers[uuid]; ok {
		info.RunningTasks++
	}
}

// DecrementRunningTasks decreases the Worker's running task count.
func (s *WorkerStore) DecrementRunningTasks(uuid string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if info, ok := s.workers[uuid]; ok {
		if info.RunningTasks > 0 {
			info.RunningTasks--
		}
	}
}

// ResetRunningTasks sets a Worker's running task count to 0.
// Called when a Worker disconnects.
func (s *WorkerStore) ResetRunningTasks(uuid string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if info, ok := s.workers[uuid]; ok {
		info.RunningTasks = 0
	}
}

// List returns all known Workers.
func (s *WorkerStore) List() []*WorkerInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*WorkerInfo, 0, len(s.workers))
	for _, info := range s.workers {
		copy := *info
		result = append(result, &copy)
	}
	return result
}

// GetByUUID returns a single Worker by UUID.
func (s *WorkerStore) GetByUUID(uuid string) *WorkerInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if info, ok := s.workers[uuid]; ok {
		copy := *info
		return &copy
	}
	return nil
}

// GetOnlineUUIDs returns UUIDs of all currently connected Workers.
func (s *WorkerStore) GetOnlineUUIDs() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var uuids []string
	for u := range s.conns {
		uuids = append(uuids, u)
	}
	return uuids
}

// offlineDetector runs every 30s and marks Workers as offline
// if they haven't sent a heartbeat in 45s.
func (s *WorkerStore) offlineDetector() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.Lock()
		now := time.Now()
		for uuid, last := range s.lastBeat {
			if now.Sub(last) > 45*time.Second {
				if info, ok := s.workers[uuid]; ok && info.Online {
					info.Online = false
					info.Status = "offline"
					log.Printf("[master] Worker %s (%s) marked offline (no heartbeat for 45s)", uuid, info.Name)
					delete(s.conns, uuid)
				}
			}
		}
		s.mu.Unlock()
	}
}
