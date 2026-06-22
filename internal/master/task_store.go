package master

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// Task status constants.
const (
	TaskStatusPending   = "pending"
	TaskStatusAssigned  = "assigned"
	TaskStatusRunning   = "running"
	TaskStatusCompleted = "completed"
	TaskStatusFailed    = "failed"
	TaskStatusCancelled = "cancelled"
)

// Task represents a scanning task.
type Task struct {
	ID         string   `json:"id"`
	Name       string   `json:"name"`
	Status     string   `json:"status"`
	ASNs       []string `json:"asns"`
	CIDRs      []string `json:"cidrs,omitempty"` // pre-resolved CIDRs (skips ASN resolution)
	Ports      []int    `json:"ports"`
	MaxRate    int      `json:"max_rate"`
	AssignedTo string   `json:"assigned_to,omitempty"` // Worker UUID
	Progress   string   `json:"progress,omitempty"`
	Phase      string   `json:"phase,omitempty"`
	TotalIPs   int      `json:"total_ips"`
	ScannedIPs int      `json:"scanned_ips"`
	Hits       int      `json:"hits"`
	CreatedAt  int64    `json:"created_at"`
	AssignedAt int64    `json:"assigned_at,omitempty"`
	StartedAt  int64    `json:"started_at,omitempty"`
	CompletedAt int64   `json:"completed_at,omitempty"`
	AssignedCount int   `json:"assigned_count"` // how many times reassigned
	GroupID    string   `json:"group_id,omitempty"`    // shared across shards of a split task
	ShardIndex int      `json:"shard_index,omitempty"` // 0-based index within group
	ShardTotal int      `json:"shard_total,omitempty"` // total shards in group
}

// TaskStore manages all tasks.
type TaskStore struct {
	mu        sync.RWMutex
	tasks     map[string]*Task
	pendingQ  []string        // ordered queue of pending task IDs
	nextID    atomic.Int64
}

func NewTaskStore() *TaskStore {
	return &TaskStore{
		tasks: make(map[string]*Task),
	}
}

// CreateTask creates a new task and returns its ID.
func (s *TaskStore) CreateTask(name string, asns []string, ports []int, maxRate int, totalIPs int) *Task {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := fmt.Sprintf("t_%d", s.nextID.Add(1))
	now := time.Now().Unix()

	task := &Task{
		ID:        id,
		Name:      name,
		Status:    TaskStatusPending,
		ASNs:      asns,
		Ports:     ports,
		MaxRate:   maxRate,
		TotalIPs:  totalIPs,
		CreatedAt: now,
	}

	s.tasks[id] = task
	s.pendingQ = append(s.pendingQ, id)
	log.Printf("[task] Created %s: %s (ASNs=%v ports=%v)", id, name, asns, ports)
	return task
}

// CreateCIDRShards creates shard tasks with pre-resolved CIDR groups.
// Each shard gets its own subset of CIDRs (distributed by master).
// This is the new CIDR-based sharding: no IP generation, feeds CIDRs directly to masscan.
func (s *TaskStore) CreateCIDRShards(name string, asns []string, cidrGroups [][]string, ports []int, maxRate int) []*Task {
	groupID := fmt.Sprintf("g_%d", time.Now().UnixNano())
	shards := make([]*Task, len(cidrGroups))
	for i, cidrs := range cidrGroups {
		id := fmt.Sprintf("t_%d", s.nextID.Add(1))
		now := time.Now().Unix()
		task := &Task{
			ID:         id,
			Name:       fmt.Sprintf("%s-%d", name, i+1),
			Status:     TaskStatusPending,
			ASNs:       asns,
			CIDRs:      cidrs,
			Ports:      ports,
			MaxRate:    maxRate,
			TotalIPs:   len(cidrs),
			CreatedAt:  now,
			GroupID:    groupID,
			ShardIndex: i,
			ShardTotal: len(cidrGroups),
		}
		s.mu.Lock()
		s.tasks[id] = task
		s.pendingQ = append(s.pendingQ, id)
		s.mu.Unlock()
		log.Printf("[task] Created CIDR shard %s/%d: %s (%d CIDRs)", groupID, i, id, len(cidrs))
		shards[i] = task
	}
	return shards
}

// CreateShards splits a task into N shards based on IP count.
// Each shard gets all ASNs; IP range is determined by shard_index/shard_total at runtime.
// If shardsCount <= 1 or len(asns) <= 1, falls back to single task.
// For backward compatibility: if shardsCount == 0 and len(asns) > 1, splits by ASN.
func (s *TaskStore) CreateShards(name string, asns []string, ports []int, maxRate int, totalIPs int, shardsCount int) []*Task {
	// Legacy behavior: split by ASN when no shardsCount specified
	if shardsCount <= 0 {
		if len(asns) <= 1 {
			return []*Task{s.CreateTask(name, asns, ports, maxRate, totalIPs)}
		}
		// Old ASN-based sharding
		groupID := fmt.Sprintf("g_%d", time.Now().UnixNano())
		shards := make([]*Task, len(asns))
		for i, asn := range asns {
			id := fmt.Sprintf("t_%d", s.nextID.Add(1))
			now := time.Now().Unix()
			task := &Task{
				ID:         id,
				Name:       fmt.Sprintf("%s-%d", name, i+1),
				Status:     TaskStatusPending,
				ASNs:       []string{asn},
				Ports:      ports,
				MaxRate:    maxRate,
				TotalIPs:   totalIPs / len(asns),
				CreatedAt:  now,
				GroupID:    groupID,
				ShardIndex: i,
				ShardTotal: len(asns),
			}
			s.mu.Lock()
			s.tasks[id] = task
			s.pendingQ = append(s.pendingQ, id)
			s.mu.Unlock()
			log.Printf("[task] Created shard(legacy) %s/%d: %s (ASN=%v)", groupID, i, id, asn)
			shards[i] = task
		}
		return shards
	}

	// IP-count-based sharding: each shard gets ALL ASNs, splits IPs at runtime
	groupID := fmt.Sprintf("g_%d", time.Now().UnixNano())
	shards := make([]*Task, shardsCount)
	perShard := totalIPs / shardsCount
	if perShard < 100 {
		perShard = 100 // minimum per shard
	}
	for i := 0; i < shardsCount; i++ {
		id := fmt.Sprintf("t_%d", s.nextID.Add(1))
		now := time.Now().Unix()
		task := &Task{
			ID:         id,
			Name:       fmt.Sprintf("%s-%d", name, i+1),
			Status:     TaskStatusPending,
			ASNs:       asns,
			Ports:      ports,
			MaxRate:    maxRate,
			TotalIPs:   perShard,
			CreatedAt:  now,
			GroupID:    groupID,
			ShardIndex: i,
			ShardTotal: shardsCount,
		}
		s.mu.Lock()
		s.tasks[id] = task
		s.pendingQ = append(s.pendingQ, id)
		s.mu.Unlock()
		log.Printf("[task] Created shard(ip-split) %s/%d: %s (ASNs=%v)", groupID, i, id, asns)
		shards[i] = task
	}
	return shards
}

// AssignTask assigns a pending task to a Worker.
func (s *TaskStore) AssignTask(taskID, workerUUID string) *Task {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, ok := s.tasks[taskID]
	if !ok || task.Status != TaskStatusPending {
		return nil
	}

	now := time.Now().Unix()
	task.Status = TaskStatusAssigned
	task.AssignedTo = workerUUID
	task.AssignedAt = now
	task.AssignedCount++

	// Remove from pending queue
	for i, id := range s.pendingQ {
		if id == taskID {
			s.pendingQ = append(s.pendingQ[:i], s.pendingQ[i+1:]...)
			break
		}
	}

	log.Printf("[task] Assigned %s to worker %s (attempt #%d)", taskID, workerUUID, task.AssignedCount)
	return task
}

// StartTask marks a task as running (Worker reports it started).
func (s *TaskStore) StartTask(taskID string) *Task {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, ok := s.tasks[taskID]
	if !ok {
		return nil
	}
	if task.Status == TaskStatusAssigned {
		task.Status = TaskStatusRunning
		task.StartedAt = time.Now().Unix()
	}
	return task
}

// UpdateProgress updates task progress from a Worker report.
func (s *TaskStore) UpdateProgress(taskID, phase, progress string, scannedIPs, totalIPs, hits int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, ok := s.tasks[taskID]
	if !ok {
		return
	}
	task.Phase = phase
	task.Progress = progress
	task.ScannedIPs = scannedIPs
	if totalIPs > 0 {
		task.TotalIPs = totalIPs
	}
	task.Hits = hits
}

// CompleteTask marks a task as completed.
func (s *TaskStore) CompleteTask(taskID string, hits int) *Task {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, ok := s.tasks[taskID]
	if !ok {
		return nil
	}
	task.Status = TaskStatusCompleted
	task.Hits = hits
	task.CompletedAt = time.Now().Unix()
	log.Printf("[task] Completed %s: %d hits", taskID, hits)
	return task
}

// CancelTask cancels a task and notifies its assigned worker.
func (s *TaskStore) CancelTask(taskID string) *Task {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, ok := s.tasks[taskID]
	if !ok {
		return nil
	}
	if task.Status == TaskStatusCompleted || task.Status == TaskStatusCancelled || task.Status == TaskStatusFailed {
		return nil
	}

	wasPending := task.Status == TaskStatusPending
	task.Status = TaskStatusCancelled
	task.CompletedAt = time.Now().Unix()
	log.Printf("[task] Cancelled %s", taskID)

	// Remove from pending queue if was pending
	if wasPending {
		for i, id := range s.pendingQ {
			if id == taskID {
				s.pendingQ = append(s.pendingQ[:i], s.pendingQ[i+1:]...)
				break
			}
		}
	}

	return task
}

// CancelGroup cancels all tasks in a group.
func (s *TaskStore) CancelGroup(groupID string) []*Task {
	tasks := s.GetByGroupID(groupID)
	var cancelled []*Task
	for _, t := range tasks {
		if c := s.CancelTask(t.ID); c != nil {
			cancelled = append(cancelled, c)
		}
	}
	return cancelled
}

func (s *TaskStore) FailTask(taskID string) *Task {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, ok := s.tasks[taskID]
	if !ok {
		return nil
	}
	task.Status = TaskStatusFailed
	task.CompletedAt = time.Now().Unix()
	log.Printf("[task] Failed %s", taskID)
	return task
}

// ReassignTask puts a task back to pending when Worker goes offline.
func (s *TaskStore) ReassignTask(taskID string) *Task {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, ok := s.tasks[taskID]
	if !ok {
		return nil
	}
	if task.Status != TaskStatusAssigned && task.Status != TaskStatusRunning {
		return nil
	}

	// Only reassign if assigned or running
	task.Status = TaskStatusPending
	task.AssignedTo = ""
	s.pendingQ = append(s.pendingQ, taskID)

	log.Printf("[task] Reassigned %s back to pending (attempt #%d)", taskID, task.AssignedCount)
	return task
}

// GetPending returns the next pending task (does not assign).
func (s *TaskStore) GetPending() *Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.pendingQ) == 0 {
		return nil
	}
	return s.tasks[s.pendingQ[0]]
}

// PopPending removes and returns the next pending task.
func (s *TaskStore) PopPending() *Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.pendingQ) == 0 {
		return nil
	}
	id := s.pendingQ[0]
	s.pendingQ = s.pendingQ[1:]
	task := s.tasks[id]
	return task
}

// GetByID returns a task by ID.
func (s *TaskStore) GetByID(id string) *Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	task, ok := s.tasks[id]
	if !ok {
		return nil
	}
	copy := *task
	return &copy
}

// GetByGroupID returns all tasks in a group.
func (s *TaskStore) GetByGroupID(groupID string) []*Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*Task
	for _, t := range s.tasks {
		if t.GroupID == groupID {
			copy := *t
			result = append(result, &copy)
		}
	}
	return result
}

// List returns all tasks.
func (s *TaskStore) List() []*Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*Task, 0, len(s.tasks))
	for _, t := range s.tasks {
		copy := *t
		result = append(result, &copy)
	}
	return result
}

// GetWorkerActiveTasks returns tasks assigned/running for a specific Worker.
func (s *TaskStore) GetWorkerActiveTasks(workerUUID string) []*Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*Task
	for _, t := range s.tasks {
		if t.AssignedTo == workerUUID && (t.Status == TaskStatusAssigned || t.Status == TaskStatusRunning) {
			copy := *t
			result = append(result, &copy)
		}
	}
	return result
}

// ReassignWorkerTasks puts all active tasks for a Worker back to pending.
// Called when Worker disconnects.
func (s *TaskStore) ReassignWorkerTasks(workerUUID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := 0
	now := time.Now()
	for _, t := range s.tasks {
		if t.AssignedTo == workerUUID && (t.Status == TaskStatusAssigned || t.Status == TaskStatusRunning) {
			// Only reassign if assigned more than 3 minutes ago
			if t.Status == TaskStatusRunning && now.Unix()-t.StartedAt < 180 {
				continue // give worker 3 min grace period to reconnect
			}
			t.Status = TaskStatusPending
			t.AssignedTo = ""
			s.pendingQ = append(s.pendingQ, t.ID)
			count++
			log.Printf("[task] Reassigned %s (was on offline worker %s)", t.ID, workerUUID)
		}
	}
	return count
}

// TaskCounts returns counts by status.
func (s *TaskStore) TaskCounts() map[string]int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	counts := map[string]int{
		TaskStatusPending:   0,
		TaskStatusAssigned:  0,
		TaskStatusRunning:   0,
		TaskStatusCompleted: 0,
		TaskStatusFailed:    0,
		TaskStatusCancelled: 0,
	}
	for _, t := range s.tasks {
		counts[t.Status]++
	}
	return counts
}
