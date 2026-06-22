package master

import (
	"encoding/json"
	"log"
	"time"

	"github.com/e13815332/multiscan/internal/protocol"
)

// TaskScheduler assigns pending tasks to idle Workers.
type TaskScheduler struct {
	store  *WorkerStore
	tasks  *TaskStore
	events *EventQueueStore
}

func NewTaskScheduler(store *WorkerStore, tasks *TaskStore, events *EventQueueStore) *TaskScheduler {
	s := &TaskScheduler{
		store:  store,
		tasks:  tasks,
		events: events,
	}
	go s.loop()
	return s
}

// loop runs the scheduler every 5 seconds.
func (s *TaskScheduler) loop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		s.Tick()
	}
}

// Tick runs one scheduling pass: assigns pending tasks to idle workers.
// Exported so the handler can trigger immediate dispatch on worker pull.
func (s *TaskScheduler) Tick() {
	idleWorkers := s.getIdleWorkers()
	if len(idleWorkers) == 0 {
		return
	}

	for {
		task := s.tasks.GetPending()
		if task == nil {
			return
		}
		if len(idleWorkers) == 0 {
			return
		}

		worker := idleWorkers[0]
		idleWorkers = idleWorkers[1:]

		assigned := s.tasks.AssignTask(task.ID, worker.UUID)
		if assigned == nil {
			continue
		}

		// Send task assignment to Worker
		params := protocol.TaskAssignParams{
			TaskID:     task.ID,
			ASNs:       task.ASNs,
			CIDRs:      task.CIDRs,
			Ports:      task.Ports,
			MaxRate:    task.MaxRate,
			ShardIndex: task.ShardIndex,
			ShardTotal: task.ShardTotal,
		}
		req, err := protocol.NewRequest(protocol.MethodMasterTaskAssign, params, 0)
		if err != nil {
			log.Printf("[scheduler] encode error: %v", err)
			s.tasks.ReassignTask(task.ID)
			continue
		}
		data, err := json.Marshal(req)
		if err != nil {
			log.Printf("[scheduler] marshal error: %v", err)
			s.tasks.ReassignTask(task.ID)
			continue
		}

		conn := s.store.GetConn(worker.UUID)
		if conn == nil || conn.Write(data) != nil {
			log.Printf("[scheduler] Failed to send task %s to worker %s", task.ID, worker.UUID)
			s.tasks.ReassignTask(task.ID)
			continue
		}

		// Mark worker as busy immediately so next tick doesn't reassign
		info := s.store.GetByUUID(worker.UUID)
		if info != nil {
			info.CurrentTask = task.ID
			info.Status = "busy"
			info.Phase = "assigned"
			info.Progress = "0%"
		}
		// Track running task count for capacity-based scheduling
		s.store.IncrementRunningTasks(worker.UUID)

		log.Printf("[scheduler] Dispatched %s to %s (%s)", task.ID, worker.Name, worker.UUID)
	}
}

// OnWorkerDisconnect handles Worker disconnection: reassigns tasks and resets load.
func (s *TaskScheduler) OnWorkerDisconnect(workerUUID string) {
	count := s.tasks.ReassignWorkerTasks(workerUUID)
	if count > 0 {
		log.Printf("[scheduler] Reassigned %d tasks from disconnected worker %s", count, workerUUID)
	}
	// Reset running task count — worker is gone, no more load
	s.store.ResetRunningTasks(workerUUID)
}

// OnWorkerConnect sends any queued events to a reconnecting Worker.
func (s *TaskScheduler) OnWorkerConnect(workerUUID string) {
	q := s.events.GetOrCreate(workerUUID)
	pending := q.PopPending()
	if len(pending) > 0 {
		log.Printf("[scheduler] Sending %d queued events to reconnected worker %s", len(pending), workerUUID)
		conn := s.store.GetConn(workerUUID)
		if conn != nil {
			for _, evt := range pending {
				req, err := protocol.NewRequest(evt.Method, evt.Params, 0)
				if err != nil {
					continue
				}
				data, _ := json.Marshal(req)
				conn.Write(data)
			}
		}
	}
}

func (s *TaskScheduler) getIdleWorkers() []*WorkerInfo {
	all := s.store.List()
	var idle []*WorkerInfo
	for _, w := range all {
		if !w.Online {
			continue
		}
		// Check capacity: running tasks must be below hardware limit
		maxTasks := 1
		if w.Capabilities != nil && w.Capabilities.MaxTasks > 0 {
			maxTasks = w.Capabilities.MaxTasks
		}
		if w.RunningTasks < maxTasks {
			idle = append(idle, w)
		}
	}
	return idle
}
