package master

import (
	"encoding/json"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

var dashboardUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// DashboardHub manages browser WebSocket connections for the live panel.
type DashboardHub struct {
	mu      sync.RWMutex
	clients map[*SafeConn]bool
	store   *WorkerStore
	tasks   *TaskStore
}

func NewDashboardHub(store *WorkerStore, tasks *TaskStore) *DashboardHub {
	return &DashboardHub{
		clients: make(map[*SafeConn]bool),
		store:   store,
		tasks:   tasks,
	}
}

// HandleWS handles browser WebSocket connections.
func (h *DashboardHub) HandleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := dashboardUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	safeConn := NewSafeConn(func(data []byte) error {
		return conn.WriteMessage(websocket.TextMessage, data)
	})

	h.mu.Lock()
	h.clients[safeConn] = true
	h.mu.Unlock()

	// Send initial state
	h.sendInit(safeConn)

	// Keep connection alive by reading (detect disconnect)
	go func() {
		defer func() {
			h.mu.Lock()
			delete(h.clients, safeConn)
			h.mu.Unlock()
			safeConn.Close()
		}()
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
		}
	}()
}

func (h *DashboardHub) sendInit(conn *SafeConn) {
	wList := h.store.List()
	tList := h.tasks.List()

	wMap := make(map[string]*WorkerInfo)
	for _, w := range wList {
		wMap[w.UUID] = w
	}
	tMap := make(map[string]*Task)
	for _, t := range tList {
		tMap[t.ID] = t
	}

	msg := map[string]any{
		"type":    "init",
		"workers": wMap,
		"tasks":   tMap,
	}
	data, _ := json.Marshal(msg)
	conn.Write(data)
}

func (h *DashboardHub) broadcast(v any) {
	data, err := json.Marshal(v)
	if err != nil {
		return
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	for conn := range h.clients {
		conn.Write(data)
	}
}

// BroadcastWorkerUpdate sends a worker status update to all dashboard clients.
func (h *DashboardHub) BroadcastWorkerUpdate(worker *WorkerInfo) {
	h.broadcast(map[string]any{
		"type":   "worker_update",
		"worker": worker,
	})
}

// BroadcastWorkerOnline sends a worker online event.
func (h *DashboardHub) BroadcastWorkerOnline(worker *WorkerInfo) {
	h.broadcast(map[string]any{
		"type":   "worker_online",
		"worker": worker,
	})
}

// BroadcastWorkerOffline sends a worker offline event.
func (h *DashboardHub) BroadcastWorkerOffline(uuid string) {
	h.broadcast(map[string]any{
		"type": "worker_offline",
		"uuid": uuid,
	})
}

// BroadcastTaskUpdate sends a task update to all dashboard clients.
func (h *DashboardHub) BroadcastTaskUpdate(task *Task) {
	h.broadcast(map[string]any{
		"type": "task_update",
		"task": task,
	})
}

// BroadcastTaskCompleted sends a task completed event.
func (h *DashboardHub) BroadcastTaskCompleted(task *Task) {
	h.broadcast(map[string]any{
		"type": "task_completed",
		"task": task,
	})
}

// BroadcastTaskCreated sends a task created event.
func (h *DashboardHub) BroadcastTaskCreated(task *Task) {
	h.broadcast(map[string]any{
		"type": "task_created",
		"task": task,
	})
}
