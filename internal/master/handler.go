package master

import (
	"encoding/json"
	"log"
	"net/http"
	"sync/atomic"

	"github.com/e13815332/multiscan/internal/protocol"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// Handler manages WebSocket connections from Workers.
type Handler struct {
	store     *WorkerStore
	tasks     *TaskStore
	scheduler *TaskScheduler
	dashboard *DashboardHub
	nextID    atomic.Int64
	tokenMap  map[string]string
}

func NewHandler(store *WorkerStore, tasks *TaskStore, scheduler *TaskScheduler, dashboard *DashboardHub) *Handler {
	return &Handler{
		store:     store,
		tasks:     tasks,
		scheduler: scheduler,
		dashboard: dashboard,
		tokenMap:  make(map[string]string),
	}
}

// HandleWorkerWS handles the Worker WebSocket upgrade and communication.
func (h *Handler) HandleWorkerWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[master] WebSocket upgrade error: %v", err)
		return
	}

	workerUUID := ""
	workerName := ""
	addr := r.RemoteAddr
	log.Printf("[master] New WebSocket connection from %s", addr)

	safeConn := NewSafeConn(func(data []byte) error {
		return conn.WriteMessage(websocket.TextMessage, data)
	})

	for {
		_, msgBytes, err := conn.ReadMessage()
		if err != nil {
			log.Printf("[master] Worker %s (%s) disconnected: %v", workerUUID, workerName, err)
			if workerUUID != "" {
				h.store.Disconnect(workerUUID)
				h.scheduler.OnWorkerDisconnect(workerUUID)
				if h.dashboard != nil {
					h.dashboard.BroadcastWorkerOffline(workerUUID)
				}
			}
			safeConn.Close()
			return
		}

		var req protocol.Request
		if err := json.Unmarshal(msgBytes, &req); err != nil {
			resp := protocol.NewErrorResponse(protocol.ErrCodeParse, "Parse error", 0)
			h.sendJSON(safeConn, resp)
			continue
		}

		h.handleMessage(safeConn, req, &workerUUID, &workerName)
	}
}

func (h *Handler) handleMessage(conn *SafeConn, req protocol.Request, workerUUID, workerName *string) {
	switch req.Method {
	case protocol.MethodWorkerRegister:
		h.handleRegister(conn, req, workerUUID, workerName)
	case protocol.MethodWorkerHeartbeat:
		h.handleHeartbeat(req, workerUUID)
	case protocol.MethodWorkerReport:
		h.handleReport(req, workerUUID)
	case protocol.MethodWorkerResult:
		h.handleResult(req, workerUUID)
	case protocol.MethodWorkerPull:
		h.handlePull(conn, req, workerUUID)
	case protocol.MethodWorkerPong:
		// nothing to do
	default:
		resp := protocol.NewErrorResponse(protocol.ErrCodeMethodNotFound, "Method not found: "+req.Method, req.ID)
		h.sendJSON(conn, resp)
	}
}

func (h *Handler) handleRegister(conn *SafeConn, req protocol.Request, workerUUID, workerName *string) {
	var params protocol.RegisterParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		resp := protocol.NewErrorResponse(protocol.ErrCodeInvalidParams, "Invalid register params", req.ID)
		h.sendJSON(conn, resp)
		return
	}
	if params.UUID == "" {
		params.UUID = params.Name
	}

	info := h.store.Register(params.UUID, params.Name, "", params.Capabilities)
	*workerUUID = params.UUID
	*workerName = params.Name
	h.store.SetConn(params.UUID, conn)

	log.Printf("[master] Worker registered: %s (%s) caps=%+v", params.UUID, params.Name, params.Capabilities)

	// Notify scheduler
	h.scheduler.OnWorkerConnect(params.UUID)

	// Notify dashboard
	if h.dashboard != nil {
		h.dashboard.BroadcastWorkerOnline(info)
	}

	result := protocol.RegisterResult{UUID: info.UUID, Token: ""}
	resp, _ := protocol.NewResponse(result, req.ID)
	h.sendJSON(conn, resp)
}

func (h *Handler) handleHeartbeat(req protocol.Request, workerUUID *string) {
	if *workerUUID == "" {
		return
	}
	var params protocol.HeartbeatParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return
	}
	h.store.Heartbeat(*workerUUID, params)
}

func (h *Handler) handleReport(req protocol.Request, workerUUID *string) {
	if *workerUUID == "" {
		return
	}
	var params protocol.ReportParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return
	}

	h.store.UpdateProgress(*workerUUID, params)
	h.tasks.UpdateProgress(params.TaskID, params.Phase, params.Progress, params.ScannedIPs, params.TotalIPs, params.Hits)

	// Push to dashboard
	if h.dashboard != nil {
		task := h.tasks.GetByID(params.TaskID)
		if task != nil {
			h.dashboard.BroadcastTaskUpdate(task)
		}
		worker := h.store.GetByUUID(*workerUUID)
		if worker != nil {
			h.dashboard.BroadcastWorkerUpdate(worker)
		}
	}

	log.Printf("[master] Progress from %s (task %s): %s phase=%s hits=%d",
		*workerUUID, params.TaskID, params.Progress, params.Phase, params.Hits)
}

func (h *Handler) handleResult(req protocol.Request, workerUUID *string) {
	if *workerUUID == "" {
		return
	}
	var params protocol.TaskResult
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return
	}

	log.Printf("[master] Result from %s (task %s): status=%s hits=%d",
		*workerUUID, params.TaskID, params.Status, params.Hits)

	if params.Status == "completed" {
		h.tasks.CompleteTask(params.TaskID, params.Hits)
	} else {
		h.tasks.FailTask(params.TaskID)
	}

	// Push to dashboard
	if h.dashboard != nil {
		task := h.tasks.GetByID(params.TaskID)
		if task != nil {
			if task.Status == "completed" {
				h.dashboard.BroadcastTaskCompleted(task)
			} else {
				h.dashboard.BroadcastTaskUpdate(task)
			}
		}
	}

	// Decrement running task count (worker may still have other tasks)
	h.store.DecrementRunningTasks(*workerUUID)

	// Return success
	resp, _ := protocol.NewResponse(map[string]string{"ack": "ok"}, req.ID)
	h.sendJSON(h.store.GetConn(*workerUUID), resp)
}

func (h *Handler) handlePull(conn *SafeConn, req protocol.Request, workerUUID *string) {
	if *workerUUID == "" {
		return
	}

	// Worker is asking for a task — mark it idle
	h.store.setWorkerStatus(*workerUUID, "idle")

	// Respond with ack
	resp, _ := protocol.NewResponse(map[string]string{"status": "idle"}, req.ID)
	h.sendJSON(conn, resp)

	// Immediately try to dispatch a pending task (no waiting for 5s scheduler tick)
	h.scheduler.Tick()
}

func (h *Handler) sendJSON(conn *SafeConn, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		return
	}
	conn.Write(data)
}
