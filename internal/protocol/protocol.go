// Package protocol defines JSON-RPC v2 message types for Worker ↔ Master communication.
package protocol

import "encoding/json"

// Version is the JSON-RPC version string.
const Version = "2.0"

// --- Request / Response ---

// Request is a JSON-RPC v2 request.
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
	ID      int64           `json:"id"`
}

// Response is a JSON-RPC v2 response.
type Response struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *Error          `json:"error,omitempty"`
	ID      int64           `json:"id"`
}

// Error represents a JSON-RPC v2 error object.
type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Standard JSON-RPC error codes.
const (
	ErrCodeParse     = -32700
	ErrCodeInvalidReq = -32600
	ErrCodeMethodNotFound = -32601
	ErrCodeInvalidParams = -32602
	ErrCodeInternal = -32603
)

// NewRequest creates a JSON-RPC v2 request.
func NewRequest(method string, params any, id int64) (Request, error) {
	var raw json.RawMessage
	if params != nil {
		b, err := json.Marshal(params)
		if err != nil {
			return Request{}, err
		}
		raw = b
	}
	return Request{
		JSONRPC: Version,
		Method:  method,
		Params:  raw,
		ID:      id,
	}, nil
}

// NewResponse creates a successful JSON-RPC v2 response.
func NewResponse(result any, id int64) (Response, error) {
	var raw json.RawMessage
	if result != nil {
		b, err := json.Marshal(result)
		if err != nil {
			return Response{}, err
		}
		raw = b
	}
	return Response{
		JSONRPC: Version,
		Result:  raw,
		ID:      id,
	}, nil
}

// NewErrorResponse creates an error JSON-RPC v2 response.
func NewErrorResponse(code int, message string, id int64) Response {
	return Response{
		JSONRPC: Version,
		Error:   &Error{Code: code, Message: message},
		ID:      id,
	}
}

// --- Method constants ---

const (
	// Worker → Master
	MethodWorkerRegister  = "worker.register"
	MethodWorkerPull      = "worker.pull"
	MethodWorkerReport    = "worker.report"
	MethodWorkerResult    = "worker.result"
	MethodWorkerHeartbeat = "worker.heartbeat"
	MethodWorkerPong      = "worker.pong"

	// Master → Worker (events)
	MethodMasterTaskAssign = "master.task_assign"
	MethodMasterTaskCancel = "master.task_cancel"
	MethodMasterPing       = "master.ping"
	MethodMasterNotify     = "master.notify"
)

// --- Params / Result types ---

// WorkerCapabilities describes the Worker's hardware limits.
// Used by Master to avoid overloading the Worker.
type WorkerCapabilities struct {
	CPUCount      int `json:"cpu_count"`      // runtime.NumCPU()
	MemoryMB      int `json:"memory_mb"`      // /proc/meminfo MemTotal / 1024
	MaxTasks      int `json:"max_tasks"`      // max(1, memory_mb/512)
	MaxConcurrent int `json:"max_concurrent"` // min(200, max(50, cpu_count*50))
	MaxRate       int `json:"max_rate"`       // 14000 (masscan hard cap)
}

// RegisterParams is sent by Worker on first connection.
type RegisterParams struct {
	UUID         string              `json:"uuid"`
	Name         string              `json:"name"`
	Capabilities *WorkerCapabilities `json:"capabilities,omitempty"`
}

// RegisterResult is returned by Master after registration.
type RegisterResult struct {
	UUID  string `json:"uuid"`
	Token string `json:"token"`
}

// HeartbeatParams is sent periodically by Worker.
type HeartbeatParams struct {
	CPUPercent  float64 `json:"cpu_percent"`
	MemPercent  float64 `json:"memory_percent"`
	DiskPercent float64 `json:"disk_percent"`
	CurrentTask string  `json:"current_task,omitempty"`
	Phase       string  `json:"phase,omitempty"`
	Progress    string  `json:"progress,omitempty"`
}

// ReportParams is sent by Worker on phase change or periodically.
type ReportParams struct {
	TaskID       string   `json:"task_id"`
	Phase        string   `json:"phase"`
	Progress     string   `json:"progress"`
	ScannedIPs   int      `json:"scanned_ips"`
	TotalIPs     int      `json:"total_ips"`
	Hits         int      `json:"hits"`
	AckEventIDs  []string `json:"ack_event_ids,omitempty"`
}

// PullParams is sent by Worker to request a task.
type PullParams struct {
	Status string `json:"status"` // "idle" or "completed"
}

// TaskAssignParams is sent by Master to assign a task.
type TaskAssignParams struct {
	TaskID     string   `json:"task_id"`
	ASNs       []string `json:"asns"`
	CIDRs      []string `json:"cidrs,omitempty"` // pre-resolved CIDRs (master-side), skips ASN resolution
	Ports      []int    `json:"ports"`
	MaxRate    int      `json:"max_rate"`
	IPs        []string `json:"ips,omitempty"` // optional pre-resolved IP list
	ShardIndex int      `json:"shard_index,omitempty"`
	ShardTotal int      `json:"shard_total,omitempty"`
}

// TaskCancelParams is sent by Master to cancel a task.
type TaskCancelParams struct {
	TaskID string `json:"task_id"`
	Reason string `json:"reason"`
}

// NotifyParams is a generic notification.
type NotifyParams struct {
	Level   string `json:"level"` // "info", "warn", "error"
	Message string `json:"message"`
}

// TaskResult is returned by Worker after task completion.
type TaskResult struct {
	TaskID     string        `json:"task_id"`
	Status     string        `json:"status"` // "completed", "failed", "cancelled"
	Hits       int           `json:"hits"`
	TotalIPs   int           `json:"total_ips"`
	Duration   string        `json:"duration"` // human-readable
	Results    []ScanEntry   `json:"results,omitempty"`
}

// ScanEntry is one scanned result line.
type ScanEntry struct {
	IP     string `json:"ip"`
	Port   int    `json:"port"`
	Status string `json:"status"` // "ok", "fail"
	Colo   string `json:"colo,omitempty"`
	ASN    string `json:"asn,omitempty"`
	Delay  int    `json:"delay,omitempty"` // ms
}

// --- Event queue types ---

// Event is a message queued for offline Worker delivery.
type Event struct {
	ID        string `json:"id"`
	Method    string `json:"method"`
	Params    any    `json:"params,omitempty"`
	CreatedAt int64  `json:"created_at"` // unix timestamp
	ExpiresAt int64  `json:"expires_at"` // unix timestamp, 0 = no expiry
}
