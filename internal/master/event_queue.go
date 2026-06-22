package master

import (
	"sync"
	"time"
)

const (
	maxEventQueueSize  = 128
	defaultEventTTL    = 5 * time.Minute
	taskCancelEventTTL = 30 * time.Second
)

// Event is a message queued for an offline Worker.
type Event struct {
	ID        string    `json:"id"`
	Method    string    `json:"method"`
	Params    any       `json:"params,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

// EventQueue holds pending events for a single Worker that was offline.
type EventQueue struct {
	mu     sync.Mutex
	events []Event
	nextID int
}

func NewEventQueue() *EventQueue {
	return &EventQueue{events: make([]Event, 0, maxEventQueueSize)}
}

// Push adds an event to the queue. Drops oldest if full.
func (q *EventQueue) Push(method string, params any, ttl time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.nextID++
	now := time.Now()

	evt := Event{
		ID:        method + "_" + itoa(q.nextID),
		Method:    method,
		Params:    params,
		CreatedAt: now,
		ExpiresAt: now.Add(ttl),
	}

	if len(q.events) >= maxEventQueueSize {
		// Drop oldest
		q.events = q.events[1:]
	}
	q.events = append(q.events, evt)
}

// PopPending returns all non-expired events and removes them.
func (q *EventQueue) PopPending() []Event {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now()
	var pending []Event
	var remaining []Event

	for _, e := range q.events {
		if now.After(e.ExpiresAt) {
			continue // expired, drop
		}
		pending = append(pending, e)
	}

	q.events = remaining // clear all processed events
	return pending
}

// Ack removes acknowledged events by ID.
func (q *EventQueue) Ack(ids []string) {
	if len(ids) == 0 {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	var remaining []Event
	for _, e := range q.events {
		if !idSet[e.ID] {
			remaining = append(remaining, e)
		}
	}
	q.events = remaining
}

// Len returns the number of events in the queue.
func (q *EventQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.events)
}

// EventQueueStore manages event queues per Worker.
type EventQueueStore struct {
	mu      sync.RWMutex
	queues  map[string]*EventQueue
}

func NewEventQueueStore() *EventQueueStore {
	return &EventQueueStore{
		queues: make(map[string]*EventQueue),
	}
}

// GetOrCreate returns the event queue for a Worker, creating if needed.
func (s *EventQueueStore) GetOrCreate(workerUUID string) *EventQueue {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, ok := s.queues[workerUUID]
	if !ok {
		q = NewEventQueue()
		s.queues[workerUUID] = q
	}
	return q
}

// Remove deletes a Worker's event queue.
func (s *EventQueueStore) Remove(workerUUID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.queues, workerUUID)
}

// PushEvent pushes an event to a specific Worker's queue (if exists).
func (s *EventQueueStore) PushEvent(workerUUID, method string, params any, ttl time.Duration) {
	q := s.GetOrCreate(workerUUID)
	q.Push(method, params, ttl)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
