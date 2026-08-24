// Package model defines the core data types shared across the Queuemaxxing
// queue: the durable Message record, queue ordering modes, message lifecycle
// statuses, and the HTTP request/response DTOs.
//
// This package is dependency-free (standard library only) and knows nothing
// about HTTP, locking, or storage. See ARCHITECTURE.md for how these types
// flow through the system.
package model

import (
	"errors"
	"fmt"
	"time"
)

// QueueMode selects the tie-breaking order used when multiple visible
// messages share the highest priority.
type QueueMode string

const (
	// ModeFIFO breaks priority ties by smallest sequence number
	// (earliest EnqueuedAt): first in, first out.
	ModeFIFO QueueMode = "FIFO"

	// ModeLIFO breaks priority ties by largest sequence number
	// (latest EnqueuedAt): last in, first out.
	ModeLIFO QueueMode = "LIFO"
)

// Valid reports whether m is a recognized queue mode.
func (m QueueMode) Valid() bool {
	return m == ModeFIFO || m == ModeLIFO
}

// ParseQueueMode converts a string (e.g. from a flag or config file) into a
// QueueMode, returning an error for unknown values.
func ParseQueueMode(s string) (QueueMode, error) {
	m := QueueMode(s)
	if !m.Valid() {
		return "", fmt.Errorf("invalid queue mode %q: must be %q or %q", s, ModeFIFO, ModeLIFO)
	}
	return m, nil
}

// Status is the lifecycle state of a message.
type Status string

const (
	// StatusPending means the message is stored and will become a dequeue
	// candidate once time.Now() >= AvailableAt.
	StatusPending Status = "PENDING"

	// StatusInflight means the message has been delivered to a consumer and
	// is awaiting acknowledgement. It is invisible to other consumers.
	StatusInflight Status = "INFLIGHT"

	// StatusAcked means the message has been acknowledged and consumed.
	// Acked messages are deleted from live state; the status exists for WAL
	// records and observability.
	StatusAcked Status = "ACKED"
)

// Message is the canonical queue record. It is the unit of durability: the
// WAL persists messages in this shape, and recovery reconstructs them
// exactly.
type Message struct {
	// ID uniquely identifies the message. Assigned by the server at enqueue
	// time and used by consumers to acknowledge delivery.
	ID string `json:"id"`

	// Seq is a strictly monotonic sequence number assigned under the queue
	// lock at enqueue time. It is the authoritative total order for
	// tie-breaking (timestamps may collide; Seq never does).
	Seq uint64 `json:"seq"`

	// Payload is the opaque message body supplied by the producer.
	Payload string `json:"payload"`

	// Priority orders dequeues: higher integers are dequeued first.
	Priority int `json:"priority"`

	// EnqueuedAt is the server-assigned time the enqueue was accepted.
	EnqueuedAt time.Time `json:"enqueued_at"`

	// AvailableAt is the earliest time the message may be dequeued. The
	// message is invisible to consumers while time.Now() < AvailableAt.
	// Equal to EnqueuedAt when no delay was requested.
	AvailableAt time.Time `json:"available_at"`

	// Status is the current lifecycle state.
	Status Status `json:"status"`
}

// Available reports whether the message is visible to consumers at the given
// instant. Callers pass an injected clock's now for deterministic tests.
func (m *Message) Available(now time.Time) bool {
	return !now.Before(m.AvailableAt)
}

// EnqueueRequest is the JSON body of POST /enqueue.
type EnqueueRequest struct {
	// Payload is the message body. Required; must be non-empty.
	Payload string `json:"payload"`

	// Priority orders dequeues (higher first). Optional; defaults to 0.
	Priority int `json:"priority"`

	// DelaySeconds keeps the message invisible for this many seconds after
	// enqueue. Optional; defaults to 0 (immediately available). Must be >= 0.
	DelaySeconds int64 `json:"delay_seconds"`
}

// Validate checks the request at the system boundary and returns a
// client-facing error describing the first problem found.
func (r *EnqueueRequest) Validate() error {
	if r.Payload == "" {
		return errors.New("payload must not be empty")
	}
	if r.DelaySeconds < 0 {
		return fmt.Errorf("delay_seconds must be >= 0, got %d", r.DelaySeconds)
	}
	return nil
}

// EnqueueResponse is the JSON body returned by POST /enqueue.
type EnqueueResponse struct {
	// ID of the newly created message.
	ID string `json:"id"`

	// AvailableAt is when the message becomes visible to consumers.
	AvailableAt time.Time `json:"available_at"`
}

// DequeueResponse is the JSON body returned by POST /dequeue when a message
// is available. An empty queue returns 204 No Content with no body.
type DequeueResponse struct {
	// ID identifies the delivered message; consumers pass it to POST /ack.
	ID string `json:"id"`

	// Payload is the message body to process.
	Payload string `json:"payload"`

	// Priority the message was enqueued with.
	Priority int `json:"priority"`

	// EnqueuedAt is when the message was originally accepted.
	EnqueuedAt time.Time `json:"enqueued_at"`
}

// DequeueResponseFrom builds the consumer-facing view of a message.
func DequeueResponseFrom(m *Message) DequeueResponse {
	return DequeueResponse{
		ID:         m.ID,
		Payload:    m.Payload,
		Priority:   m.Priority,
		EnqueuedAt: m.EnqueuedAt,
	}
}

// QueueStats is a point-in-time snapshot of queue occupancy, returned by
// GET /stats and QueueService.Stats.
type QueueStats struct {
	// Total is the number of messages currently held, both visible and
	// still waiting on AvailableAt.
	Total int `json:"total"`

	// Available is the number of PENDING messages eligible for dequeue
	// at the instant the snapshot was taken (AvailableAt <= now).
	Available int `json:"available"`

	// Delayed is the number of messages that are stored but not yet
	// visible to consumers (Total - Available).
	Delayed int `json:"delayed"`
}
