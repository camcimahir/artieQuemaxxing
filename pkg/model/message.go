// Package model defines the core data types shared across the Queuemaxxing
// queue: the durable Message record, queue ordering modes, message lifecycle
// statuses, and the HTTP request/response DTOs.
//
// This package is dependency-free (standard library only) and knows nothing
// about HTTP, locking, or storage. See ARCHITECTURE.md for how these types
// flow through the system.
package model

import (
	"encoding/json"
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
//
// Queuemaxxing v1 has exactly one live state. A message is PENDING from the
// moment its ENQUEUE record is durable until the moment its DEQUEUE record
// is durable, at which point it is deleted rather than transitioning to
// another state — delivery is the point of consumption. See README §"Delivery
// contract" for why, and Design Q&A #3 for the lease/ACK design that would
// add INFLIGHT and ACKED states.
//
// The field is persisted in every ENQUEUE record so that a future release can
// introduce further states without a log-format version bump. Recovery
// defaults a missing status to PENDING; any other value is preserved so a
// newer writer's records still replay under this build.
type Status string

const (
	// StatusPending means the message is stored and will become a dequeue
	// candidate once time.Now() >= AvailableAt. It is the only status a
	// message ever holds in this release.
	StatusPending Status = "PENDING"
)

// Message is the canonical queue record. It is the unit of durability: the
// WAL persists messages in this shape, and recovery reconstructs them
// exactly.
type Message struct {
	// ID uniquely identifies the message. Assigned by the server at enqueue
	// time. Dequeue is identified by this ID in the WAL tombstone; there is
	// no separate acknowledgement step in this release.
	ID string `json:"id"`

	// Seq is a strictly monotonic sequence number assigned under the queue
	// lock at enqueue time. It is the authoritative total order for
	// tie-breaking (timestamps may collide; Seq never does).
	Seq uint64 `json:"seq"`

	// Payload is the opaque message body supplied by the producer.
	Payload string `json:"payload"`

	// Priority orders dequeues: higher integers are dequeued first.
	Priority int `json:"priority"`

	// EnqueuedAt is the server-assigned UTC time the enqueue was accepted.
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

// MaxDelaySeconds is the largest accepted delay_seconds: ten years.
//
// The cap is a correctness requirement, not a policy preference.
// AvailableAt is computed as now.Add(DelaySeconds * time.Second), and
// time.Duration is an int64 count of nanoseconds, so any delay beyond
// roughly 9.2e9 seconds (~292 years) overflows the multiplication and wraps
// negative — which would silently make a "far future" message immediately
// visible, the exact opposite of what the caller asked for. Ten years is
// comfortably inside the safe range and far beyond any real use.
const MaxDelaySeconds int64 = 10 * 365 * 24 * 60 * 60

// Validate checks the request at the system boundary and returns a
// client-facing error describing the first problem found.
func (r *EnqueueRequest) Validate() error {
	if r.Payload == "" {
		return errors.New("payload must not be empty")
	}
	if r.DelaySeconds < 0 {
		return fmt.Errorf("delay_seconds must be >= 0, got %d", r.DelaySeconds)
	}
	if r.DelaySeconds > MaxDelaySeconds {
		return fmt.Errorf("delay_seconds must be <= %d (ten years), got %d", MaxDelaySeconds, r.DelaySeconds)
	}
	return nil
}

// QueueStats is a point-in-time snapshot of queue occupancy, returned by
// GET /stats and QueueService.Stats.
type QueueStats struct {
	// Total is the number of messages currently held, both visible and
	// still waiting on AvailableAt.
	Total int `json:"total"`

	// Available is the number of PENDING messages eligible for dequeue at
	// the instant the snapshot was taken (AvailableAt <= now).
	//
	// The Go field is named for the message-level vocabulary (AvailableAt,
	// AvailableLen) while the JSON field is named for the structure the
	// console renders (the ready heap). Both endpoints that expose this
	// number — GET /stats and GET /messages — therefore spell it "ready",
	// so a client never has to learn two words for one count.
	Available int `json:"ready"`

	// Delayed is the number of messages that are stored but not yet
	// visible to consumers (Total - Available).
	Delayed int `json:"delayed"`
}

// QueueSnapshot is the read-only view of queue contents returned by
// GET /messages and QueueService.Snapshot. It is what the web console
// renders: the two heaps, side by side, exactly as the engine holds them.
//
// Ready and Delayed are captured under a single lock acquisition together
// with Stats, so the counts and the lists can never disagree.
type QueueSnapshot struct {
	// Mode is the ordering mode this queue was opened with.
	Mode QueueMode `json:"mode"`

	// Now is the instant the snapshot was taken, from the service's clock.
	// Clients use it to render delay countdowns without trusting their own
	// wall clock.
	Now time.Time `json:"now"`

	// Stats are the occupancy counts at Now.
	Stats QueueStats `json:"stats"`

	// Ready lists visible messages in the exact order Dequeue would return
	// them: the first element is the next message out.
	Ready []Message `json:"ready"`

	// Delayed lists not-yet-visible messages ordered by AvailableAt, so the
	// first element is the next one to mature.
	Delayed []Message `json:"delayed"`

	// ReadyTruncated reports that Ready was cut short by the limit.
	ReadyTruncated bool `json:"ready_truncated"`

	// DelayedTruncated reports that Delayed was cut short by the limit.
	DelayedTruncated bool `json:"delayed_truncated"`
}

// WalTail is the read-only view of the newest write-ahead-log records,
// returned by GET /wal and QueueService.WalTail. It exists so the web
// console can show durability happening: every accepted enqueue and dequeue
// appears here as the exact record that was fsynced to disk before the
// server answered.
//
// Records holds the raw on-disk JSON objects (the log is JSONL, so each
// line is already JSON) in log order — Records[0] is the oldest record
// shown, the last element is the newest. Reading the tail never mutates the
// log; it is the inspection counterpart to the WAL exactly as GET /messages
// is to the queue.
type WalTail struct {
	// Path is the log file location on the server's disk.
	Path string `json:"path"`

	// SizeBytes is the current size of the log file.
	SizeBytes int64 `json:"size_bytes"`

	// Truncated reports that older records exist beyond what Records shows,
	// either because the requested limit cut them or because the read
	// window did not reach the start of the file.
	Truncated bool `json:"truncated"`

	// Records are the newest log records in log order, verbatim from disk.
	Records []json.RawMessage `json:"records"`
}
