// Package storage implements the durable Write-Ahead Log (WAL) for
// Queuemaxxing: an append-only JSONL file with fsync after every record
// and crash-safe replay of unconsumed messages.
//
// The WAL is a leaf lock (see SKILLS.md §2.2): code holding WAL.mu never
// calls into other locked components.
package storage

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"queuemaxxing/pkg/model"
)

const (
	// recordVersion is the current JSONL schema version.
	recordVersion = 1

	// maxRecordBytes is the largest single log line Recover will accept.
	maxRecordBytes = 16 << 20
)

// Log operation types persisted in the WAL.
const (
	// OP_ENQUEUE stores a message payload, metadata, priority, delay, and
	// timestamps as a durable insert.
	OP_ENQUEUE = "ENQUEUE"

	// OP_DEQUEUE stores a message ID tombstone marking the message consumed.
	OP_DEQUEUE = "DEQUEUE"
)

// Sentinel errors callers may match with errors.Is.
var (
	// ErrClosed is returned when a method is called on a closed WAL.
	ErrClosed = errors.New("wal is closed")

	// ErrEmptyID is returned when an enqueue or dequeue is missing an ID.
	ErrEmptyID = errors.New("message id must not be empty")
)

// logEntry is one JSONL record. ENQUEUE entries carry the full message;
// DEQUEUE entries carry only the tombstoned ID (plus the shared envelope).
type logEntry struct {
	V           int          `json:"v"`
	Op          string       `json:"op"`
	Seq         uint64       `json:"seq"`
	TS          time.Time    `json:"ts"`
	ID          string       `json:"id,omitempty"`
	Payload     string       `json:"payload,omitempty"`
	Priority    int          `json:"priority,omitempty"`
	EnqueuedAt  time.Time    `json:"enqueued_at,omitempty"`
	AvailableAt time.Time    `json:"available_at,omitempty"`
	Status      model.Status `json:"status,omitempty"`
}

// WAL is an append-only, fsync-on-write log. All methods are safe for
// concurrent use.
type WAL struct {
	mu   sync.Mutex
	file *os.File
	path string
	seq  uint64
	now  func() time.Time
}

// NewWAL opens (or creates) the append-only log at path using
// O_CREATE|O_APPEND|O_RDWR. The caller must invoke Recover to rebuild
// in-memory state from any existing records.
func NewWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("opening WAL %q: %w", path, err)
	}
	return &WAL{
		file: f,
		path: path,
		now:  time.Now,
	}, nil
}

// WriteEnqueue appends an OP_ENQUEUE record for msg and calls file.Sync
// so the record is durable before the caller mutates in-memory state.
func (w *WAL) WriteEnqueue(msg model.Message) error {
	if msg.ID == "" {
		return ErrEmptyID
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	status := msg.Status
	if status == "" {
		status = model.StatusPending
	}
	rec := logEntry{
		Op:          OP_ENQUEUE,
		Seq:         msg.Seq,
		ID:          msg.ID,
		Payload:     msg.Payload,
		Priority:    msg.Priority,
		EnqueuedAt:  msg.EnqueuedAt,
		AvailableAt: msg.AvailableAt,
		Status:      status,
	}
	if err := w.appendLocked(rec); err != nil {
		return fmt.Errorf("appending enqueue record: %w", err)
	}
	return nil
}

// WriteDequeue appends an OP_DEQUEUE tombstone for msgID and calls
// file.Sync so the consumption is durable.
func (w *WAL) WriteDequeue(msgID string) error {
	if msgID == "" {
		return ErrEmptyID
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	rec := logEntry{
		Op: OP_DEQUEUE,
		ID: msgID,
	}
	if err := w.appendLocked(rec); err != nil {
		return fmt.Errorf("appending dequeue record: %w", err)
	}
	return nil
}

// Recover reads the log from start to finish, applying each record as a
// pure fold: ENQUEUE inserts a live message (no-op if the ID already
// exists) and DEQUEUE removes it (no-op if the ID is already gone). A
// torn final line is discarded; a corrupt line anywhere before that is a
// hard error. The returned slice is the active, unconsumed messages in
// original enqueue order.
func (w *WAL) Recover() ([]model.Message, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil, ErrClosed
	}

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seeking WAL for recovery: %w", err)
	}

	scanner := bufio.NewScanner(w.file)
	scanner.Buffer(make([]byte, 64*1024), maxRecordBytes)
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading WAL: %w", err)
	}

	live := make(map[string]model.Message)
	order := make([]string, 0)
	var maxSeq uint64

	for i, line := range lines {
		if line == "" {
			continue
		}
		var rec logEntry
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			if i == len(lines)-1 {
				// Torn write at EOF: prior records are intact.
				break
			}
			return nil, fmt.Errorf("corrupt WAL record at line %d: %w", i+1, err)
		}
		if rec.Seq > maxSeq {
			maxSeq = rec.Seq
		}
		switch rec.Op {
		case OP_ENQUEUE:
			if rec.ID == "" {
				continue
			}
			if _, exists := live[rec.ID]; exists {
				continue
			}
			status := rec.Status
			if status == "" {
				status = model.StatusPending
			}
			live[rec.ID] = model.Message{
				ID:          rec.ID,
				Seq:         rec.Seq,
				Payload:     rec.Payload,
				Priority:    rec.Priority,
				EnqueuedAt:  rec.EnqueuedAt,
				AvailableAt: rec.AvailableAt,
				Status:      status,
			}
			order = append(order, rec.ID)
		case OP_DEQUEUE:
			if rec.ID == "" {
				continue
			}
			if _, exists := live[rec.ID]; !exists {
				continue
			}
			delete(live, rec.ID)
		default:
			// Unknown ops are ignored for forward-compatibility.
			continue
		}
	}

	w.seq = maxSeq

	out := make([]model.Message, 0, len(live))
	for _, id := range order {
		if msg, ok := live[id]; ok {
			out = append(out, msg)
		}
	}
	return out, nil
}

// Close closes the underlying file descriptor. Subsequent writes and
// Recover calls return ErrClosed. Close is idempotent.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	if err != nil {
		return fmt.Errorf("closing WAL: %w", err)
	}
	return nil
}

// appendLocked writes rec as a single JSONL line and Syncs. Caller holds mu.
func (w *WAL) appendLocked(rec logEntry) error {
	if w.file == nil {
		return ErrClosed
	}
	w.seq++
	if rec.Seq == 0 {
		rec.Seq = w.seq
	} else if rec.Seq > w.seq {
		w.seq = rec.Seq
	}
	rec.V = recordVersion
	rec.TS = w.now().UTC()

	b, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshaling WAL record: %w", err)
	}
	b = append(b, '\n')
	if _, err := w.file.Write(b); err != nil {
		return fmt.Errorf("writing WAL record: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("syncing WAL: %w", err)
	}
	return nil
}
