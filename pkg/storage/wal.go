// Package storage implements the durable Write-Ahead Log (WAL) for
// Queuemaxxing: an append-only JSONL file with fsync after every record
// and crash-safe replay of unconsumed messages.
//
// The WAL is a leaf lock (see SKILLS.md §2.2): code holding WAL.mu never
// calls into other locked components.
package storage

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"sync"
	"time"

	"queuemaxxing/pkg/model"
)

const (
	// recordVersion is the current JSONL schema version.
	recordVersion = 1

	// maxRecordBytes is the largest single log line Recover will accept.
	maxRecordBytes = 16 << 20

	// tailWindowBytes bounds how much of the file Tail reads. Reading a
	// fixed window from the end keeps Tail O(window) rather than O(log
	// size), which matters because the log grows forever (no compaction)
	// and the console polls this path. 4 MiB comfortably covers several
	// records even at the 1 MiB request-body cap.
	tailWindowBytes = 4 << 20
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
// DEQUEUE entries carry only the tombstoned ID plus the shared envelope.
//
// Seq and MsgSeq are deliberately separate. Seq counts records in this file
// and is what makes the log itself totally ordered; MsgSeq is the queue's
// message ordering key, which the service assigns and the scheduler breaks
// priority ties with. They coincide in a log of nothing but enqueues and
// diverge the moment a DEQUEUE is interleaved, so collapsing them into one
// field would make the log's own `seq` mean two different things depending
// on `op`.
//
// The message-scoped fields use omitempty so a DEQUEUE tombstone is a
// genuinely small record rather than an ENQUEUE shape padded with zero
// values. Priority and the timestamps are pointers because their zero values
// are meaningful: priority 0 is the documented default, so a plain
// `int` with omitempty would drop the field from the majority of ENQUEUE
// records and make the log unreadable exactly where it is read most (a jq
// filter over `.priority` would render null). A pointer distinguishes "this
// record has no priority" from "this record has priority 0". The same
// applies to time.Time, where omitempty has no effect on a struct at all.
// MsgSeq is always >= 1 and Payload is validated non-empty, so omitempty can
// never fire for them on an ENQUEUE record.
type logEntry struct {
	V   int       `json:"v"`
	Op  string    `json:"op"`
	Seq uint64    `json:"seq"`
	TS  time.Time `json:"ts"`
	ID  string    `json:"id,omitempty"`

	// Message fields, written on ENQUEUE records only.
	MsgSeq      uint64       `json:"msg_seq,omitempty"`
	Payload     string       `json:"payload,omitempty"`
	Priority    *int         `json:"priority,omitempty"`
	EnqueuedAt  *time.Time   `json:"enqueued_at,omitempty"`
	AvailableAt *time.Time   `json:"available_at,omitempty"`
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
	priority := msg.Priority
	enqueuedAt, availableAt := msg.EnqueuedAt, msg.AvailableAt
	rec := logEntry{
		Op:          OP_ENQUEUE,
		ID:          msg.ID,
		MsgSeq:      msg.Seq,
		Payload:     msg.Payload,
		Priority:    &priority,
		EnqueuedAt:  &enqueuedAt,
		AvailableAt: &availableAt,
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
// exists) and DEQUEUE removes it (no-op if the ID is already gone). The
// returned slice is the active, unconsumed messages in original enqueue
// order.
//
// A torn final line is discarded; a corrupt line anywhere before that is a
// hard error, because bytes lost mid-file mean silent data loss rather than
// an interrupted append. Distinguishing the two needs only one line of
// lookahead, so recovery holds a single pending record rather than the whole
// file. Fold state is the live set; consumed IDs are forgotten as their
// tombstones apply. Peak memory is therefore O(live set + longest record),
// not O(log size). That matters: the log retains every record ever written
// until compaction lands, so buffering it — or retaining every historical
// ID — would make startup cost grow with history instead of with the live
// set.
func (w *WAL) Recover() ([]model.Message, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil, ErrClosed
	}

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seeking WAL for recovery: %w", err)
	}

	live := make(map[string]model.Message)
	var maxSeq uint64

	// applyLine folds one record into the accumulating state. It returns the
	// parse error unhandled so the caller can decide whether this particular
	// line is allowed to be broken.
	applyLine := func(line string) error {
		if line == "" {
			return nil
		}
		var rec logEntry
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			return err
		}
		if rec.Seq > maxSeq {
			maxSeq = rec.Seq
		}
		switch rec.Op {
		case OP_ENQUEUE:
			if rec.ID == "" {
				return nil
			}
			if _, exists := live[rec.ID]; exists {
				return nil
			}
			status := rec.Status
			if status == "" {
				status = model.StatusPending
			}
			// A record with no msg_seq predates the split between record
			// order and message order, when the envelope's seq carried both.
			// That value is monotonic in enqueue order too, so falling back
			// to it preserves the relative ordering of recovered messages.
			msgSeq := rec.MsgSeq
			if msgSeq == 0 {
				msgSeq = rec.Seq
			}
			live[rec.ID] = model.Message{
				ID:          rec.ID,
				Seq:         msgSeq,
				Payload:     rec.Payload,
				Priority:    derefInt(rec.Priority),
				EnqueuedAt:  derefTime(rec.EnqueuedAt),
				AvailableAt: derefTime(rec.AvailableAt),
				Status:      status,
			}
		case OP_DEQUEUE:
			if rec.ID == "" {
				return nil
			}
			delete(live, rec.ID)
		default:
			// Unknown ops are ignored for forward-compatibility.
		}
		return nil
	}

	scanner := bufio.NewScanner(w.file)
	scanner.Buffer(make([]byte, 64*1024), maxRecordBytes)

	var (
		pending     string
		pendingLine int
		havePending bool
		lineNo      int
	)
	for scanner.Scan() {
		lineNo++
		if havePending {
			if err := applyLine(pending); err != nil {
				return nil, fmt.Errorf("corrupt WAL record at line %d: %w", pendingLine, err)
			}
		}
		pending, pendingLine, havePending = scanner.Text(), lineNo, true
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading WAL: %w", err)
	}
	if havePending {
		// The final line is the only one allowed to be broken: that is the
		// signature of a process killed between write and the next append.
		// json.Unmarshal fails before mutating anything, so a torn tail
		// leaves the folded state untouched.
		_ = applyLine(pending)
	}

	w.seq = maxSeq

	out := make([]model.Message, 0, len(live))
	for _, msg := range live {
		out = append(out, msg)
	}
	// Seq is assigned at enqueue under the service lock, so sorting by it
	// restores original enqueue order without retaining every ID the log
	// has ever seen. Peak memory is therefore the live set plus the current
	// line, not the full history.
	slices.SortFunc(out, func(a, b model.Message) int {
		if a.Seq != b.Seq {
			if a.Seq < b.Seq {
				return -1
			}
			return 1
		}
		switch {
		case a.ID < b.ID:
			return -1
		case a.ID > b.ID:
			return 1
		default:
			return 0
		}
	})
	return out, nil
}

// Tail returns the newest log records, verbatim from disk, in log order.
// A maxRecords > 0 caps the result to that many of the newest records;
// maxRecords <= 0 returns every record inside the read window.
//
// Tail reads at most tailWindowBytes from the end of the file, so its cost
// is bounded regardless of how large the log has grown. A line that does
// not parse is skipped rather than reported: corruption policy belongs to
// Recover, and the one legitimately unparseable line — a torn tail left by
// a crash mid-append — is exactly what a reader of the live log should
// ignore. Tail never mutates the log; the write path is O_APPEND, so the
// seek below cannot affect where the next record lands.
func (w *WAL) Tail(maxRecords int) (model.WalTail, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return model.WalTail{}, ErrClosed
	}

	info, err := w.file.Stat()
	if err != nil {
		return model.WalTail{}, fmt.Errorf("stat WAL: %w", err)
	}
	size := info.Size()

	var start int64
	if size > tailWindowBytes {
		start = size - tailWindowBytes
	}
	if _, err := w.file.Seek(start, io.SeekStart); err != nil {
		return model.WalTail{}, fmt.Errorf("seeking WAL for tail: %w", err)
	}
	window := make([]byte, size-start)
	if _, err := io.ReadFull(w.file, window); err != nil {
		return model.WalTail{}, fmt.Errorf("reading WAL tail: %w", err)
	}

	lines := bytes.Split(window, []byte{'\n'})
	if start > 0 && len(lines) > 0 {
		// The window almost certainly opens mid-record; drop the fragment.
		lines = lines[1:]
	}

	// Empty non-nil so JSON encodes as [] rather than null, matching the
	// documented array shape of GET /wal on a fresh log.
	records := make([]json.RawMessage, 0, len(lines))
	for _, line := range lines {
		line = bytes.TrimSpace(line)
		if len(line) == 0 || !json.Valid(line) {
			continue
		}
		records = append(records, json.RawMessage(bytes.Clone(line)))
	}
	capped := false
	if maxRecords > 0 && len(records) > maxRecords {
		records = records[len(records)-maxRecords:]
		capped = true
	}
	return model.WalTail{
		Path:      w.path,
		SizeBytes: size,
		Truncated: start > 0 || capped,
		Records:   records,
	}, nil
}

// derefInt unwraps an optional record integer, yielding zero when the record
// omitted it.
func derefInt(v *int) int {
	if v == nil {
		return 0
	}
	return *v
}

// derefTime unwraps an optional record timestamp, yielding the zero time
// when the record omitted it.
func derefTime(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
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

// appendLocked stamps the envelope, writes rec as a single JSONL line, and
// Syncs. Caller holds mu.
func (w *WAL) appendLocked(rec logEntry) error {
	if w.file == nil {
		return ErrClosed
	}
	w.seq++
	rec.Seq = w.seq
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
