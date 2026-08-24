// Package service implements QueueService, the single concurrency choke
// point for Queuemaxxing: it assigns IDs and sequence numbers, enforces
// the WAL-first write protocol, and mutates the in-memory FrankensteinQueue.
package service

import (
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"time"

	"queuemaxxing/pkg/engine"
	"queuemaxxing/pkg/model"
	"queuemaxxing/pkg/storage"
)

// ErrClosed is returned when a method is called on a closed QueueService.
var ErrClosed = errors.New("queue service is closed")

// QueueService is the durable, thread-safe queue facade. Internal state is
// guarded by mu; the engine and WAL are leaf locks acquired only while
// holding mu, matching the lock order in SKILLS.md §2.2.
type QueueService struct {
	mu     sync.RWMutex
	queue  *engine.FrankensteinQueue
	wal    *storage.WAL
	now    func() time.Time
	seq    uint64
	closed bool
}

// Option configures a QueueService at construction time.
type Option func(*QueueService)

// WithNow overrides the clock used to stamp EnqueuedAt/AvailableAt and to
// judge delay visibility (default time.Now). Tests inject a fake clock so
// delay semantics can be verified deterministically, per SKILLS.md §1.4.
func WithNow(now func() time.Time) Option {
	return func(s *QueueService) {
		if now != nil {
			s.now = now
		}
	}
}

// NewQueueService opens the WAL at walPath, recovers active messages into
// a new FrankensteinQueue configured for mode, and returns a ready service.
// Optional Options (e.g. WithNow) customize the service before it is used.
func NewQueueService(walPath string, mode model.QueueMode, opts ...Option) (*QueueService, error) {
	if !mode.Valid() {
		return nil, fmt.Errorf("invalid queue mode %q: must be %q or %q", mode, model.ModeFIFO, model.ModeLIFO)
	}

	wal, err := storage.NewWAL(walPath)
	if err != nil {
		return nil, err
	}

	q, err := engine.NewFrankensteinQueue(mode)
	if err != nil {
		_ = wal.Close()
		return nil, err
	}

	recovered, err := wal.Recover()
	if err != nil {
		_ = wal.Close()
		return nil, fmt.Errorf("recovering WAL: %w", err)
	}

	var maxSeq uint64
	for _, msg := range recovered {
		q.Push(msg)
		if msg.Seq > maxSeq {
			maxSeq = msg.Seq
		}
	}

	s := &QueueService{
		queue: q,
		wal:   wal,
		now:   time.Now,
		seq:   maxSeq,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// Enqueue validates req, assigns a UUID and monotonic Seq, computes
// AvailableAt from the current time plus DelaySeconds, appends a durable
// ENQUEUE record, then inserts the message into the in-memory queue.
func (s *QueueService) Enqueue(req model.EnqueueRequest) (model.Message, error) {
	if err := req.Validate(); err != nil {
		return model.Message{}, err
	}
	id, err := newUUID()
	if err != nil {
		return model.Message{}, fmt.Errorf("generating message id: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return model.Message{}, ErrClosed
	}

	now := s.now()
	s.seq++
	msg := model.Message{
		ID:          id,
		Seq:         s.seq,
		Payload:     req.Payload,
		Priority:    req.Priority,
		EnqueuedAt:  now,
		AvailableAt: now.Add(time.Duration(req.DelaySeconds) * time.Second),
		Status:      model.StatusPending,
	}

	if err := s.wal.WriteEnqueue(msg); err != nil {
		s.seq--
		return model.Message{}, err
	}
	s.queue.Push(msg)
	return msg, nil
}

// Dequeue pops the highest-ranking ready message under the service lock.
// If a message is selected, a durable DEQUEUE record is written before the
// call returns. An empty queue returns (nil, nil). If the WAL write fails,
// the message is restored to memory so the in-process state matches the log.
func (s *QueueService) Dequeue() (*model.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}

	msg, ok := s.queue.Pop(s.now())
	if !ok {
		return nil, nil
	}
	if err := s.wal.WriteDequeue(msg.ID); err != nil {
		s.queue.Push(*msg)
		return nil, err
	}
	return msg, nil
}

// Stats returns total, available, and delayed counts at the current time.
func (s *QueueService) Stats() model.QueueStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := s.now()
	total := s.queue.Len()
	available := s.queue.AvailableLen(now)
	return model.QueueStats{
		Total:     total,
		Available: available,
		Delayed:   total - available,
	}
}

// Close closes the underlying WAL. Subsequent Enqueue and Dequeue calls
// return ErrClosed. Close is idempotent.
func (s *QueueService) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.wal.Close()
}

// newUUID returns a RFC 4122 version-4 UUID.
func newUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}
