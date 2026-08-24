// Package engine implements the core in-memory queue data structure for
// Queuemaxxing: FrankensteinQueue, a priority- and delay-aware container
// with FIFO or LIFO tie-breaking as specified in ARCHITECTURE.md §4.
//
// The engine is purely in-memory. It knows nothing about HTTP, the WAL, or
// message lifecycle status; durability and orchestration belong to higher
// layers. Time is never read internally — every time-dependent method takes
// now explicitly, which keeps delay semantics deterministic in tests.
package engine

import (
	"container/heap"
	"fmt"
	"slices"
	"sync"
	"time"

	"queuemaxxing/pkg/model"
)

// location identifies which internal heap currently holds an item, so
// Remove knows where to excise it from.
type location uint8

const (
	inDelayed location = iota
	inReady
)

// item wraps a message with heap bookkeeping: a stable arrival counter for
// deterministic ordering and an index maintained by Swap so removal from
// the middle of a heap is O(log n).
type item struct {
	msg   model.Message
	order uint64 // monotonic arrival counter assigned at Push
	loc   location
	index int // position inside the heap identified by loc
}

// readyHeap orders visible messages for selection: highest Priority first,
// ties broken by Seq according to the queue mode (smallest Seq for FIFO,
// largest for LIFO). Seq is assigned by the caller and expected to be
// unique; if two messages ever share one, the arrival counter breaks the
// tie in the same direction, keeping the order total and deterministic.
type readyHeap struct {
	mode  model.QueueMode
	items []*item
}

func (h *readyHeap) Len() int { return len(h.items) }

func (h *readyHeap) Less(i, j int) bool { return h.less(h.items[i], h.items[j]) }

// less is the selection rule itself, expressed over items rather than heap
// indices so Snapshot can reuse it to sort a copy into delivery order.
func (h *readyHeap) less(a, b *item) bool {
	if a.msg.Priority != b.msg.Priority {
		return a.msg.Priority > b.msg.Priority
	}
	if h.mode == model.ModeLIFO {
		if a.msg.Seq != b.msg.Seq {
			return a.msg.Seq > b.msg.Seq
		}
		return a.order > b.order
	}
	if a.msg.Seq != b.msg.Seq {
		return a.msg.Seq < b.msg.Seq
	}
	return a.order < b.order
}

func (h *readyHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].index = i
	h.items[j].index = j
}

func (h *readyHeap) Push(x any) {
	it := x.(*item)
	it.loc = inReady
	it.index = len(h.items)
	h.items = append(h.items, it)
}

func (h *readyHeap) Pop() any {
	old := h.items
	n := len(old)
	it := old[n-1]
	old[n-1] = nil // allow GC of the slot
	h.items = old[:n-1]
	it.index = -1
	return it
}

// delayedHeap is a min-heap on AvailableAt holding messages that are not
// yet visible. Ties fall back to arrival order purely for determinism; the
// readyHeap comparator decides final pop order after promotion.
type delayedHeap struct {
	items []*item
}

func (h *delayedHeap) Len() int { return len(h.items) }

func (h *delayedHeap) Less(i, j int) bool { return h.less(h.items[i], h.items[j]) }

// less orders by maturity time, falling back to arrival order so the
// comparison is total and Snapshot's output is deterministic.
func (h *delayedHeap) less(a, b *item) bool {
	if !a.msg.AvailableAt.Equal(b.msg.AvailableAt) {
		return a.msg.AvailableAt.Before(b.msg.AvailableAt)
	}
	return a.order < b.order
}

func (h *delayedHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].index = i
	h.items[j].index = j
}

func (h *delayedHeap) Push(x any) {
	it := x.(*item)
	it.loc = inDelayed
	it.index = len(h.items)
	h.items = append(h.items, it)
}

func (h *delayedHeap) Pop() any {
	old := h.items
	n := len(old)
	it := old[n-1]
	old[n-1] = nil
	h.items = old[:n-1]
	it.index = -1
	return it
}

// FrankensteinQueue is the thread-safe in-memory scheduling structure at
// the heart of Queuemaxxing. It stores pending messages and selects the
// next one to deliver by: (1) filtering out messages whose AvailableAt is
// after now, (2) taking the highest Priority among the visible ones, and
// (3) breaking priority ties by Seq — smallest first in FIFO mode, largest
// first in LIFO mode.
//
// Internally it keeps two container/heap structures — a ready heap ordered
// by the selection rule and a delayed min-heap ordered by AvailableAt —
// plus an ID index for O(1) lookup. Delayed messages are promoted lazily:
// every call that takes now first moves matured messages into the ready
// heap. Push, Pop, and Remove are O(log n) after any promotion; a call that
// takes now first promotes matured messages at O(k log n). Snapshot is
// O(n log n) because it sorts the heap arrays into presentation order.
//
// The mutex is internal and the structure is a strict leaf lock (methods
// never call out into other components), per SKILLS.md §2.2.
//
// Time contract: callers must pass non-decreasing now values across the
// lifetime of the queue (a real clock does this naturally). Promotion is
// permanent, so a call with a time in the future followed by one in the
// past would let the earlier call see messages that are not yet visible.
type FrankensteinQueue struct {
	mu        sync.Mutex
	ready     readyHeap
	delayed   delayedHeap
	byID      map[string]*item
	nextOrder uint64
}

// NewFrankensteinQueue constructs an empty queue with the given ordering
// mode. It returns an error for unrecognized modes.
func NewFrankensteinQueue(mode model.QueueMode) (*FrankensteinQueue, error) {
	if !mode.Valid() {
		return nil, fmt.Errorf("invalid queue mode %q: must be %q or %q", mode, model.ModeFIFO, model.ModeLIFO)
	}
	return &FrankensteinQueue{
		ready: readyHeap{mode: mode},
		byID:  make(map[string]*item),
	}, nil
}

// Mode returns the queue's ordering mode (immutable after construction).
func (q *FrankensteinQueue) Mode() model.QueueMode {
	return q.ready.mode
}

// Push stores a message. The message is invisible to Pop and Peek until
// now >= msg.AvailableAt. Pushing an ID that is already present is a no-op
// (the existing message wins), mirroring the WAL replay idempotency rule in
// SKILLS.md §3.2.
func (q *FrankensteinQueue) Push(msg model.Message) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, exists := q.byID[msg.ID]; exists {
		return
	}
	it := &item{msg: msg, order: q.nextOrder}
	q.nextOrder++
	// Everything enters through the delayed heap; the first call carrying a
	// now >= AvailableAt promotes it into the ready heap. This keeps Push
	// free of any clock dependency.
	heap.Push(&q.delayed, it)
	q.byID[msg.ID] = it
}

// Pop removes and returns the next message according to the selection rule,
// considering only messages with AvailableAt <= now. It returns (nil, false)
// when no eligible message exists. The returned message is a copy owned by
// the caller.
func (q *FrankensteinQueue) Pop(now time.Time) (*model.Message, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.promoteLocked(now)
	if q.ready.Len() == 0 {
		return nil, false
	}
	it := heap.Pop(&q.ready).(*item)
	delete(q.byID, it.msg.ID)
	return &it.msg, true
}

// Peek returns the message Pop would return for the same now, without
// removing it. The returned message is a snapshot copy; mutating it does
// not affect the queue.
func (q *FrankensteinQueue) Peek(now time.Time) (*model.Message, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.promoteLocked(now)
	if q.ready.Len() == 0 {
		return nil, false
	}
	m := q.ready.items[0].msg
	return &m, true
}

// Len returns the total number of messages held, visible or not.
func (q *FrankensteinQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.byID)
}

// AvailableLen returns the number of messages eligible for Pop at now,
// i.e. those with AvailableAt <= now.
func (q *FrankensteinQueue) AvailableLen(now time.Time) int {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.promoteLocked(now)
	return q.ready.Len()
}

// Remove deletes the message with the given ID regardless of visibility,
// reporting whether it was present. Removal from the middle of either heap
// is O(log n) via the tracked index.
func (q *FrankensteinQueue) Remove(id string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	it, ok := q.byID[id]
	if !ok {
		return false
	}
	delete(q.byID, id)
	switch it.loc {
	case inReady:
		heap.Remove(&q.ready, it.index)
	case inDelayed:
		heap.Remove(&q.delayed, it.index)
	}
	return true
}

// View is a read-only picture of both heaps taken in one lock acquisition.
// The message slices may be truncated by a limit; the Len fields always
// describe the whole queue, so a caller can report honest totals next to a
// partial page.
type View struct {
	// Ready holds visible messages in the exact order Pop would return
	// them: Ready[0] is the next message out.
	Ready []model.Message

	// Delayed holds not-yet-visible messages ordered by AvailableAt, so
	// Delayed[0] is the next one to mature.
	Delayed []model.Message

	// ReadyTruncated reports that a limit cut Ready short.
	ReadyTruncated bool

	// DelayedTruncated reports that a limit cut Delayed short.
	DelayedTruncated bool

	// ReadyLen is the total number of visible messages, ignoring any limit.
	ReadyLen int

	// DelayedLen is the total number of not-yet-visible messages, ignoring
	// any limit.
	DelayedLen int
}

// Snapshot returns a read-only view of the queue at now. Both message slices
// are copies — mutating them does not affect the queue. A limit > 0 truncates
// each slice independently to that many entries.
//
// Snapshot promotes matured messages exactly like Pop does, so the two never
// disagree about visibility, and it takes the queue lock exactly once: the
// lists and the counts in the returned View are always from the same instant,
// which is what lets a caller present them together without qualification.
//
// Cost is O(n log n) because the heap arrays are only partially ordered and
// must be sorted into presentation order. This is an inspection path (the web
// console and GET /messages), not the hot path; Pop remains O(log n).
func (q *FrankensteinQueue) Snapshot(now time.Time, limit int) View {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.promoteLocked(now)
	ready, readyTruncated := orderedMessages(q.ready.items, q.ready.less, limit)
	delayed, delayedTruncated := orderedMessages(q.delayed.items, q.delayed.less, limit)
	return View{
		Ready:            ready,
		Delayed:          delayed,
		ReadyTruncated:   readyTruncated,
		DelayedTruncated: delayedTruncated,
		ReadyLen:         q.ready.Len(),
		DelayedLen:       q.delayed.Len(),
	}
}

// orderedMessages copies items, sorts the copy with less, applies limit,
// and returns the messages plus whether anything was cut. Both comparators
// are total (they fall back to the arrival counter), so the sort is stable
// in effect and the output is deterministic.
func orderedMessages(items []*item, less func(a, b *item) bool, limit int) ([]model.Message, bool) {
	if len(items) == 0 {
		// Empty non-nil so JSON encodes as [] rather than null. GET /messages
		// documents these fields as arrays; a reviewer curling an empty
		// queue should see that shape, not a surprise null.
		return []model.Message{}, false
	}
	sorted := make([]*item, len(items))
	copy(sorted, items)
	slices.SortFunc(sorted, func(a, b *item) int {
		switch {
		case less(a, b):
			return -1
		case less(b, a):
			return 1
		default:
			return 0
		}
	})
	truncated := false
	if limit > 0 && len(sorted) > limit {
		sorted = sorted[:limit]
		truncated = true
	}
	out := make([]model.Message, len(sorted))
	for i, it := range sorted {
		out[i] = it.msg
	}
	return out, truncated
}

// promoteLocked moves every delayed message whose AvailableAt has passed
// into the ready heap. Callers must hold q.mu.
func (q *FrankensteinQueue) promoteLocked(now time.Time) {
	for q.delayed.Len() > 0 {
		top := q.delayed.items[0]
		if !top.msg.Available(now) {
			return
		}
		heap.Pop(&q.delayed)
		heap.Push(&q.ready, top)
	}
}
