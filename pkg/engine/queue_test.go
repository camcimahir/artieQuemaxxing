// Package engine_test exercises the exported FrankensteinQueue API per
// SKILLS.md: selection-order behavior is table-driven over both queue
// modes, time is injected explicitly (no sleeps), every test builds its own
// queue, and a multi-goroutine hammer keeps the suite meaningful under
// go test -race.
package engine_test

import (
	"fmt"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"queuemaxxing/pkg/engine"
	"queuemaxxing/pkg/model"
)

// base is the fixed test epoch; all times are expressed relative to it.
var base = time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)

var modes = []model.QueueMode{model.ModeFIFO, model.ModeLIFO}

func newQueue(t *testing.T, mode model.QueueMode) *engine.FrankensteinQueue {
	t.Helper()
	q, err := engine.NewFrankensteinQueue(mode)
	if err != nil {
		t.Fatalf("NewFrankensteinQueue(%q): unexpected error: %v", mode, err)
	}
	return q
}

// mkMsg builds a message enqueued at base. Distinct Seq with identical
// EnqueuedAt is deliberate: it is exactly the timestamp-collision case where
// Seq must be the authoritative tie-breaker.
func mkMsg(id string, seq uint64, priority int, availableAt time.Time) model.Message {
	return model.Message{
		ID:          id,
		Seq:         seq,
		Payload:     "payload:" + id,
		Priority:    priority,
		EnqueuedAt:  base,
		AvailableAt: availableAt,
		Status:      model.StatusPending,
	}
}

// popIDs pops with the given sequence of nows and returns the popped IDs in
// order, using "-" for pops that reported no eligible message.
func popIDs(q *engine.FrankensteinQueue, nows ...time.Time) []string {
	ids := make([]string, 0, len(nows))
	for _, now := range nows {
		m, ok := q.Pop(now)
		if !ok {
			ids = append(ids, "-")
			continue
		}
		ids = append(ids, m.ID)
	}
	return ids
}

func TestNewFrankensteinQueue_RejectsInvalidMode(t *testing.T) {
	t.Parallel()
	if _, err := engine.NewFrankensteinQueue(model.QueueMode("BOGUS")); err == nil {
		t.Fatal("expected error for invalid mode, got nil")
	}
	for _, mode := range modes {
		q := newQueue(t, mode)
		if got := q.Mode(); got != mode {
			t.Fatalf("Mode() = %q, want %q", got, mode)
		}
	}
}

// TestPop_PriorityOrdering: higher priority integers pop first regardless of
// mode or push order. All messages are available at exactly now == base,
// which also proves the AvailableAt <= now boundary is inclusive.
func TestPop_PriorityOrdering(t *testing.T) {
	t.Parallel()
	for _, mode := range modes {
		t.Run(string(mode), func(t *testing.T) {
			t.Parallel()
			q := newQueue(t, mode)
			// Pushed in scrambled priority order on purpose.
			q.Push(mkMsg("low", 1, 1, base))
			q.Push(mkMsg("high", 2, 10, base))
			q.Push(mkMsg("mid", 3, 5, base))

			want := []string{"high", "mid", "low", "-"}
			if got := popIDs(q, base, base, base, base); !slices.Equal(got, want) {
				t.Fatalf("pop order = %v, want %v", got, want)
			}
		})
	}
}

// TestPop_DelayedMessageInvisible: a delayed high-priority message must not
// pop before its AvailableAt; a ready lower-priority message pops instead.
// Visibility flips at exactly AvailableAt, not a nanosecond earlier.
func TestPop_DelayedMessageInvisible(t *testing.T) {
	t.Parallel()
	for _, mode := range modes {
		t.Run(string(mode), func(t *testing.T) {
			t.Parallel()
			q := newQueue(t, mode)
			matureAt := base.Add(10 * time.Second)
			q.Push(mkMsg("vip-delayed", 1, 10, matureAt))
			q.Push(mkMsg("pleb-ready", 2, 1, base))

			if got, ok := q.Pop(base); !ok || got.ID != "pleb-ready" {
				t.Fatalf("Pop(base) = %v, %v; want pleb-ready (delayed vip must be invisible)", got, ok)
			}
			if got, ok := q.Pop(matureAt.Add(-time.Nanosecond)); ok {
				t.Fatalf("Pop(1ns before AvailableAt) returned %q; want no message", got.ID)
			}
			if got, ok := q.Pop(matureAt); !ok || got.ID != "vip-delayed" {
				t.Fatalf("Pop(exactly AvailableAt) = %v, %v; want vip-delayed", got, ok)
			}
		})
	}
}

// TestPop_FIFOTieBreaking: equal priority, FIFO pops smallest Seq (oldest)
// first. Push order is scrambled to prove selection follows Seq, not
// insertion order.
func TestPop_FIFOTieBreaking(t *testing.T) {
	t.Parallel()
	q := newQueue(t, model.ModeFIFO)
	q.Push(mkMsg("third", 3, 5, base))
	q.Push(mkMsg("first", 1, 5, base))
	q.Push(mkMsg("second", 2, 5, base))

	want := []string{"first", "second", "third"}
	if got := popIDs(q, base, base, base); !slices.Equal(got, want) {
		t.Fatalf("FIFO pop order = %v, want %v", got, want)
	}
}

// TestPop_LIFOTieBreaking: equal priority, LIFO pops largest Seq (newest)
// first, again independent of push order.
func TestPop_LIFOTieBreaking(t *testing.T) {
	t.Parallel()
	q := newQueue(t, model.ModeLIFO)
	q.Push(mkMsg("third", 3, 5, base))
	q.Push(mkMsg("first", 1, 5, base))
	q.Push(mkMsg("second", 2, 5, base))

	want := []string{"third", "second", "first"}
	if got := popIDs(q, base, base, base); !slices.Equal(got, want) {
		t.Fatalf("LIFO pop order = %v, want %v", got, want)
	}
}

// TestPop_FrankensteinScenario: delay, priority, and tie-breaking interact.
// Three messages:
//   - slowpoke:  priority 9, invisible until base+30s
//   - early:     priority 3, visible immediately, Seq 2
//   - latecomer: priority 3, visible immediately, Seq 3
//
// At base only the two priority-3 messages are visible, so the mode's
// tie-breaker decides. At base+30s slowpoke is visible and its higher
// priority beats the leftover.
func TestPop_FrankensteinScenario(t *testing.T) {
	t.Parallel()
	cases := []struct {
		mode model.QueueMode
		want []string
	}{
		{model.ModeFIFO, []string{"early", "slowpoke", "latecomer", "-"}},
		{model.ModeLIFO, []string{"latecomer", "slowpoke", "early", "-"}},
	}
	for _, tc := range cases {
		t.Run(string(tc.mode), func(t *testing.T) {
			t.Parallel()
			q := newQueue(t, tc.mode)
			q.Push(mkMsg("slowpoke", 1, 9, base.Add(30*time.Second)))
			q.Push(mkMsg("early", 2, 3, base))
			q.Push(mkMsg("latecomer", 3, 3, base))

			later := base.Add(30 * time.Second)
			if got := popIDs(q, base, later, later, later); !slices.Equal(got, tc.want) {
				t.Fatalf("pop order = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestPeek: Peek returns exactly what Pop would (per mode), without
// removing, and respects delay visibility.
func TestPeek(t *testing.T) {
	t.Parallel()
	cases := []struct {
		mode model.QueueMode
		want string
	}{
		{model.ModeFIFO, "older"},
		{model.ModeLIFO, "newer"},
	}
	for _, tc := range cases {
		t.Run(string(tc.mode), func(t *testing.T) {
			t.Parallel()
			q := newQueue(t, tc.mode)
			matureAt := base.Add(5 * time.Second)
			q.Push(mkMsg("older", 1, 7, matureAt))
			q.Push(mkMsg("newer", 2, 7, matureAt))

			if m, ok := q.Peek(base); ok {
				t.Fatalf("Peek(base) = %q; want none, both messages are delayed", m.ID)
			}
			peeked, ok := q.Peek(matureAt)
			if !ok || peeked.ID != tc.want {
				t.Fatalf("Peek(matureAt) = %v, %v; want %q", peeked, ok, tc.want)
			}
			if got := q.Len(); got != 2 {
				t.Fatalf("Len() after Peek = %d, want 2 (Peek must not remove)", got)
			}

			// Mutating the snapshot must not corrupt internal ordering.
			peeked.Priority = -999
			popped, ok := q.Pop(matureAt)
			if !ok || popped.ID != tc.want || popped.Priority != 7 {
				t.Fatalf("Pop after mutated Peek = %+v, %v; want ID %q with priority 7", popped, ok, tc.want)
			}
		})
	}
}

func TestPop_EmptyQueue(t *testing.T) {
	t.Parallel()
	q := newQueue(t, model.ModeFIFO)
	if m, ok := q.Pop(base); ok || m != nil {
		t.Fatalf("Pop on empty queue = %v, %v; want nil, false", m, ok)
	}
	if m, ok := q.Peek(base); ok || m != nil {
		t.Fatalf("Peek on empty queue = %v, %v; want nil, false", m, ok)
	}
}

// TestLenAndAvailableLen: Len counts everything; AvailableLen counts only
// messages whose AvailableAt has passed at the given now.
func TestLenAndAvailableLen(t *testing.T) {
	t.Parallel()
	q := newQueue(t, model.ModeFIFO)
	matureAt := base.Add(time.Minute)
	q.Push(mkMsg("ready-1", 1, 0, base))
	q.Push(mkMsg("ready-2", 2, 0, base))
	q.Push(mkMsg("sleepy", 3, 0, matureAt))

	if got := q.Len(); got != 3 {
		t.Fatalf("Len() = %d, want 3", got)
	}
	if got := q.AvailableLen(base); got != 2 {
		t.Fatalf("AvailableLen(base) = %d, want 2", got)
	}
	if got := q.AvailableLen(matureAt); got != 3 {
		t.Fatalf("AvailableLen(matureAt) = %d, want 3", got)
	}
	if _, ok := q.Pop(matureAt); !ok {
		t.Fatal("Pop(matureAt): expected a message")
	}
	if got, want := q.Len(), 2; got != want {
		t.Fatalf("Len() after one Pop = %d, want %d", got, want)
	}
	if got, want := q.AvailableLen(matureAt), 2; got != want {
		t.Fatalf("AvailableLen after one Pop = %d, want %d", got, want)
	}
}

// TestRemove: removal works for both visible and still-delayed messages,
// reports presence accurately, and removed messages never pop.
// TestRemove excises messages from either heap. It asserts a pop order, so it
// runs for both modes per SKILLS.md §1.3.1 — removing from the middle of a
// heap must leave the surviving order intact in whichever direction the mode
// breaks ties.
func TestRemove(t *testing.T) {
	t.Parallel()
	for _, mode := range modes {
		mode := mode
		t.Run(string(mode), func(t *testing.T) {
			t.Parallel()
			q := newQueue(t, mode)
			matureAt := base.Add(time.Hour)
			q.Push(mkMsg("keep-a", 1, 5, base))
			q.Push(mkMsg("drop-ready", 2, 5, base))
			q.Push(mkMsg("keep-b", 3, 5, base))
			q.Push(mkMsg("drop-delayed", 4, 99, matureAt))

			if !q.Remove("drop-ready") {
				t.Fatal(`Remove("drop-ready") = false, want true`)
			}
			if !q.Remove("drop-delayed") {
				t.Fatal(`Remove("drop-delayed") = false, want true`)
			}
			if q.Remove("drop-ready") {
				t.Fatal("second Remove of the same ID = true, want false")
			}
			if q.Remove("never-existed") {
				t.Fatal("Remove of unknown ID = true, want false")
			}
			if got := q.Len(); got != 2 {
				t.Fatalf("Len() after removals = %d, want 2", got)
			}

			// Neither removed message may surface, even past the delay horizon.
			want := []string{"keep-a", "keep-b", "-"}
			if mode == model.ModeLIFO {
				want = []string{"keep-b", "keep-a", "-"}
			}
			if got := popIDs(q, matureAt, matureAt, matureAt); !slices.Equal(got, want) {
				t.Fatalf("pop order after removals = %v, want %v", got, want)
			}
		})
	}
}

// TestPush_DuplicateID: pushing an already-present ID is a no-op (the
// original message wins), mirroring WAL replay idempotency. Once the ID is
// popped or removed it may be reused.
func TestPush_DuplicateID(t *testing.T) {
	t.Parallel()
	q := newQueue(t, model.ModeFIFO)
	q.Push(mkMsg("dup", 1, 1, base))
	q.Push(mkMsg("dup", 2, 100, base)) // must be ignored

	if got := q.Len(); got != 1 {
		t.Fatalf("Len() after duplicate push = %d, want 1", got)
	}
	m, ok := q.Pop(base)
	if !ok || m.Priority != 1 || m.Seq != 1 {
		t.Fatalf("Pop = %+v, %v; want the original message (priority 1, seq 1)", m, ok)
	}

	// ID is free again after the message left the queue.
	q.Push(mkMsg("dup", 3, 42, base))
	if m, ok := q.Pop(base); !ok || m.Seq != 3 {
		t.Fatalf("Pop after re-push = %+v, %v; want seq 3", m, ok)
	}
}

// TestConcurrent_PushPopRemove: 8 producers, 8 consumers, and removers all
// hammer one queue. Invariants: every message is consumed exactly once
// (either popped once or removed once, never both), and the queue drains to
// empty. Run with -race per SKILLS.md §2.1.
func TestConcurrent_PushPopRemove(t *testing.T) {
	t.Parallel()
	for _, mode := range modes {
		t.Run(string(mode), func(t *testing.T) {
			t.Parallel()
			const (
				producers   = 8
				perProducer = 250
				consumers   = 8
				total       = producers * perProducer
			)
			q := newQueue(t, mode)

			var (
				seq      atomic.Uint64
				consumed sync.Map // ID -> "popped" | "removed"
				popped   atomic.Int64
				removed  atomic.Int64
				wg       sync.WaitGroup
				// Bounded escape hatch so an invariant violation fails the
				// test instead of hanging it (sanctioned by SKILLS.md §1.4:
				// this bounds a check, it does not synchronize anything).
				deadline = time.Now().Add(30 * time.Second)
			)

			claim := func(id, how string) {
				if prev, dup := consumed.LoadOrStore(id, how); dup {
					t.Errorf("message %s consumed twice: %s then %s", id, prev, how)
				}
			}

			for p := 0; p < producers; p++ {
				wg.Add(1)
				go func(p int) {
					defer wg.Done()
					for i := 0; i < perProducer; i++ {
						s := seq.Add(1)
						id := fmt.Sprintf("msg-%d-%d", p, i)
						q.Push(mkMsg(id, s, int(s%7), base))
						// Removers: each producer tries to steal back every
						// 5th message it pushed; success and failure are
						// both legal, but success must count as consumption.
						if i%5 == 0 && q.Remove(id) {
							claim(id, "removed")
							removed.Add(1)
						}
					}
				}(p)
			}
			for c := 0; c < consumers; c++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for popped.Load()+removed.Load() < total {
						if time.Now().After(deadline) {
							t.Error("deadline exceeded: messages were lost or never became poppable")
							return
						}
						m, ok := q.Pop(base)
						if !ok {
							runtime.Gosched()
							continue
						}
						claim(m.ID, "popped")
						popped.Add(1)
					}
				}()
			}
			wg.Wait()

			if got := popped.Load() + removed.Load(); got != total {
				t.Fatalf("consumed %d messages (%d popped + %d removed), want %d",
					got, popped.Load(), removed.Load(), total)
			}
			if got := q.Len(); got != 0 {
				t.Fatalf("Len() after drain = %d, want 0", got)
			}
			if m, ok := q.Pop(base); ok {
				t.Fatalf("Pop after drain returned %q, want none", m.ID)
			}
		})
	}
}

// msgIDs extracts IDs in slice order for snapshot assertions.
func msgIDs(msgs []model.Message) []string {
	ids := make([]string, len(msgs))
	for i, m := range msgs {
		ids[i] = m.ID
	}
	return ids
}

// TestSnapshot_OrdersReadyByPopRuleAndDelayedByMaturity: the snapshot is the
// console's view of the queue, so its ready list must agree with Pop exactly
// — same visibility filter, same order — and its delayed list must be sorted
// by AvailableAt so a UI can render "next to mature" first.
func TestSnapshot_OrdersReadyByPopRuleAndDelayedByMaturity(t *testing.T) {
	t.Parallel()
	for _, mode := range modes {
		mode := mode
		t.Run(string(mode), func(t *testing.T) {
			t.Parallel()
			q := newQueue(t, mode)

			// Visible: two tied at priority 5, one at priority 1.
			q.Push(mkMsg("a", 1, 1, base))
			q.Push(mkMsg("b", 2, 5, base))
			q.Push(mkMsg("d", 4, 5, base))
			// Delayed, pushed in the opposite order to their maturity so a
			// naive "insertion order" implementation would fail.
			q.Push(mkMsg("c", 3, 10, base.Add(4*time.Second)))
			q.Push(mkMsg("e", 5, 10, base.Add(2*time.Second)))

			view := q.Snapshot(base, 0)
			if view.ReadyTruncated || view.DelayedTruncated {
				t.Fatalf("limit 0 must not truncate; got readyTrunc=%v delayedTrunc=%v", view.ReadyTruncated, view.DelayedTruncated)
			}
			if view.ReadyLen != 3 || view.DelayedLen != 2 {
				t.Fatalf("View lens = ready:%d delayed:%d, want 3 and 2", view.ReadyLen, view.DelayedLen)
			}

			if got, want := msgIDs(view.Delayed), []string{"e", "c"}; !slices.Equal(got, want) {
				t.Fatalf("delayed order = %v, want %v (soonest AvailableAt first)", got, want)
			}

			wantReady := []string{"b", "d", "a"}
			if mode == model.ModeLIFO {
				wantReady = []string{"d", "b", "a"}
			}
			if got := msgIDs(view.Ready); !slices.Equal(got, wantReady) {
				t.Fatalf("ready order = %v, want %v", got, wantReady)
			}

			// Snapshot is read-only: nothing was consumed.
			if got := q.Len(); got != 5 {
				t.Fatalf("Len() after Snapshot = %d, want 5 (Snapshot must not consume)", got)
			}

			// And Pop must now deliver precisely the order the snapshot showed.
			if got := popIDs(q, base, base, base); !slices.Equal(got, wantReady) {
				t.Fatalf("pop order = %v, but Snapshot advertised %v", got, wantReady)
			}
		})
	}
}

// TestSnapshot_LimitTruncatesEachListIndependently: a limit caps each list on
// its own, the flags say which one was cut, and the Len fields keep describing
// the whole queue. The truncated page must be the *head* of the pop order, so
// the assertion is mode-dependent and runs for both (SKILLS.md §1.3.1).
func TestSnapshot_LimitTruncatesEachListIndependently(t *testing.T) {
	t.Parallel()
	for _, mode := range modes {
		mode := mode
		t.Run(string(mode), func(t *testing.T) {
			t.Parallel()
			q := newQueue(t, mode)
			for i := 1; i <= 4; i++ {
				q.Push(mkMsg(fmt.Sprintf("r%d", i), uint64(i), 0, base))
			}
			q.Push(mkMsg("d1", 10, 0, base.Add(time.Minute)))

			view := q.Snapshot(base, 2)
			if len(view.Ready) != 2 || !view.ReadyTruncated {
				t.Fatalf("ready = %v (truncated=%v), want 2 entries and truncated=true", msgIDs(view.Ready), view.ReadyTruncated)
			}
			want := []string{"r1", "r2"}
			if mode == model.ModeLIFO {
				want = []string{"r4", "r3"}
			}
			if got := msgIDs(view.Ready); !slices.Equal(got, want) {
				t.Fatalf("truncated ready = %v, want %v (the head of the %s pop order, not an arbitrary pair)", got, want, mode)
			}
			if len(view.Delayed) != 1 || view.DelayedTruncated {
				t.Fatalf("delayed = %v (truncated=%v), want 1 entry and truncated=false", msgIDs(view.Delayed), view.DelayedTruncated)
			}
			if view.ReadyLen != 4 || view.DelayedLen != 1 {
				t.Fatalf("View lens under a limit = ready:%d delayed:%d, want 4 and 1 (counts describe the queue, not the page)", view.ReadyLen, view.DelayedLen)
			}
		})
	}
}

// TestSnapshot_PromotesMaturedMessagesLikePop: visibility must be judged the
// same way by both calls, otherwise the console would show a message as
// delayed that Pop would happily hand out.
func TestSnapshot_PromotesMaturedMessagesLikePop(t *testing.T) {
	t.Parallel()
	q := newQueue(t, model.ModeFIFO)
	q.Push(mkMsg("late", 1, 0, base.Add(3*time.Second)))

	view := q.Snapshot(base, 0)
	if len(view.Ready) != 0 || len(view.Delayed) != 1 {
		t.Fatalf("before AvailableAt: ready=%v delayed=%v, want ready empty and delayed=[late]", msgIDs(view.Ready), msgIDs(view.Delayed))
	}

	// Exactly at AvailableAt the message must be visible in both views.
	view = q.Snapshot(base.Add(3*time.Second), 0)
	if got, want := msgIDs(view.Ready), []string{"late"}; !slices.Equal(got, want) {
		t.Fatalf("at AvailableAt: ready = %v, want %v", got, want)
	}
	if len(view.Delayed) != 0 {
		t.Fatalf("at AvailableAt: delayed = %v, want empty", msgIDs(view.Delayed))
	}
}

// TestSnapshot_EmptyQueue returns empty slices, not nils, so the JSON
// encoder emits [] rather than null and GET /messages matches its documented
// array shape even when the queue is empty.
func TestSnapshot_EmptyQueue(t *testing.T) {
	t.Parallel()
	q := newQueue(t, model.ModeFIFO)
	view := q.Snapshot(base, 10)
	if view.Ready == nil || view.Delayed == nil {
		t.Fatal("empty queue Snapshot returned nil slices; JSON would encode them as null, not []")
	}
	if len(view.Ready) != 0 || len(view.Delayed) != 0 || view.ReadyTruncated || view.DelayedTruncated {
		t.Fatalf("empty queue Snapshot = ready:%v/%v delayed:%v/%v, want all empty and untruncated",
			msgIDs(view.Ready), view.ReadyTruncated, msgIDs(view.Delayed), view.DelayedTruncated)
	}
	if view.ReadyLen != 0 || view.DelayedLen != 0 {
		t.Fatalf("empty queue lens = ready:%d delayed:%d, want zeros", view.ReadyLen, view.DelayedLen)
	}
}
