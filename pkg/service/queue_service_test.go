package service

import (
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"queuemaxxing/pkg/model"
)

var base = time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)

var modes = []model.QueueMode{model.ModeFIFO, model.ModeLIFO}

func openService(t *testing.T, mode model.QueueMode) (*QueueService, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "wal.log")
	svc, err := NewQueueService(path, mode)
	if err != nil {
		t.Fatalf("NewQueueService: unexpected error: %v", err)
	}
	t.Cleanup(func() { _ = svc.Close() })
	return svc, path
}

func reopenService(t *testing.T, path string, mode model.QueueMode) *QueueService {
	t.Helper()
	svc, err := NewQueueService(path, mode)
	if err != nil {
		t.Fatalf("NewQueueService(%q): unexpected error: %v", path, err)
	}
	t.Cleanup(func() { _ = svc.Close() })
	return svc
}

func drain(t *testing.T, svc *QueueService) []model.Message {
	t.Helper()
	var out []model.Message
	for {
		msg, err := svc.Dequeue()
		if err != nil {
			t.Fatalf("Dequeue: %v", err)
		}
		if msg == nil {
			return out
		}
		out = append(out, *msg)
	}
}

func idsOf(msgs []model.Message) []string {
	ids := make([]string, len(msgs))
	for i, m := range msgs {
		ids[i] = m.ID
	}
	return ids
}

func TestNewQueueService_RejectsInvalidMode(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "wal.log")
	if _, err := NewQueueService(path, model.QueueMode("BOGUS")); err == nil {
		t.Fatal("expected error for invalid mode, got nil")
	}
}

func TestEnqueue_RejectsInvalidRequest(t *testing.T) {
	t.Parallel()
	svc, _ := openService(t, model.ModeFIFO)

	if _, err := svc.Enqueue(model.EnqueueRequest{}); err == nil {
		t.Fatal("empty payload: expected error, got nil")
	}
	if _, err := svc.Enqueue(model.EnqueueRequest{Payload: "x", DelaySeconds: -1}); err == nil {
		t.Fatal("negative delay: expected error, got nil")
	}
	if stats := svc.Stats(); stats.Total != 0 {
		t.Fatalf("invalid requests mutated queue: stats=%+v", stats)
	}
}

func TestEnqueue_AssignsUUIDAndAvailableAt(t *testing.T) {
	t.Parallel()
	svc, _ := openService(t, model.ModeFIFO)
	svc.now = func() time.Time { return base }

	msg, err := svc.Enqueue(model.EnqueueRequest{Payload: "hello", Priority: 3, DelaySeconds: 7})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if msg.ID == "" {
		t.Fatal("expected UUID, got empty ID")
	}
	if msg.Seq != 1 {
		t.Fatalf("Seq = %d, want 1", msg.Seq)
	}
	if msg.Payload != "hello" || msg.Priority != 3 {
		t.Fatalf("payload/priority mismatch: %+v", msg)
	}
	if !msg.EnqueuedAt.Equal(base) {
		t.Fatalf("EnqueuedAt = %v, want %v", msg.EnqueuedAt, base)
	}
	wantAvail := base.Add(7 * time.Second)
	if !msg.AvailableAt.Equal(wantAvail) {
		t.Fatalf("AvailableAt = %v, want %v", msg.AvailableAt, wantAvail)
	}
	if msg.Status != model.StatusPending {
		t.Fatalf("Status = %q, want PENDING", msg.Status)
	}
}

func TestDequeue_ReturnsNilWhenEmpty(t *testing.T) {
	t.Parallel()
	svc, _ := openService(t, model.ModeFIFO)
	msg, err := svc.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if msg != nil {
		t.Fatalf("expected nil message from empty queue, got %+v", msg)
	}
}

func TestDequeue_PriorityThenModeTieBreak(t *testing.T) {
	t.Parallel()
	for _, mode := range modes {
		mode := mode
		t.Run(string(mode), func(t *testing.T) {
			t.Parallel()
			svc, _ := openService(t, mode)
			svc.now = func() time.Time { return base }

			// Equal-priority messages exercise FIFO (smallest Seq) vs
			// LIFO (largest Seq). A higher-priority message must win first.
			mustEnqueue(t, svc, model.EnqueueRequest{Payload: "a", Priority: 1})
			mustEnqueue(t, svc, model.EnqueueRequest{Payload: "b", Priority: 1})
			mustEnqueue(t, svc, model.EnqueueRequest{Payload: "hi", Priority: 9})
			mustEnqueue(t, svc, model.EnqueueRequest{Payload: "c", Priority: 1})

			got := payloads(t, drain(t, svc))
			var want []string
			if mode == model.ModeFIFO {
				want = []string{"hi", "a", "b", "c"}
			} else {
				want = []string{"hi", "c", "b", "a"}
			}
			if len(got) != len(want) {
				t.Fatalf("dequeued %v, want %v", got, want)
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("dequeued %v, want %v", got, want)
				}
			}
		})
	}
}

func TestDequeue_DelayedInvisibleUntilAvailableAt(t *testing.T) {
	t.Parallel()
	for _, mode := range modes {
		mode := mode
		t.Run(string(mode), func(t *testing.T) {
			t.Parallel()
			svc, _ := openService(t, mode)
			now := base
			svc.now = func() time.Time { return now }

			mustEnqueue(t, svc, model.EnqueueRequest{Payload: "ready"})
			mustEnqueue(t, svc, model.EnqueueRequest{Payload: "later", DelaySeconds: 5})

			stats := svc.Stats()
			if stats.Total != 2 || stats.Available != 1 || stats.Delayed != 1 {
				t.Fatalf("stats before delay elapsed: %+v, want total=2 available=1 delayed=1", stats)
			}

			msg, err := svc.Dequeue()
			if err != nil {
				t.Fatalf("Dequeue: %v", err)
			}
			if msg == nil || msg.Payload != "ready" {
				t.Fatalf("got %+v, want payload=ready", msg)
			}
			msg, err = svc.Dequeue()
			if err != nil {
				t.Fatalf("Dequeue: %v", err)
			}
			if msg != nil {
				t.Fatalf("delayed message visible too early: %+v", msg)
			}

			now = base.Add(5 * time.Second)
			msg, err = svc.Dequeue()
			if err != nil {
				t.Fatalf("Dequeue at AvailableAt: %v", err)
			}
			if msg == nil || msg.Payload != "later" {
				t.Fatalf("got %+v, want payload=later at exact AvailableAt", msg)
			}
		})
	}
}

func TestStats_CountsAvailableAndDelayed(t *testing.T) {
	t.Parallel()
	svc, _ := openService(t, model.ModeFIFO)
	now := base
	svc.now = func() time.Time { return now }

	mustEnqueue(t, svc, model.EnqueueRequest{Payload: "a"})
	mustEnqueue(t, svc, model.EnqueueRequest{Payload: "b", DelaySeconds: 10})
	mustEnqueue(t, svc, model.EnqueueRequest{Payload: "c", DelaySeconds: 10})

	got := svc.Stats()
	want := model.QueueStats{Total: 3, Available: 1, Delayed: 2}
	if got != want {
		t.Fatalf("Stats() = %+v, want %+v", got, want)
	}

	now = base.Add(10 * time.Second)
	got = svc.Stats()
	want = model.QueueStats{Total: 3, Available: 3, Delayed: 0}
	if got != want {
		t.Fatalf("Stats() after delay = %+v, want %+v", got, want)
	}
}

func TestQueueService_Restart_RecoversUnconsumedState(t *testing.T) {
	t.Parallel()
	for _, mode := range modes {
		mode := mode
		t.Run(string(mode), func(t *testing.T) {
			t.Parallel()
			svc, path := openService(t, mode)
			svc.now = func() time.Time { return base }

			a := mustEnqueue(t, svc, model.EnqueueRequest{Payload: "keep-a", Priority: 1})
			b := mustEnqueue(t, svc, model.EnqueueRequest{Payload: "keep-b", Priority: 1})
			c := mustEnqueue(t, svc, model.EnqueueRequest{Payload: "keep-c", Priority: 1})

			taken, err := svc.Dequeue()
			if err != nil || taken == nil {
				t.Fatalf("Dequeue: msg=%v err=%v", taken, err)
			}
			if err := svc.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			svc2 := reopenService(t, path, mode)
			svc2.now = func() time.Time { return base }
			got := drain(t, svc2)
			gotIDs := idsOf(got)
			var wantIDs []string
			if mode == model.ModeFIFO {
				// FIFO pops smallest Seq first, so taken is a; remaining b, c.
				wantIDs = []string{b.ID, c.ID}
				if taken.ID != a.ID {
					t.Fatalf("first dequeue ID = %s, want %s", taken.ID, a.ID)
				}
			} else {
				// LIFO pops largest Seq first, so taken is c; remaining b, a.
				wantIDs = []string{b.ID, a.ID}
				if taken.ID != c.ID {
					t.Fatalf("first dequeue ID = %s, want %s", taken.ID, c.ID)
				}
			}
			if len(gotIDs) != len(wantIDs) {
				t.Fatalf("recovered IDs = %v, want %v", gotIDs, wantIDs)
			}
			for i := range wantIDs {
				if gotIDs[i] != wantIDs[i] {
					t.Fatalf("recovered IDs = %v, want %v", gotIDs, wantIDs)
				}
			}
			if stats := svc2.Stats(); stats.Total != 0 || stats.Available != 0 || stats.Delayed != 0 {
				t.Fatalf("stats after full drain = %+v", stats)
			}
		})
	}
}

func TestClose_RejectsSubsequentMutations(t *testing.T) {
	t.Parallel()
	svc, _ := openService(t, model.ModeFIFO)
	if err := svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := svc.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := svc.Enqueue(model.EnqueueRequest{Payload: "x"}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Enqueue after close: got %v, want ErrClosed", err)
	}
	if _, err := svc.Dequeue(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Dequeue after close: got %v, want ErrClosed", err)
	}
}

// TestQueueService_ConcurrentProduceConsume_RestartConsistent hammers the
// service with 50 producers and 50 consumers, then reopens the same WAL
// and asserts no duplicates and a lossless restart.
func TestQueueService_ConcurrentProduceConsume_RestartConsistent(t *testing.T) {
	t.Parallel()
	svc, path := openService(t, model.ModeFIFO)

	const (
		producers      = 50
		consumers      = 50
		perProducer    = 8
		maxPerConsumer = 4
	)
	totalProduced := producers * perProducer

	var (
		prodWG, consWG sync.WaitGroup
		producedMu     sync.Mutex
		produced       = make(map[string]string, totalProduced)
		consumedMu     sync.Mutex
		consumed       = make(map[string]string)
		firstErr       atomic.Value
		producersDone  atomic.Bool
	)
	storeErr := func(err error) {
		if err != nil {
			firstErr.CompareAndSwap(nil, err)
		}
	}

	start := make(chan struct{})

	prodWG.Add(producers)
	for p := 0; p < producers; p++ {
		p := p
		go func() {
			defer prodWG.Done()
			<-start
			for i := 0; i < perProducer; i++ {
				payload := fmt.Sprintf("p-%d-%d", p, i)
				msg, err := svc.Enqueue(model.EnqueueRequest{
					Payload:  payload,
					Priority: i % 7,
				})
				if err != nil {
					storeErr(fmt.Errorf("producer %d enqueue %d: %w", p, i, err))
					return
				}
				producedMu.Lock()
				produced[msg.ID] = payload
				producedMu.Unlock()
			}
		}()
	}

	consWG.Add(consumers)
	for c := 0; c < consumers; c++ {
		go func() {
			defer consWG.Done()
			<-start
			for n := 0; n < maxPerConsumer; n++ {
				for {
					if firstErr.Load() != nil {
						return
					}
					msg, err := svc.Dequeue()
					if err != nil {
						storeErr(fmt.Errorf("consumer dequeue: %w", err))
						return
					}
					if msg != nil {
						consumedMu.Lock()
						if _, dup := consumed[msg.ID]; dup {
							consumedMu.Unlock()
							storeErr(fmt.Errorf("duplicate delivery of %s", msg.ID))
							return
						}
						consumed[msg.ID] = msg.Payload
						consumedMu.Unlock()
						break
					}
					if producersDone.Load() {
						stats := svc.Stats()
						if stats.Available == 0 {
							return
						}
					}
					runtime.Gosched()
				}
			}
		}()
	}

	close(start)
	prodWG.Wait()
	producersDone.Store(true)

	done := make(chan struct{})
	go func() {
		consWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Minute):
		t.Fatal("timed out waiting for consumers (possible deadlock or stuck dequeue)")
	}

	if err, _ := firstErr.Load().(error); err != nil {
		t.Fatalf("concurrent phase: %v", err)
	}

	producedMu.Lock()
	producedCopy := copyMap(produced)
	producedMu.Unlock()
	consumedMu.Lock()
	consumedCopy := copyMap(consumed)
	consumedMu.Unlock()

	if len(producedCopy) != totalProduced {
		t.Fatalf("produced %d messages, want %d", len(producedCopy), totalProduced)
	}
	for id, payload := range consumedCopy {
		want, ok := producedCopy[id]
		if !ok {
			t.Fatalf("consumed unknown id %s", id)
		}
		if payload != want {
			t.Fatalf("id %s payload %q, want %q", id, payload, want)
		}
	}

	remainingBeforeClose := svc.Stats()
	if remainingBeforeClose.Total != totalProduced-len(consumedCopy) {
		t.Fatalf("stats.Total = %d, want %d (produced %d, consumed %d)",
			remainingBeforeClose.Total, totalProduced-len(consumedCopy), totalProduced, len(consumedCopy))
	}

	if err := svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	svc2 := reopenService(t, path, model.ModeFIFO)
	drained := make(map[string]string)
	for {
		msg, err := svc2.Dequeue()
		if err != nil {
			t.Fatalf("Dequeue after restart: %v", err)
		}
		if msg == nil {
			break
		}
		if _, dup := drained[msg.ID]; dup {
			t.Fatalf("duplicate delivery after restart: %s", msg.ID)
		}
		if _, already := consumedCopy[msg.ID]; already {
			t.Fatalf("message %s delivered both before and after restart", msg.ID)
		}
		want, ok := producedCopy[msg.ID]
		if !ok {
			t.Fatalf("restart drained unknown id %s", msg.ID)
		}
		if msg.Payload != want {
			t.Fatalf("id %s payload %q after restart, want %q", msg.ID, msg.Payload, want)
		}
		drained[msg.ID] = msg.Payload
	}

	if len(consumedCopy)+len(drained) != totalProduced {
		t.Fatalf("consumed %d + recovered %d = %d, want %d produced",
			len(consumedCopy), len(drained), len(consumedCopy)+len(drained), totalProduced)
	}
	for id := range producedCopy {
		_, inConsumed := consumedCopy[id]
		_, inDrained := drained[id]
		if inConsumed == inDrained {
			t.Fatalf("id %s consumed=%v recovered=%v (must be in exactly one set)", id, inConsumed, inDrained)
		}
	}
	if stats := svc2.Stats(); stats.Total != 0 || stats.Available != 0 || stats.Delayed != 0 {
		t.Fatalf("stats after post-restart drain = %+v, want zeros", stats)
	}
}

func mustEnqueue(t *testing.T, svc *QueueService, req model.EnqueueRequest) model.Message {
	t.Helper()
	msg, err := svc.Enqueue(req)
	if err != nil {
		t.Fatalf("Enqueue(%q): %v", req.Payload, err)
	}
	return msg
}

func payloads(t *testing.T, msgs []model.Message) []string {
	t.Helper()
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = m.Payload
	}
	return out
}

func copyMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
