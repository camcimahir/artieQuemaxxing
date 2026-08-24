// Package e2e contains full-stack crash-recovery tests for Queuemaxxing.
//
// Each test drives a real HTTP server (pkg/api) backed by a real
// QueueService (pkg/service) writing a real WAL on disk (pkg/storage). A
// hard application crash is simulated by abandoning the running service
// without ever calling Close(): no shutdown code runs, the WAL file
// descriptor is orphaned, and the file holds exactly the bytes the
// write→fsync protocol had made durable — the same disk state a SIGKILL
// leaves behind. A brand-new QueueService opened on the same WAL file must
// reconstruct the surviving state precisely.
package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"queuemaxxing/pkg/api"
	"queuemaxxing/pkg/model"
	"queuemaxxing/pkg/service"
)

// t0 is the fixed epoch for the injected clock. Delay semantics are tested
// with a fake clock, never wall-clock sleeps (SKILLS.md §1.4).
var t0 = time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)

var modes = []model.QueueMode{model.ModeFIFO, model.ModeLIFO}

// fakeClock is a race-safe, manually advanced clock. It is shared between
// the test goroutine and the HTTP handler goroutines, so reads and writes
// go through an atomic pointer.
type fakeClock struct {
	v atomic.Pointer[time.Time]
}

func newFakeClock(start time.Time) *fakeClock {
	c := &fakeClock{}
	c.Set(start)
	return c
}

func (c *fakeClock) Now() time.Time  { return *c.v.Load() }
func (c *fakeClock) Set(t time.Time) { c.v.Store(&t) }

// env is one running "process": a QueueService plus the HTTP server in
// front of it, driven over real sockets.
type env struct {
	t       *testing.T
	svc     *service.QueueService
	srv     *httptest.Server
	client  *http.Client
	crashed bool
}

// startEnv boots a service on walPath and serves it over HTTP. A nil clock
// keeps the default time.Now (used by the concurrency test, which only
// relies on far-future delays staying invisible).
func startEnv(t *testing.T, walPath string, mode model.QueueMode, clock *fakeClock) *env {
	t.Helper()
	var opts []service.Option
	if clock != nil {
		opts = append(opts, service.WithNow(clock.Now))
	}
	svc, err := service.NewQueueService(walPath, mode, opts...)
	if err != nil {
		t.Fatalf("NewQueueService(%q, %s): %v", walPath, mode, err)
	}
	srv := httptest.NewServer(api.NewHandler(svc))
	e := &env{t: t, svc: svc, srv: srv, client: srv.Client()}
	t.Cleanup(func() {
		if e.crashed {
			// Deliberately abandoned: the crash simulation depends on
			// Close() never running for this instance *before* recovery —
			// and it did not, because t.Cleanup funcs run only after the
			// test body has finished asserting on the recovered state.
			// Releasing the orphaned descriptor here cannot change what
			// recovery saw: the WAL is unbuffered and fsynced per record,
			// so the bytes on disk were already final the moment crash()
			// returned. This exists purely so t.TempDir can unlink the
			// file on Windows, which refuses to delete files that still
			// have an open handle.
			_ = e.svc.Close()
			return
		}
		e.srv.Close()
		if err := e.svc.Close(); err != nil {
			t.Errorf("closing service: %v", err)
		}
	})
	return e
}

// crash simulates a hard process kill. The HTTP listener is torn down (in
// a real crash the sockets die with the process), but QueueService.Close —
// and therefore WAL.Close — is deliberately never called: no clean
// shutdown path runs, and the WAL file keeps exactly the durable bytes.
func (e *env) crash() {
	e.crashed = true
	e.srv.CloseClientConnections()
	e.srv.Close()
}

// tryEnqueue posts one message and returns the server-assigned record. It
// never touches testing.T, so concurrent workers may call it.
func (e *env) tryEnqueue(payload string, priority int, delaySeconds int64) (model.Message, error) {
	var msg model.Message
	body, err := json.Marshal(model.EnqueueRequest{Payload: payload, Priority: priority, DelaySeconds: delaySeconds})
	if err != nil {
		return msg, fmt.Errorf("marshal enqueue request: %w", err)
	}
	resp, err := e.client.Post(e.srv.URL+"/enqueue", "application/json", bytes.NewReader(body))
	if err != nil {
		return msg, fmt.Errorf("POST /enqueue: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return msg, fmt.Errorf("POST /enqueue: reading body: %w", err)
	}
	if resp.StatusCode != http.StatusCreated {
		return msg, fmt.Errorf("POST /enqueue: status %d, want 201; body=%s", resp.StatusCode, raw)
	}
	if err := json.Unmarshal(raw, &msg); err != nil {
		return msg, fmt.Errorf("POST /enqueue: decode: %w; body=%s", err, raw)
	}
	return msg, nil
}

func (e *env) enqueue(payload string, priority int, delaySeconds int64) model.Message {
	e.t.Helper()
	msg, err := e.tryEnqueue(payload, priority, delaySeconds)
	if err != nil {
		e.t.Fatal(err)
	}
	return msg
}

// tryDequeue returns the delivered message, nil for 204 No Content, or an
// error. It never touches testing.T, so concurrent workers may call it.
func (e *env) tryDequeue() (*model.Message, error) {
	resp, err := e.client.Post(e.srv.URL+"/dequeue", "application/json", http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("POST /dequeue: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("POST /dequeue: reading body: %w", err)
	}
	switch resp.StatusCode {
	case http.StatusNoContent:
		return nil, nil
	case http.StatusOK:
		var msg model.Message
		if err := json.Unmarshal(raw, &msg); err != nil {
			return nil, fmt.Errorf("POST /dequeue: decode: %w; body=%s", err, raw)
		}
		return &msg, nil
	default:
		return nil, fmt.Errorf("POST /dequeue: status %d; body=%s", resp.StatusCode, raw)
	}
}

func (e *env) dequeue() *model.Message {
	e.t.Helper()
	msg, err := e.tryDequeue()
	if err != nil {
		e.t.Fatal(err)
	}
	return msg
}

func (e *env) stats() model.QueueStats {
	e.t.Helper()
	resp, err := e.client.Get(e.srv.URL + "/stats")
	if err != nil {
		e.t.Fatalf("GET /stats: %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		e.t.Fatalf("GET /stats: reading body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		e.t.Fatalf("GET /stats: status %d; body=%s", resp.StatusCode, raw)
	}
	var st model.QueueStats
	if err := json.Unmarshal(raw, &st); err != nil {
		e.t.Fatalf("GET /stats: decode: %v; body=%s", err, raw)
	}
	return st
}

// rankCmp reproduces the documented selection rule (ARCHITECTURE.md §4):
// highest Priority first, ties broken by Seq — smallest for FIFO, largest
// for LIFO. Expected orders are computed from this reference model rather
// than hardcoded.
func rankCmp(mode model.QueueMode) func(a, b model.Message) int {
	return func(a, b model.Message) int {
		if a.Priority != b.Priority {
			if a.Priority > b.Priority {
				return -1
			}
			return 1
		}
		if a.Seq == b.Seq {
			return 0
		}
		first := a.Seq < b.Seq
		if mode == model.ModeLIFO {
			first = !first
		}
		if first {
			return -1
		}
		return 1
	}
}

// visibleAt returns the messages from pool with AvailableAt <= now, sorted
// into the exact order the queue must deliver them for the given mode.
func visibleAt(pool []model.Message, now time.Time, mode model.QueueMode) []model.Message {
	var out []model.Message
	for _, m := range pool {
		if m.Available(now) {
			out = append(out, m)
		}
	}
	slices.SortFunc(out, rankCmp(mode))
	return out
}

func assertSameMessage(t *testing.T, got, want model.Message) {
	t.Helper()
	if got.Seq != want.Seq || got.Payload != want.Payload || got.Priority != want.Priority {
		t.Fatalf("recovered message %s mutated:\n got seq=%d payload=%q priority=%d\nwant seq=%d payload=%q priority=%d",
			want.ID, got.Seq, got.Payload, got.Priority, want.Seq, want.Payload, want.Priority)
	}
	if !got.EnqueuedAt.Equal(want.EnqueuedAt) {
		t.Fatalf("recovered message %s EnqueuedAt = %v, want %v", want.ID, got.EnqueuedAt, want.EnqueuedAt)
	}
	if !got.AvailableAt.Equal(want.AvailableAt) {
		t.Fatalf("recovered message %s AvailableAt = %v, want %v", want.ID, got.AvailableAt, want.AvailableAt)
	}
}

// TestE2E_CrashRecovery_SurvivorsDrainInPriorityDelayOrder is the
// end-to-end crash recovery scenario:
//
//  1. spin up the queue service (real HTTP handlers, real WAL on disk),
//  2. enqueue 100 messages with mixed priorities and delays,
//  3. dequeue 30 messages,
//  4. hard-crash the process — no Close(), plus a torn record appended to
//     the WAL as a crash mid-append would leave,
//  5. re-initialize a brand-new QueueService on the exact same WAL file,
//  6. verify exactly 70 messages remain and drain in perfect
//     priority/delay order.
//
// Selection order depends on the queue mode, so the whole scenario is
// table-driven over FIFO and LIFO (SKILLS.md §1.3).
func TestE2E_CrashRecovery_SurvivorsDrainInPriorityDelayOrder(t *testing.T) {
	t.Parallel()
	for _, mode := range modes {
		mode := mode
		t.Run(string(mode), func(t *testing.T) {
			t.Parallel()
			walPath := filepath.Join(t.TempDir(), "queue.wal")
			clock := newFakeClock(t0)

			// ── 1. Spin up the queue service ─────────────────────────
			env1 := startEnv(t, walPath, mode, clock)

			// ── 2. Enqueue 100 messages with mixed priorities/delays ─
			// Priorities cycle pseudo-randomly through 0..9. Delays:
			//   i%5 ∈ {0,1,2} → visible immediately   (60 messages)
			//   i%5 == 3     → visible at t0+60s      (20 messages)
			//   i%5 == 4     → visible at t0+120s     (20 messages)
			const total = 100
			enqueued := make([]model.Message, 0, total)
			for i := 0; i < total; i++ {
				var delay int64
				switch i % 5 {
				case 3:
					delay = 60
				case 4:
					delay = 120
				}
				msg := env1.enqueue(fmt.Sprintf("msg-%03d", i), (i*7)%10, delay)
				wantAvail := t0.Add(time.Duration(delay) * time.Second)
				if !msg.AvailableAt.Equal(wantAvail) {
					t.Fatalf("message %d: AvailableAt = %v, want %v", i, msg.AvailableAt, wantAvail)
				}
				enqueued = append(enqueued, msg)
			}
			if st := env1.stats(); st.Total != 100 || st.Available != 60 || st.Delayed != 40 {
				t.Fatalf("stats after enqueue = %+v, want total=100 ready=60 delayed=40", st)
			}

			// ── 3. Dequeue 30 messages, checking their order too ─────
			visible := visibleAt(enqueued, t0, mode)
			if len(visible) != 60 {
				t.Fatalf("reference model: %d visible at t0, want 60 (test bug)", len(visible))
			}
			consumed := make(map[string]bool, 30)
			for i, want := range visible[:30] {
				got := env1.dequeue()
				if got == nil {
					t.Fatalf("pre-crash dequeue %d: got 204, want %s", i, want.ID)
				}
				if got.ID != want.ID {
					t.Fatalf("pre-crash dequeue %d: got %s (pri %d, seq %d), want %s (pri %d, seq %d)",
						i, got.ID, got.Priority, got.Seq, want.ID, want.Priority, want.Seq)
				}
				consumed[got.ID] = true
			}

			// ── 4. Hard crash: abandon the service, never Close() ────
			env1.crash()

			// A crash can also catch the WAL mid-append. Model it by
			// appending a torn, newline-less ENQUEUE fragment straight to
			// the file. Recovery must discard it: the record was never
			// fsynced, so its producer never received a 201.
			f, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0o644)
			if err != nil {
				t.Fatalf("open WAL for torn append: %v", err)
			}
			if _, err := f.WriteString(`{"v":1,"op":"ENQUEUE","seq":10001,"id":"m-torn","payload":"died-mid-wr`); err != nil {
				t.Fatalf("appending torn record: %v", err)
			}
			if err := f.Close(); err != nil {
				t.Fatalf("closing torn append: %v", err)
			}

			// ── 5. Brand-new QueueService on the exact same WAL ──────
			env2 := startEnv(t, walPath, mode, clock)

			// ── 6. Exactly 70 messages survived ──────────────────────
			remaining := make([]model.Message, 0, total-len(consumed))
			for _, m := range enqueued {
				if !consumed[m.ID] {
					remaining = append(remaining, m)
				}
			}
			if len(remaining) != 70 {
				t.Fatalf("reference model: %d remaining, want 70 (test bug)", len(remaining))
			}
			if st := env2.stats(); st.Total != 70 || st.Available != 30 || st.Delayed != 40 {
				t.Fatalf("stats after recovery = %+v, want total=70 ready=30 delayed=40", st)
			}

			// Drain in three phases, advancing the injected clock across
			// each delay boundary. Every phase must deliver its batch in
			// exact priority/Seq order, then report empty.
			phases := []struct {
				name  string
				now   time.Time
				count int
			}{
				{name: "visible immediately", now: t0, count: 30},
				{name: "after the 60s delay", now: t0.Add(60 * time.Second), count: 20},
				{name: "after the 120s delay", now: t0.Add(120 * time.Second), count: 20},
			}
			drained := make(map[string]bool, len(remaining))
			recovered := make([]model.Message, 0, len(remaining))
			for i, ph := range phases {
				if i > 0 {
					// One second before the boundary the batch must still
					// be invisible: AvailableAt <= now is exact.
					clock.Set(ph.now.Add(-time.Second))
					if msg := env2.dequeue(); msg != nil {
						t.Fatalf("%s: message %s visible 1s before its AvailableAt", ph.name, msg.ID)
					}
				}
				clock.Set(ph.now)

				expect := make([]model.Message, 0, ph.count)
				for _, m := range visibleAt(remaining, ph.now, mode) {
					if !drained[m.ID] {
						expect = append(expect, m)
					}
				}
				if len(expect) != ph.count {
					t.Fatalf("%s: reference model expects %d messages, plan says %d (test bug)", ph.name, len(expect), ph.count)
				}

				for j, want := range expect {
					got := env2.dequeue()
					if got == nil {
						t.Fatalf("%s: dequeue %d/%d returned nothing, want %s (pri %d, seq %d)",
							ph.name, j+1, len(expect), want.ID, want.Priority, want.Seq)
					}
					if got.ID != want.ID {
						t.Fatalf("%s: dequeue %d/%d order violation: got %s (pri %d, seq %d), want %s (pri %d, seq %d)",
							ph.name, j+1, len(expect), got.ID, got.Priority, got.Seq, want.ID, want.Priority, want.Seq)
					}
					assertSameMessage(t, *got, want)
					drained[got.ID] = true
					recovered = append(recovered, *got)
				}

				// Nothing further may be visible until the next boundary.
				if extra := env2.dequeue(); extra != nil {
					t.Fatalf("%s: extra message %s delivered; a delayed batch leaked early", ph.name, extra.ID)
				}
				left := len(remaining) - len(recovered)
				if st := env2.stats(); st.Total != left || st.Available != 0 || st.Delayed != left {
					t.Fatalf("%s: stats = %+v, want total=%d ready=0 delayed=%d", ph.name, st, left, left)
				}
			}

			if len(recovered) != 70 {
				t.Fatalf("recovered %d messages, want exactly 70", len(recovered))
			}

			// Ledger: the 30 consumed before the crash are gone forever
			// (destructive dequeue), the other 70 were delivered exactly
			// once after recovery, and together they cover all 100.
			seen := make(map[string]bool, total)
			for id := range consumed {
				seen[id] = true
			}
			for _, m := range recovered {
				if consumed[m.ID] {
					t.Fatalf("message %s delivered both before and after the crash", m.ID)
				}
				if seen[m.ID] {
					t.Fatalf("message %s delivered twice after recovery", m.ID)
				}
				seen[m.ID] = true
			}
			for _, m := range enqueued {
				if !seen[m.ID] {
					t.Fatalf("message %s was lost across the crash", m.ID)
				}
			}
		})
	}
}

// TestE2E_ConcurrentLoadThenCrash_NoLossNoDuplicates hammers the HTTP API
// from concurrent producers and consumers (SKILLS.md §1.3.6), hard-crashes
// the process twice, and proves the ledger balances: every produced
// message is delivered exactly once across both crashes, and far-future
// delayed messages stay invisible until a recovered service's clock passes
// their AvailableAt.
func TestE2E_ConcurrentLoadThenCrash_NoLossNoDuplicates(t *testing.T) {
	t.Parallel()
	walPath := filepath.Join(t.TempDir(), "queue.wal")
	env1 := startEnv(t, walPath, model.ModeFIFO, nil) // real clock

	const (
		producers    = 10
		perProducer  = 20
		consumers    = 8
		takesPerCons = 12
		longDelaySec = 3600 // never matures under the real clock
	)
	const totalProduced = producers * perProducer

	var (
		prodWG, consWG sync.WaitGroup
		producedMu     sync.Mutex
		produced       = make(map[string]string, totalProduced)
		delayedIDs     = make(map[string]bool)
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
			for j := 0; j < perProducer; j++ {
				var delay int64
				if j%10 == 9 {
					delay = longDelaySec
				}
				payload := fmt.Sprintf("p-%02d-%02d", p, j)
				msg, err := env1.tryEnqueue(payload, (p+j)%7, delay)
				if err != nil {
					storeErr(fmt.Errorf("producer %d message %d: %w", p, j, err))
					return
				}
				producedMu.Lock()
				produced[msg.ID] = payload
				if delay > 0 {
					delayedIDs[msg.ID] = true
				}
				producedMu.Unlock()
			}
		}()
	}

	consWG.Add(consumers)
	for c := 0; c < consumers; c++ {
		c := c
		go func() {
			defer consWG.Done()
			<-start
			for n := 0; n < takesPerCons; n++ {
				for {
					if firstErr.Load() != nil {
						return
					}
					msg, err := env1.tryDequeue()
					if err != nil {
						storeErr(fmt.Errorf("consumer %d: %w", c, err))
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
						// Anything still enqueued after this point is the
						// recovery phase's job to account for.
						return
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
	if len(produced) != totalProduced {
		t.Fatalf("produced %d messages, want %d", len(produced), totalProduced)
	}
	for id := range consumed {
		if delayedIDs[id] {
			t.Fatalf("delayed message %s consumed %ds before its AvailableAt", id, longDelaySec)
		}
	}
	visibleProduced := totalProduced - len(delayedIDs)

	// ── Crash #1: mid-workload state, no Close() ─────────────────────
	env1.crash()

	env2 := startEnv(t, walPath, model.ModeFIFO, nil)
	if st := env2.stats(); st.Total != totalProduced-len(consumed) ||
		st.Available != visibleProduced-len(consumed) || st.Delayed != len(delayedIDs) {
		t.Fatalf("stats after crash #1 = %+v, want total=%d ready=%d delayed=%d",
			st, totalProduced-len(consumed), visibleProduced-len(consumed), len(delayedIDs))
	}

	// Drain every visible survivor; the delayed ones must not appear.
	drained := make(map[string]string)
	for {
		msg := env2.dequeue()
		if msg == nil {
			break
		}
		if _, dup := drained[msg.ID]; dup {
			t.Fatalf("duplicate delivery of %s after crash #1", msg.ID)
		}
		if _, was := consumed[msg.ID]; was {
			t.Fatalf("message %s delivered both before and after crash #1", msg.ID)
		}
		if delayedIDs[msg.ID] {
			t.Fatalf("delayed message %s became visible early after crash #1", msg.ID)
		}
		want, ok := produced[msg.ID]
		if !ok {
			t.Fatalf("unknown message %s appeared after crash #1", msg.ID)
		}
		if msg.Payload != want {
			t.Fatalf("message %s payload = %q after recovery, want %q", msg.ID, msg.Payload, want)
		}
		drained[msg.ID] = msg.Payload
	}
	if got, want := len(drained), visibleProduced-len(consumed); got != want {
		t.Fatalf("drained %d visible messages after crash #1, want %d", got, want)
	}
	if st := env2.stats(); st.Total != len(delayedIDs) || st.Available != 0 || st.Delayed != len(delayedIDs) {
		t.Fatalf("stats after visible drain = %+v, want total=%d ready=0 delayed=%d", st, len(delayedIDs), len(delayedIDs))
	}

	// ── Crash #2: only delayed messages remain ───────────────────────
	env2.crash()

	// The recovered service's injected clock sits past every AvailableAt,
	// so the delayed survivors — and only they — must all drain now.
	env3 := startEnv(t, walPath, model.ModeFIFO, newFakeClock(time.Now().Add(2*time.Hour)))
	late := make(map[string]string)
	for {
		msg := env3.dequeue()
		if msg == nil {
			break
		}
		if _, dup := late[msg.ID]; dup {
			t.Fatalf("duplicate delivery of %s after crash #2", msg.ID)
		}
		if !delayedIDs[msg.ID] {
			t.Fatalf("message %s resurrected after crash #2 despite earlier delivery", msg.ID)
		}
		if msg.Payload != produced[msg.ID] {
			t.Fatalf("message %s payload = %q after crash #2, want %q", msg.ID, msg.Payload, produced[msg.ID])
		}
		late[msg.ID] = msg.Payload
	}
	if len(late) != len(delayedIDs) {
		t.Fatalf("recovered %d delayed messages after crash #2, want %d", len(late), len(delayedIDs))
	}
	if st := env3.stats(); st.Total != 0 || st.Available != 0 || st.Delayed != 0 {
		t.Fatalf("stats after full drain = %+v, want zeros", st)
	}

	// Ledger: every produced message was delivered in exactly one of the
	// three windows — before crash #1, between crashes, or after crash #2.
	for id := range produced {
		n := 0
		if _, ok := consumed[id]; ok {
			n++
		}
		if _, ok := drained[id]; ok {
			n++
		}
		if _, ok := late[id]; ok {
			n++
		}
		if n != 1 {
			t.Fatalf("message %s delivered %d times across crashes, want exactly once", id, n)
		}
	}
}
