# Queuemaxxing — Engineering Conventions & Testing Rules

This document defines the mandatory rules for all contributors, **including
automated subagent test writers**. Read `ARCHITECTURE.md` first for system
context. When these rules conflict with convenience, the rules win.

---

## 1. Testing Rules (for humans and subagent test writers)

### 1.1 Mandatory commands

Every change must pass all of the following before it is considered done:

```bash
gofmt -l .            # must print nothing
go vet ./...          # must report nothing
go test -v ./...      # all tests pass; -v feeds the colored reporter (see §1.5)
go test -race -v ./... # all tests pass under the race detector — NON-NEGOTIABLE
```

### 1.2 Test placement and naming

- Tests live next to the code they test: `pkg/queue/service.go` →
  `pkg/queue/service_test.go`, same package (white-box) unless the test only
  exercises the exported API, in which case use `package queue_test`.
- Test names describe behavior, not implementation:
  `TestDequeue_ReturnsHighestPriorityFirst`, not `TestHeapPop2`.
- Use subtests (`t.Run`) and table-driven tests for enumerable cases
  (ordering modes, priority ties, delay boundaries).
- Every test package must install the colored reporter via `TestMain` (see
  §1.5) so pass/fail is marked green ✅ / red ❌.

### 1.3 What subagent test writers MUST cover for any queue change

1. **Ordering matrix:** every behavioral test that involves selection order
   must be run for both `FIFO` and `LIFO` modes (table-driven over mode).
2. **Priority:** higher integer dequeues first; equal priorities fall back to
   the mode's tie-breaker (smallest `Seq` for FIFO, largest for LIFO).
3. **Delay boundaries:** a message with `AvailableAt` in the future is
   invisible; it becomes visible at exactly `AvailableAt <= now`. Never test
   this with `time.Sleep` around real wall-clock deadlines — inject a clock
   (see §1.4).
4. **Persistence round-trip:** enqueue/dequeue/ack against a temp-dir WAL
   (`t.TempDir()`), close, reopen, replay, and assert the recovered state is
   byte-for-byte equivalent (same IDs, statuses, and dequeue order).
5. **Crash simulation:** truncate the WAL file mid-record (torn write) and
   assert recovery succeeds, dropping only the torn tail.
6. **Concurrency:** at least one test per feature that hammers the service
   from ≥ 8 goroutines (mixed enqueue/dequeue/ack) and asserts invariants
   afterwards (no message delivered twice before ack, no message lost, counts
   balance). These tests are the reason `-race` is mandatory.

### 1.4 Determinism rules

- **No sleeps for synchronization.** Use channels, `sync.WaitGroup`, or an
  injected clock. `time.Sleep` in a test is a review-blocking defect unless
  it is bounding a non-determinism check, not creating one.
- **Injectable clock:** components that read time accept a
  `now func() time.Time` (default `time.Now`). Tests substitute a fake to
  test delay semantics deterministically.
- **No shared global state** between tests. Every test constructs its own
  service with its own `t.TempDir()` WAL path so tests can run with
  `-shuffle=on` and in parallel (`t.Parallel()` where safe).
- Tests must not depend on map iteration order, goroutine scheduling order,
  or timestamp uniqueness — `Seq` is the only total order.

### 1.5 Test output format (green / red marks)

Raw `--- PASS:` / `--- FAIL:` chrome is hard to scan. Every `*_test.go`
package MUST route `TestMain` through `pkg/testutil.Run` so results reprint
with colored marks:

```go
func TestMain(m *testing.M) {
	testutil.Run(m)
}
```

Required rendering:

- Passing tests: green, prefixed with `✅`
- Failing tests: red, prefixed with `❌`
- End of run: a `Results: P/T passing, F/T failing` summary

Example:

```
✅ WriteEnqueue_AppendAndReadBack (0.00s)
✅ Recover_CrashSimulation_ReturnsUnconsumedMessages (0.00s)
❌ Recover_CorruptMiddleLineIsError (0.00s)
    Recover: expected error for corrupt middle line, got nil

============================
Results: 2/3 passing, 1/3 failing
============================
```

Rules for subagent test writers:

- Do **not** copy-paste a custom `TestMain` formatter. Call `testutil.Run`.
- Do **not** strip `t.Errorf` / `t.Fatalf` details: failure text stays
  uncolored so diffs remain copy-pasteable.
- Do **not** color production logs or HTTP responses; marks are test-output
  only.
- `testutil.Run` forces `-test.v` so marks appear even when the caller omits
  `-v`. Prefer `go test -v ./...` anyway so CI logs match local output.

---

## 2. Concurrency Conventions

### 2.1 Race detector enforcement

`go test -race -v ./...` is part of the definition of done and must be wired
into CI. A change that passes plain tests but fails under `-race` is broken.

### 2.2 Lock ordering (deadlock avoidance)

- There is a **single canonical lock order**. If more than one lock must be
  held, acquire in this order and release in reverse:

  1. `QueueService.mu` (cross-component queue state)
  2. `FrankensteinQueue.mu` (engine internals, `pkg/engine`)
  3. `WAL.mu` (log writer)

  The engine and WAL locks are **leaf locks**: code holding them must never
  call into any other locked component, and they are never held
  simultaneously with each other — **never call back into the service from
  engine or WAL code.**
- Never hold any lock across:
  - an HTTP response write,
  - a channel send/receive that another lock-holder may be blocked on,
  - arbitrary user callbacks.
  Holding `mu` across the WAL append + `Sync()` is the one sanctioned
  exception (it is required for log/state order equivalence — see
  `ARCHITECTURE.md` §5).
- Prefer one coarse mutex that is provably correct over fine-grained locks
  that are fast and wrong. Optimize only with benchmarks in hand.
- Use `defer mu.Unlock()` immediately after locking unless there is a
  measured reason not to; early returns must never leak a held lock.
- `sync.RWMutex` is permitted for read-mostly paths (`stats`), but writes to
  any shared field always require the write lock — no "benign races," the
  race detector defines the truth.

### 2.3 Non-blocking discipline

- Handlers never block waiting for queue conditions while holding a lock.
  Empty dequeue returns immediately (`204 No Content`); long-polling, if ever
  added, must wait **outside** the critical section.
- No goroutine may be started without a defined shutdown path (context
  cancellation or close signal) — no leaked goroutines in tests
  (`goleak`-style checks encouraged).

---

## 3. Storage Conventions

### 3.1 File sync semantics

- **Durability rule:** a WAL record is durable only after `file.Sync()`
  returns nil. The in-memory mutation and the HTTP success response must both
  happen **after** the sync. Write → Sync → Mutate → Respond, always.
- A failed `Sync()` is a hard error: the operation fails, memory is NOT
  mutated, and the error propagates to the client as `500`. Never "log and
  continue" a durability failure.
- Each record is written as one buffered write flushed as a single line;
  never interleave partial lines from concurrent callers (the WAL writer is
  mutex-guarded).
- Files are opened with `O_APPEND|O_CREATE|O_WRONLY`. No seeks, no in-place
  edits — the log is append-only by construction.

### 3.2 Log replay idempotency

- Replay must be a **pure fold** over records: `state = apply(state, record)`
  with no side effects (no network, no re-logging, no time reads).
- `apply` is idempotent and tolerant of records made redundant by later
  compaction or duplicated by retries:
  - `ENQUEUE` for an existing ID → no-op.
  - `DEQUEUE` for a missing or already-inflight ID → no-op.
  - `ACK` for a missing ID → no-op.
  Redundant records are silently skipped (optionally counted for stats);
  they are never errors.
- Replaying the same log twice must produce identical state to replaying it
  once. There must be a test asserting exactly this.
- The final line of a log may be torn (crash mid-write): a line that fails
  to parse **at the end of the file** is discarded; a corrupt line in the
  middle of the file is a hard error (it implies bytes were lost).
- After replay, any message left `INFLIGHT` (dequeued, never acked) is
  requeued as `PENDING` — at-least-once delivery is the contract.

---

## 4. Code Style

### 4.1 Error handling

- Standard idiomatic Go: return `error` as the last value; check every
  error. No `_ =` discards for errors that can matter (that includes
  `Sync()`, `Close()` on writable files, and `json.Marshal` of user input).
- Wrap with context using `fmt.Errorf("appending enqueue record: %w", err)`.
  Wrap at the point where you can add information; don't re-wrap the same
  error at every level.
- Define sentinel errors for conditions callers branch on
  (`ErrEmpty`, `ErrNotFound`, `ErrAlreadyAcked`) and match with
  `errors.Is` / `errors.As` — never string-compare error messages.
- Map errors to HTTP status codes only in the HTTP layer; inner packages
  know nothing about HTTP.

### 4.2 No panic in production code

- `panic` is forbidden in all non-test code paths reachable at runtime.
  Invalid input returns an error; impossible states return an error too
  (better a `500` than a crashed queue holding unacked messages).
- The only tolerated panics are true programmer-error guards in `init`-time
  configuration (e.g., an invalid hardcoded regexp) — and each one requires
  a justifying comment.
- HTTP handlers are wrapped in a recovery middleware as a last-resort
  safety net; its triggering is treated as a bug to fix, not a feature to
  rely on.

### 4.3 General

- `gofmt` (or `goimports`) formatting is mandatory; CI rejects unformatted
  code.
- Exported identifiers have doc comments; packages have a package comment.
- Small interfaces, defined at the point of use (the consumer declares what
  it needs); accept interfaces, return concrete types.
- No global mutable state; dependencies are passed explicitly through
  constructors (`queue.NewService(wal, opts...)`).
