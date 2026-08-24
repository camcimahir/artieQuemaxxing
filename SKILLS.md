# Queuemaxxing - Engineering Conventions & Testing Rules

The mandatory rules for anyone changing this codebase. Read
[`ARCHITECTURE.md`](ARCHITECTURE.md) first for system context. When these
rules conflict with convenience, the rules win.

Every rule below describes something the code does today. Where a rule refers
to behavior that does not exist yet, it is marked as such.

---

## 1. Testing Rules

### 1.1 Mandatory commands

Every change must pass all of the following before it is considered done:

```bash
gofmt -l .              # must print nothing
go vet ./...            # must report nothing
go test -v ./...        # all tests pass; -v is what surfaces the ✅/❌ marks
go test -race ./...     # all tests pass under the race detector - NON-NEGOTIABLE
```

Use `-v` deliberately. `testutil.Run` appends `-test.v` to the test *binary*,
but the `go test` driver still discards a passing package's stdout unless `-v`
is passed to `go test` itself, so a plain `go test ./...` prints only the usual
one-line package summaries and the marks appear solely for failing packages.

A `.gitattributes` pins every text file to LF (`* text=auto eol=lf`). Without
it a clone on Windows gets CRLF working-tree files and `gofmt -l .` lists
every file in the repo - do not remove it.

`-race` requires cgo. On Linux and macOS that works out of the box; on Windows
it needs a C toolchain on `PATH` (MinGW-w64 or similar) and
`CGO_ENABLED=1`. Without one, `go test -race` fails with
`-race requires cgo` before running a single test.

### 1.2 Test placement and naming

- Tests live next to the code they test: `pkg/service/queue_service.go` →
  `pkg/service/queue_service_test.go`. Use the same package for white-box
  tests that need internals - `pkg/service` injects `svc.now` directly and
  `pkg/api` calls the unexported `recoverMiddleware`; use `package <name>_test`
  when the test only exercises the exported API, as `pkg/engine` and
  `pkg/storage` do.
- Test names describe behavior, not implementation:
  `TestDequeue_DelayedInvisibleUntilAvailableAt`, not `TestHeapPop2`.
- Use subtests (`t.Run`) and table-driven tests for enumerable cases -
  ordering modes, priority ties, delay boundaries.
- Every test package installs the colored reporter via `TestMain` (see §1.5).

### 1.3 What every queue change must cover

1. **Ordering matrix:** any behavioral test involving selection order runs for
   both `FIFO` and `LIFO`, table-driven over `modes`.
2. **Priority:** higher integer dequeues first; equal priorities fall back to
   the mode's tie-breaker (smallest `Seq` for FIFO, largest for LIFO).
3. **Delay boundaries:** a message with `AvailableAt` in the future is
   invisible and becomes visible at exactly `AvailableAt <= now` - the
   boundary is inclusive, and there is a test asserting it is still invisible
   one instant early. Never test this with `time.Sleep` around real
   wall-clock deadlines; inject a clock (§1.4).
4. **Persistence round-trip:** enqueue and dequeue against a `t.TempDir()`
   WAL, close, reopen, replay, and assert the recovered messages and their
   dequeue order are identical to what a crash-free run would have produced.
5. **Crash simulation:** append a torn half-record to the WAL and assert
   recovery succeeds, dropping only the torn tail; assert that a corrupt line
   in the *middle* of the file is a hard error instead.
6. **Concurrency:** at least one test per feature that hammers the component
   from ≥ 8 goroutines with mixed enqueues and dequeues, then asserts the
   invariants - no message delivered twice, none lost, counts balance. These
   tests are the reason `-race` is mandatory.

### 1.4 Determinism rules

- **No sleeps for synchronization.** Use channels, `sync.WaitGroup`, or an
  injected clock. `time.Sleep` in a test is a review-blocking defect unless it
  is bounding a non-determinism check rather than creating one.
- **Injectable clock:** `QueueService` accepts `WithNow(func() time.Time)` and
  the engine takes `now` as an argument on every time-dependent method. Tests
  substitute a fake so delay semantics are exercised without waiting.
- **No shared global state** between tests. Every test constructs its own
  service with its own `t.TempDir()` WAL path, so the suite runs correctly
  with `-shuffle=on` and with `t.Parallel()`.
- Tests must not depend on map iteration order, goroutine scheduling, or
  timestamp uniqueness - `Seq` is the only total order.
- A test that deliberately abandons a resource to simulate a crash must still
  release it in `t.Cleanup`, *after* the recovery assertions have run. Windows
  refuses to unlink a file with an open handle, so a leaked descriptor turns
  `t.TempDir` cleanup into a test failure on that platform even when the
  logic under test is correct.

### 1.5 Test output format (green / red marks)

Raw `--- PASS:` / `--- FAIL:` chrome is hard to scan. Every test package
routes `TestMain` through `pkg/testutil.Run` so results reprint with colored
marks:

```go
func TestMain(m *testing.M) {
	testutil.Run(m)
}
```

Required rendering:

- Passing tests: green, prefixed with `✅`
- Failing tests: red, prefixed with `❌`
- Skipped tests: prefixed with `⏭`
- End of run: a `Results: P/T passing, F/T failing` summary

```
✅ TestWriteEnqueue_AppendAndReadBack (0.00s)
✅ TestRecover_CrashSimulation_ReturnsUnconsumedMessages (0.00s)
    wal_test.go: Recover: expected error for corrupt middle line, got nil
❌ TestRecover_CorruptMiddleLineIsError (0.00s)

============================
Results: 2/3 passing, 1/3 failing
============================
```

The detail line sits *above* its mark: Go emits `t.Errorf` output before the
`--- FAIL:` line, and the reporter preserves stream order instead of buffering
per test.

Rules:

- Do **not** copy-paste a custom `TestMain` formatter. Call `testutil.Run`.
- Do **not** strip `t.Errorf` / `t.Fatalf` details: failure text passes through
  uncolored, keeping its `file:line:` prefix, so diffs remain copy-pasteable.
  The reporter drops only blank lines and Go's own `ok` / `FAIL` package
  summaries, which it replaces with the totals block.
- Do **not** color production logs or HTTP responses; marks are test output
  only.

---

## 2. Concurrency Conventions

### 2.1 Race detector enforcement

`go test -race ./...` is part of the definition of done. A change that passes
plain tests but fails under `-race` is broken. Nothing here is wired into CI
yet - there is no CI configuration in this repository - so running it is on
you.

### 2.2 Lock ordering (deadlock avoidance)

- There is a **single canonical lock order**. If more than one lock must be
  held, acquire in this order and release in reverse:

  1. `QueueService.mu` (cross-component queue state, `pkg/service`)
  2. `FrankensteinQueue.mu` (engine internals, `pkg/engine`)
  3. `WAL.mu` (log writer, `pkg/storage`)

  The engine and WAL locks are **leaf locks**: code holding them must never
  call into any other locked component, and they are never held simultaneously
  with each other - **never call back into the service from engine or WAL
  code.**
- Never hold any lock across:
  - an HTTP response write,
  - a channel send/receive that another lock-holder may be blocked on,
  - arbitrary user callbacks.

  Holding `QueueService.mu` across the WAL append and `Sync()` is the one
  sanctioned exception. It is required for log order to equal state-mutation
  order (see `ARCHITECTURE.md` §5), and it is why throughput is bounded by
  serialized `fsync`.
- Prefer one coarse mutex that is provably correct over fine-grained locks
  that are fast and wrong. Optimize only with benchmarks in hand.
- Use `defer mu.Unlock()` immediately after locking unless there is a measured
  reason not to; early returns must never leak a held lock.
- `sync.RWMutex` is used for the read-mostly paths (`Stats`, `Snapshot`), but
  writes to any shared field always take the write lock. No "benign races" -
  the race detector defines the truth.

### 2.3 Non-blocking discipline

- Handlers never block waiting for queue conditions. An empty dequeue returns
  `204 No Content` immediately. Long-polling, if it is ever added, must wait
  **outside** the critical section.
- No goroutine may be started without a defined shutdown path. The queue
  packages start none; `cmd/server` starts exactly one, to run
  `srv.ListenAndServe`, and it terminates when `srv.Shutdown` makes that call
  return. Any new background worker - the lease expiry sweeper, for
  example - must ship with its own cancellation.

---

## 3. Storage Conventions

### 3.1 File sync semantics

- **Durability rule:** a WAL record is durable only after `file.Sync()`
  returns nil. The in-memory mutation and the HTTP success response both
  happen **after** the sync. Write → Sync → Mutate → Respond, always.
- A failed `Sync()` is a hard error: the operation fails, memory is not
  mutated, and the error propagates to the client as `500`. Never "log and
  continue" a durability failure.
- Each record is marshaled and written as a single `write` of one complete
  line. Concurrent callers never interleave partial lines because the writer
  is mutex-guarded.
- The log file is opened `O_CREATE|O_APPEND|O_RDWR`. `O_APPEND` is what makes
  it append-only by construction; `O_RDWR` rather than `O_WRONLY` is required
  because `Recover` seeks to the start and reads the same handle. There are no
  seeks on the write path and no in-place edits, ever.

### 3.2 Log replay idempotency

- Replay is a **pure fold** over records: `state = apply(state, record)`, with
  no side effects - no network, no re-logging, no clock reads.
- `apply` is idempotent and tolerant of redundant records:
  - `ENQUEUE` for an ID already live → no-op.
  - `DEQUEUE` for a missing ID → no-op.
  - An unrecognized `op` → skipped, never an error. This is what lets a log
    written by a newer build still replay under an older one.
- Replaying the same log twice must produce identical state to replaying it
  once. `TestRecover_IdempotentReplay` asserts exactly this.
- The final line of a log may be torn (crash mid-write): a line that fails to
  parse **at the end of the file** is discarded; a line that fails to parse in
  the middle is a hard error, because it implies bytes were lost rather than
  never written.
- Recovery must not buffer the whole file. Distinguishing a torn tail from
  mid-file corruption needs exactly one line of lookahead, so recovery holds
  one pending record and no more. Fold state is the live set of unconsumed
  messages; consumed IDs are dropped as their tombstones apply, so peak
  memory is O(live set + longest record), not O(log size).
- Every state change is one record, and every record is independently
  applicable. Preserve that: it is what makes the log replayable, and it is
  the property the Raft path in README Q&A #3 depends on.

---

## 4. Code Style

### 4.1 Error handling

- Standard idiomatic Go: return `error` as the last value; check every error.
  No `_ =` discards for errors that can matter - that includes `Sync()`,
  `Close()` on a writable file in the normal path, and `json.Marshal` of user
  input.
- The sanctioned exception is **cleanup on a path that is already returning a
  different error**, where the discard is the deliberate choice not to mask the
  real cause. `NewQueueService` does this twice (`_ = wal.Close()` when
  construction fails after the log was opened), as does the checkout worker's
  final `tabwriter` flush. Anywhere else, handle it. A discard outside this exception
  needs a comment justifying itself, or it is a defect.
- Wrap with context using `fmt.Errorf("appending enqueue record: %w", err)`.
  Wrap where you can add information; don't re-wrap the same error at every
  level.
- Define sentinel errors for conditions callers branch on and match them with
  `errors.Is` / `errors.As`, never by string comparison. The existing
  sentinels are `service.ErrClosed`, `storage.ErrClosed`, and
  `storage.ErrEmptyID`.
- Map errors to HTTP status codes only in the HTTP layer. Inner packages know
  nothing about HTTP.

### 4.2 No panic in production code

- `panic` is forbidden in all non-test code paths reachable at runtime.
  Invalid input returns an error; impossible states return an error too -
  better a `500` than a crashed process holding a queue.
- HTTP handlers are wrapped in a recovery middleware as a last-resort safety
  net. Its triggering is a bug to fix, not a feature to rely on.

### 4.3 General

- `gofmt` formatting is mandatory.
- Exported identifiers have doc comments; every package has a package comment.
- Small interfaces, defined at the point of use: `pkg/api` declares the
  `Queue` interface describing what the handlers need, and `*service.QueueService`
  satisfies it. Accept interfaces, return concrete types.
- No global mutable state. Dependencies are passed explicitly through
  constructors - `service.NewQueueService(walPath, mode, opts...)`.
- Do not add a third-party dependency. "Zero dependencies, one binary, one
  file on disk" is the product, not an incidental property of the build; a new
  entry in `go.mod` needs a much stronger justification than convenience. The
  web console follows the same rule: it is a single hand-written HTML file
  embedded with `go:embed`, with no build step and no package manager.
