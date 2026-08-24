# Queuemaxxing - Architecture

Queuemaxxing is a durable, concurrent HTTP message queue written in Go with
**zero external dependencies**. All queue state lives in process memory,
backed by a custom append-only Write-Ahead Log (WAL) on local disk that
provides full crash recovery.

This document describes the system **as implemented**. Where a capability is
designed but not built, it says so explicitly and points at the Design Q&A in
[`README.md`](README.md) that works it out. Nothing described here without
such a marker is aspirational.

---

## 1. High-Level System Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                          HTTP Layer  (pkg/api)                   │
│  net/http ServeMux, Go 1.22 method patterns:                     │
│    POST /enqueue      POST|GET /dequeue      GET /messages       │
│    GET /wal           GET /stats             GET /health         │
│    GET /   (embedded web console)                                │
│  Responsibilities: routing, JSON (de)serialization, request      │
│  validation, HTTP status mapping. NO business logic.             │
└───────────────────────────────┬──────────────────────────────────┘
                                │ typed requests (model.EnqueueRequest, …)
┌───────────────────────────────▼──────────────────────────────────┐
│                Service / Lock Layer  (pkg/service)               │
│  QueueService: the single concurrency choke point.               │
│  - Owns a sync.RWMutex guarding all queue mutations              │
│  - Assigns message IDs (UUIDv4) and monotonic sequence numbers   │
│  - Orchestrates the WAL-first write protocol:                    │
│      1. append record  ->  2. fsync  ->  3. mutate memory        │
│  - Owns the injectable clock (WithNow) that dates every message  │
└───────────────────────────────┬──────────────────────────────────┘
                                │ guarded mutations
┌───────────────────────────────▼──────────────────────────────────┐
│              In-Memory Queue State  (pkg/engine)                 │
│  FrankensteinQueue holds every stored message:                   │
│  - ready:    heap ordered by the selection rule (§4)             │
│  - delayed:  min-heap keyed on AvailableAt                       │
│  - byID:     map[string]*item for O(1) lookup and Remove         │
└───────────────────────────────┬──────────────────────────────────┘
                                │ append / replay
┌───────────────────────────────▼──────────────────────────────────┐
│                  WAL Storage Engine  (pkg/storage)               │
│  - Append-only JSONL log file                                    │
│  - file.Sync() after every append (durability before response)   │
│  - Replay on startup rebuilds the exact in-memory state          │
│  - Idempotent replay: duplicate/orphan records are safe no-ops   │
└──────────────────────────────────────────────────────────────────┘
```

### Request flow: Enqueue

1. HTTP layer decodes and validates `EnqueueRequest` (non-empty payload,
   `0 <= delay_seconds <= MaxDelaySeconds`), with the body capped at 1 MiB.
2. Service generates a UUIDv4, takes the write lock, assigns the next `Seq`,
   reads the clock once, normalizes it to UTC, and computes
   `AvailableAt = EnqueuedAt + DelaySeconds`.
3. Service appends an `ENQUEUE` record to the WAL and calls `file.Sync()`.
   **Only after the sync succeeds** is the message inserted into the engine.
   A failed sync rolls the sequence counter back, leaves memory untouched,
   and surfaces as `500`.
4. Lock released; HTTP layer returns `201 Created` with the full message.

### Request flow: Dequeue

1. Service takes the write lock and calls `engine.Pop(now)`, which first
   promotes every delayed message whose `AvailableAt` has passed, then pops
   the winner per §4.
2. If nothing is eligible, the handler returns `204 No Content` immediately.
   Dequeue never blocks waiting for work to arrive.
3. Otherwise a `DEQUEUE` tombstone is appended and synced **before** the
   message is handed to the consumer, so the log never claims a message is
   still queued after it has been delivered. If that sync fails, the message
   is pushed back into the engine and the call returns `500`.
4. The handler returns `200 OK` with the message.

### Delivery contract: at-most-once

Delivery **is** consumption. There is no acknowledgement step: once the
`DEQUEUE` record is durable the message is gone from both memory and the
recovered state, so a consumer that crashes after receiving it has lost that
message.

This is a deliberate v1 boundary, not an oversight. It buys a recovery
invariant that fits in one sentence - *the live set is every `ENQUEUE`
without a matching `DEQUEUE`* - which is what lets the crash tests in
[`test/e2e_recovery_test.go`](test/e2e_recovery_test.go) assert exact survivor
sets rather than approximate ones.

Upgrading to **at-least-once** means adding leases: a dequeue would log
`OP_LEASE {id, deadline}` and hold the message in an invisible in-flight
table until an `OP_ACK` arrived, with an expiry sweeper requeueing whatever
lapsed. That design - including heartbeats for slow consumers and how replay
re-arms unexpired leases - is worked out in README Design Q&A #3. **It is not
implemented.** The `Status` field on `Message` is persisted on every `ENQUEUE`
record (tombstones carry no message fields at all), so that change can land
without a log-format version bump - but today it only ever holds `PENDING`.

---

## 2. Data Models

### `Message`

| Field         | Type        | Description                                                   |
|---------------|-------------|---------------------------------------------------------------|
| `ID`          | `string`    | UUIDv4 assigned by the server at enqueue.                     |
| `Seq`         | `uint64`    | Monotonic message sequence assigned under the service lock.   |
| `Payload`     | `string`    | Opaque message body supplied by the producer.                 |
| `Priority`    | `int`       | Higher integers dequeue first. Default `0`.                   |
| `EnqueuedAt`  | `time.Time` | Server-assigned UTC time the enqueue was accepted.            |
| `AvailableAt` | `time.Time` | Message is invisible to consumers until `now >= AvailableAt`. |
| `Status`      | `Status`    | Always `PENDING` in this release (see §1 delivery contract).  |

Lifecycle as built:

```
ENQUEUE (durable)                    DEQUEUE (durable)
   │                                        │
   ▼                                        ▼
PENDING ──(AvailableAt <= now, selected)──> delivered and deleted
   ▲
   └── delayed messages are also PENDING; they sit in the `delayed` heap
       and are invisible to consumers until they mature
```

`Seq` is the authoritative total order. Timestamps can collide under a coarse
clock; `Seq` is assigned inside the critical section and never does.

### `QueueMode`

```go
type QueueMode string

const (
    ModeFIFO QueueMode = "FIFO" // ties broken by smallest Seq (earliest enqueue)
    ModeLIFO QueueMode = "LIFO" // ties broken by largest  Seq (latest enqueue)
)
```

The mode is fixed per process at construction, from `-mode` / `QUEUE_MODE`.

> **Known limitation.** The mode is *not* recorded in the log. Reopening an
> existing WAL under a different mode is accepted silently and reorders every
> surviving message. Writing a mode marker into the log and refusing a
> mismatched reopen is listed under future work in README Q&A #3.

### DTOs

`EnqueueRequest` carries `Payload`, optional `Priority`, and optional
`DelaySeconds`; `Validate` enforces them at the boundary. `QueueStats`,
`QueueSnapshot`, and `WalTail` are the read-only views behind `GET /stats`,
`GET /messages`, and `GET /wal` respectively.

`POST /enqueue` and `POST /dequeue` return the **full `Message`** - there is
no separate response DTO. See [`pkg/model/message.go`](pkg/model/message.go)
for the canonical definitions and JSON tags.

`MaxDelaySeconds` (ten years) is a correctness bound, not a policy preference.
`AvailableAt` is computed as `now.Add(DelaySeconds * time.Second)`, and
`time.Duration` is an int64 nanosecond count, so a delay past roughly 292
years overflows that multiplication and wraps negative - which would make a
"far future" message instantly visible, the exact opposite of the request.

---

## 3. Log Record Specification (WAL)

**Format: JSONL** - one JSON object per line, newline-terminated, UTF-8.
JSONL is chosen over a binary encoding for debuggability (`cat`/`jq` the log),
trivial forward-compatibility (unknown fields and unknown ops are ignored),
and because encoding cost is dwarfed by the mandatory `fsync` per record.

Every record shares an envelope:

| Field | Type   | Meaning                                                             |
|-------|--------|---------------------------------------------------------------------|
| `v`   | int    | Record schema version (currently `1`).                              |
| `op`  | string | `ENQUEUE` or `DEQUEUE`.                                             |
| `seq` | uint64 | **Record** number: the append counter, strictly increasing per file. |
| `ts`  | string | Wall-clock time the record was written (RFC 3339 nano, UTC).        |

`seq` counts records; `msg_seq` (ENQUEUE only) carries the queue's message
ordering key. The two coincide in a log of pure enqueues and diverge as soon
as a `DEQUEUE` is interleaved - which is exactly why they are separate fields
rather than one field meaning different things depending on `op`. Message
timestamps (`enqueued_at`, `available_at`) and the envelope `ts` are UTC.

### `ENQUEUE` record

```json
{"v":1,"op":"ENQUEUE","seq":5,"ts":"2026-08-24T03:25:11.156Z",
 "id":"76df0f53-9b49-43fe-89c9-fc6e5b4aad6b","msg_seq":4,
 "payload":"Task E","priority":10,
 "enqueued_at":"2026-08-24T03:25:11.156Z",
 "available_at":"2026-08-24T03:26:41.156Z","status":"PENDING"}
```

### `DEQUEUE` record

A tombstone carries only the envelope and the ID. Message-scoped fields are
omitted entirely rather than written as zero values, so the log stays
readable:

```json
{"v":1,"op":"DEQUEUE","seq":6,"ts":"2026-08-24T03:26:50.525Z",
 "id":"76df0f53-9b49-43fe-89c9-fc6e5b4aad6b"}
```

### Durability & recovery rules

- **WAL-first invariant:** a record is appended and `file.Sync()`ed *before*
  the corresponding in-memory mutation and before the HTTP response.
- **Replay on startup** is a pure fold over the records:
  - `ENQUEUE` → insert the message as live. An ID already present is a no-op.
  - `DEQUEUE` → delete the ID. An ID already gone is a no-op.
  - Unrecognized `op` values are skipped, so a newer writer's records do not
    break an older reader.
- **Idempotent replay:** applying the log twice yields the same state as
  applying it once. Asserted directly by `TestRecover_IdempotentReplay`.
- **Torn writes:** the *last* line of the file is the only one allowed to be
  unparseable - that is the signature of a crash caught mid-append - and it is
  discarded. A line that fails to parse *anywhere earlier* is a hard error,
  because it implies bytes were lost rather than never written.

  Note the precise rule: recovery keys off *position*, not shape. It cannot
  distinguish a truncated final record from a complete but corrupt one, and
  discards either. That is the right trade - a writer that fsyncs each record
  before returning can only ever leave damage at the tail - but "torn" here
  means "unparseable and last", not "detected as partial".

  Deciding this needs exactly one line of lookahead, so recovery holds a
  single pending record rather than buffering the file. Peak memory is
  therefore O(live set + longest record) - the live messages must be
  accumulated to be returned - not O(log size), which is what matters because
  the log retains consumed messages forever. A single record is capped at
  16 MiB by the scanner buffer.
- **Recovery output** is the live messages sorted by `msg_seq` (original
  enqueue order), each carrying its original priority and `available_at`.
  Post-recovery ordering is therefore identical to what it would have been
  without the crash. Sorting by `msg_seq` is what lets recovery forget
  consumed IDs instead of retaining every identifier the log has ever seen.

### Reading the log over HTTP (`GET /wal`)

`WAL.Tail` powers `GET /wal`: it returns the newest records verbatim — each
line is already a JSON object, so they pass through untouched — plus the
file's path, size, and whether older records were cut off. Three properties
keep it honest and cheap:

- **Read-only.** Tail never writes. The write path opens the file with
  `O_APPEND`, so Tail's seek cannot affect where the next record lands, and
  it shares the WAL mutex with the writers, so a record is either fully
  visible or not yet visible — never half of one.
- **Bounded.** It reads at most a fixed window (4 MiB) from the end of the
  file, so polling it — the console refreshes this panel continuously — costs
  O(window), not O(history).
- **Tolerant where Recover is strict.** An unparseable line is skipped, not
  an error: the one legitimately broken line in a live log is a torn tail
  from a previous crash, and corruption *policy* (torn tail dropped, mid-file
  corruption fatal) belongs to `Recover` alone.

The endpoint exists for the web console's durability panel: it is what lets a
tester watch the `ENQUEUE` record land on disk before the enqueue's `201`
arrives, and watch tombstones appear as messages are consumed.

> **Known limitation.** The log is never compacted or truncated, so disk usage
> and startup scan time grow with total history rather than with the live set.
> Snapshots plus segment deletion are designed in README Q&A #3 and are **not
> implemented**.

---

## 4. In-Memory Scheduling & Tie-Breaking

`Dequeue` selects exactly one message, under the service lock:

1. **Visibility filter.** Exclude every message where `AvailableAt > now`.
   Implementation: delayed messages live in a min-heap keyed on `AvailableAt`;
   each call that takes a `now` first pops everything matured off that heap
   and pushes it into the ready heap.
2. **Priority selection.** Among visible messages, the **highest `Priority`**
   wins (larger integer first).
3. **Tie-breaking within a priority group:**
   - **FIFO:** smallest `Seq` (earliest enqueue).
   - **LIFO:** largest `Seq` (latest enqueue).

### Ready-index data structure

The ready set is a binary heap (`container/heap`) whose comparator encodes
both rules:

```
less(a, b):
    if a.Priority != b.Priority:  return a.Priority > b.Priority   # higher first
    if mode == FIFO:              return a.Seq < b.Seq             # oldest first
    else (LIFO):                  return a.Seq > b.Seq             # newest first
```

This yields `O(log n)` push and heap pop. Any call that takes `now` first
promotes matured delayed messages, which is `O(k log n)` for the `k`
messages that became visible since the last such call - so `Pop`, `Peek`,
and `AvailableLen` are `O(k log n)` then `O(log n)` / `O(1)`. Reading the
head after promotion is `O(1)`. Because `Seq` is a strictly monotonic
`uint64` assigned under the lock, the order is total and deterministic -
two messages can never compare equal.
As a defensive measure each heap item also carries an internal arrival counter
and falls back to it in the mode-appropriate direction, so ordering stays
total even if a caller ever supplies duplicate `Seq` values.

`FrankensteinQueue` is self-contained and internally locked, with no knowledge
of HTTP, the WAL, or durability. Its time-dependent methods (`Pop`, `Peek`,
`AvailableLen`, `Snapshot`) take `now` explicitly rather than reading a clock,
which is what makes delay semantics deterministically testable. Callers must
pass non-decreasing `now` values: promotion is permanent, matching real clocks.

`Peek` and `Remove` are part of the container's API and are covered by the
engine test suite (`Remove` including under the concurrent hammer test), but
no service-layer caller uses them today - nothing in the HTTP surface cancels
or inspects an individual message.

### Delay promotion correctness

Promotion is lazy: it happens inside whichever call supplies a `now` - `Pop`,
`Peek`, `AvailableLen`, or `Snapshot`. No background ticker is needed for
correctness, because a delayed message can only be *returned* by a call that
promotes first. There is no sweeper goroutine in the implementation.

### Snapshot ordering

`Snapshot(now, limit)` powers `GET /messages` and the web console. It applies
the same promotion step as `Pop`, so the two can never disagree about
visibility, then sorts copies of both heap arrays into presentation order -
ready by the selection rule above, delayed by `AvailableAt`. Empty heaps
encode as JSON arrays (`[]`), not `null`. The sort makes Snapshot `O(n log n)`,
which is why it is an inspection path with a bounded limit rather than
something the hot path uses. After promotion, `Pop` itself is `O(log n)`.

---

## 5. Concurrency Model

- `QueueService.mu` (a `sync.RWMutex`) guards cross-component invariants: the
  sequence counter, the closed flag, and the WAL-then-memory ordering. WAL
  appends happen **while holding the lock**, so log order always equals
  state-mutation order - this is what makes replay deterministic. The cost is
  that `fsync` is serialized; group commit is the planned fix (README Q&A #3).
- `Enqueue`, `Dequeue`, and `Close` take the write lock. `Stats`, `Snapshot`,
  and `WalTail` take the read lock, so concurrent readers never block one
  another and never observe a half-applied write. `WalTail` then takes the
  WAL's own leaf lock to read the file, which respects the canonical order.
- One subtlety worth stating plainly: those read-lock paths still *mutate* the
  engine, because promotion off the delayed heap is lazy and happens inside
  whichever call supplies a `now`. That is safe - the engine has its own mutex,
  so the writes are properly synchronized and `-race` is clean - but it means
  the service read lock guards the service's own fields, not the engine's
  immutability. Promotion is monotonic and idempotent, so concurrent readers
  cannot derive inconsistent views from it. `Snapshot` additionally takes the
  engine lock exactly once, returning its lists and its counts from a single
  observation, so the two can never disagree.
- `FrankensteinQueue` and `WAL` each carry their own mutex and are strict
  **leaf locks**: neither ever calls back into another locked component, so
  they cannot participate in a lock cycle, and each is safe to use and
  race-test standalone.
- Handlers are non-blocking. An empty dequeue returns `204` immediately; no
  handler parks a goroutine waiting for work, with or without a lock held.
- No background goroutines exist in the queue itself - no sweeper, no
  promotion ticker. The only long-lived goroutine is the one `cmd/server`
  starts to run `srv.ListenAndServe`, and it exits when `srv.Shutdown` makes
  that call return.
- **After `Close`, memory reads keep working and everything else fails.**
  `Enqueue` and `Dequeue` return `ErrClosed` → `503`, because durability is no
  longer available; `Stats` and `Snapshot` keep answering from intact memory,
  because they are not claiming anything untrue. `WalTail` also returns
  `ErrClosed`: its data source is the file, and the file handle is gone.
  `Close` is idempotent.
- Lock ordering, sync semantics, and race-testing requirements are codified in
  [`SKILLS.md`](SKILLS.md) and are mandatory for all contributions.

---

## 6. Package Layout

```
queuemaxxing/
├── go.mod                      # module queuemaxxing - no dependencies
├── ARCHITECTURE.md             # this document
├── SKILLS.md                   # engineering & testing conventions
├── README.md                   # usage, HTTP API, and Design Q&A
├── cmd/
│   ├── server/                 # main: flags/env, wiring, HTTP server, shutdown
│   └── client/                 # checkout-dispatch worker that uses the HTTP API
├── pkg/
│   ├── model/                  # Message, QueueMode, DTOs (stdlib only)
│   ├── engine/                 # FrankensteinQueue: ready/delayed heaps
│   ├── storage/                # WAL: append-only JSONL, fsync, replay
│   ├── service/                # QueueService: lock layer, WAL-first protocol
│   ├── api/                    # HTTP handlers, routing, status mapping
│   │   └── ui/console.html     # web console, embedded via go:embed
│   └── testutil/               # shared TestMain reporter (✅/❌ output)
└── test/
    └── e2e_recovery_test.go    # full-stack hard-crash recovery tests
```

Not shown above: each of `pkg/api`, `pkg/engine`, `pkg/service`, `pkg/storage`,
and `test` also contains a small `main_test.go` that wires `TestMain` to
the reporter in §7. `.gitattributes` pins every text file to LF so `gofmt` is
clean on a clone regardless of the checkout platform.

Dependency direction is strictly downward:

```
cmd/server → api → service → {engine, storage} → model
cmd/client → model
```

No package imports anything outside the standard library and this module;
`model` sits at the bottom and imports only the standard library.

---

## 7. Test Output Format

Package tests do not use Go's default `--- PASS:` / `--- FAIL:` listing. Each
test package's `TestMain` calls `testutil.Run`
([`pkg/testutil/run.go`](pkg/testutil/run.go)), which rewrites the stream so
humans and review logs can scan results:

| Result | Mark | Color              |
|--------|------|--------------------|
| pass   | ✅    | green (`\033[32m`) |
| fail   | ❌    | red (`\033[31m`)   |
| skip   | ⏭    | uncolored          |

```
✅ TestWriteEnqueue_AppendAndReadBack (0.00s)
    wal_test.go: Recover: expected error for corrupt middle line, got nil
❌ TestRecover_CorruptMiddleLineIsError (0.00s)

============================
Results: 7/8 passing, 1/8 failing
============================
```

The detail line appears *above* its mark because Go emits `t.Errorf` output
before the `--- FAIL:` line, and the reporter preserves stream order rather
than buffering per test. Failure text keeps its `file:line:` prefix and its
original wording so diffs stay copy-pasteable; the reporter only drops blank
lines and Go's own `ok` / `FAIL` package summaries, which it replaces with the
totals block.

`testutil.Run` appends `-test.v` to the *test binary's* arguments, but that
does not make `go test` print a passing package's output - the `go` driver
discards stdout for packages that pass unless `-v` is passed to `go test`
itself. **Run `go test -v ./...` to see the marks.** With a plain
`go test ./...` you get Go's usual one-line-per-package summary, and the marks
surface only for packages that fail.
