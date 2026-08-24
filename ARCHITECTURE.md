# Queuemaxxing — Architecture

Queuemaxxing is a high-concurrency, durable HTTP message queue written in Go
with **zero external database dependencies**. All state lives in process
memory, backed by a custom append-only Write-Ahead Log (WAL) on local disk
that enables full crash recovery.

---

## 1. High-Level System Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                          HTTP Layer                              │
│  net/http handlers: POST /enqueue, POST /dequeue, POST /ack,     │
│  GET /stats, GET /healthz                                        │
│  Responsibilities: routing, JSON (de)serialization, request      │
│  validation, HTTP status mapping. NO business logic.             │
└───────────────────────────────┬──────────────────────────────────┘
                                │ typed requests (model.EnqueueRequest, ...)
┌───────────────────────────────▼──────────────────────────────────┐
│                     Service / Lock Layer                         │
│  QueueService: the single concurrency choke point.               │
│  - Owns sync.Mutex / sync.RWMutex guarding queue state           │
│  - Assigns monotonic sequence IDs and message IDs                │
│  - Orchestrates the WAL-first write protocol:                    │
│      1. append record to WAL  ->  2. fsync  ->  3. mutate memory │
│  - Enforces ordering mode (FIFO/LIFO), priority, delay           │
└───────────────────────────────┬──────────────────────────────────┘
                                │ guarded mutations
┌───────────────────────────────▼──────────────────────────────────┐
│                   In-Memory Queue State                          │
│  pkg/engine FrankensteinQueue (holds all PENDING messages):      │
│  - ready:    priority-ordered heap of AVAILABLE messages         │
│  - delayed:  min-heap keyed on AvailableAt for future messages   │
│  - byID:     map[string]*item for O(1) lookup and Remove         │
│  Service-owned state:                                            │
│  - inflight: map[string]*model.Message   (dequeued, awaiting ACK)│
│  - nextSeq:  uint64 monotonic sequence counter                   │
└───────────────────────────────┬──────────────────────────────────┘
                                │ append / replay
┌───────────────────────────────▼──────────────────────────────────┐
│                    WAL Storage Engine                            │
│  - Append-only JSONL log file (wal.log)                          │
│  - file.Sync() after every append (durability before ack)        │
│  - Replay on startup rebuilds the exact in-memory state          │
│  - Idempotent replay: duplicate/late records are safe no-ops     │
└──────────────────────────────────────────────────────────────────┘
```

### Request flow: Enqueue

1. HTTP layer decodes and validates `EnqueueRequest`.
2. Service acquires the queue mutex, assigns `ID`, `Seq`, `EnqueuedAt`,
   computes `AvailableAt = EnqueuedAt + DelaySeconds`.
3. Service appends an `ENQUEUE` record to the WAL and calls `file.Sync()`.
   **Only after the sync succeeds** is the message inserted into memory
   (into `delayed` if `AvailableAt > now`, otherwise into `ready`).
4. Mutex released; HTTP layer returns `201 Created` with the message ID.

### Request flow: Dequeue

1. Service acquires the mutex, promotes any `delayed` messages whose
   `AvailableAt <= now` into `ready`.
2. Selects the winner per the scheduling algorithm (see §4).
3. Appends a `DEQUEUE` record to the WAL, syncs, then moves the message
   from `ready` to `inflight` and sets `Status = INFLIGHT`.
4. Returns the message payload plus its ID (used later for ACK).
5. If nothing is available, returns `204 No Content` (non-blocking) —
   consumers poll; the server never parks a goroutine holding the lock.

### Request flow: ACK

1. Service acquires the mutex, verifies the ID exists in `inflight`.
2. Appends an `ACK` record, syncs, then deletes the message from both
   `inflight` and `messages`. The message is now permanently consumed.

---

## 2. Data Models

### `Message`

| Field        | Type        | Description                                                    |
|--------------|-------------|----------------------------------------------------------------|
| `ID`         | `string`    | Globally unique identifier (UUIDv4 or `seq`-derived).          |
| `Seq`        | `uint64`    | Monotonic sequence number assigned at enqueue; total order.    |
| `Payload`    | `string`    | Opaque message body supplied by the producer.                  |
| `Priority`   | `int`       | Higher integers dequeue first. Default `0`.                    |
| `EnqueuedAt` | `time.Time` | Server-assigned timestamp when the enqueue was accepted.       |
| `AvailableAt`| `time.Time` | Message is invisible to consumers until `now >= AvailableAt`.  |
| `Status`     | `Status`    | Lifecycle state: `PENDING`, `INFLIGHT`, `ACKED`.               |

Lifecycle:

```
ENQUEUE                    DEQUEUE                 ACK
   │                          │                     │
   ▼                          ▼                     ▼
PENDING ──(AvailableAt<=now, selected)──> INFLIGHT ──> ACKED (deleted)
   ▲
   └── delayed messages stay PENDING but are held in the `delayed` heap
```

### `QueueMode`

```go
type QueueMode string

const (
    ModeFIFO QueueMode = "FIFO" // ties broken by smallest Seq / EnqueuedAt
    ModeLIFO QueueMode = "LIFO" // ties broken by largest  Seq / EnqueuedAt
)
```

The mode is fixed per queue at construction time and recorded in the WAL
header so recovery uses the same ordering semantics.

### `EnqueueRequest` / `DequeueResponse`

`EnqueueRequest` carries `Payload`, optional `Priority`, and optional
`DelaySeconds`. `DequeueResponse` returns the full message view the consumer
needs to process and later ACK. See `pkg/model/message.go` for the canonical
struct definitions and JSON tags.

---

## 3. Log Record Specification (WAL)

**Format: JSONL** — one JSON object per line, newline-terminated, UTF-8.
JSONL is chosen over binary for debuggability (`cat`/`jq` the log), trivial
forward-compatibility (unknown fields are ignored), and because encoding cost
is dwarfed by the mandatory `fsync` per record.

Every record shares an envelope:

```json
{"v":1,"op":"<OP>","seq":<uint64>,"ts":"<RFC3339Nano>", ...op-specific fields}
```

| Field | Type   | Meaning                                            |
|-------|--------|----------------------------------------------------|
| `v`   | int    | Record schema version (currently `1`).             |
| `op`  | string | `ENQUEUE`, `DEQUEUE`, or `ACK`.                    |
| `seq` | uint64 | WAL sequence number, strictly increasing per file. |
| `ts`  | string | Wall-clock time the record was written.            |

### `ENQUEUE` record

```json
{"v":1,"op":"ENQUEUE","seq":42,"ts":"2026-08-23T21:00:00.123456789Z",
 "id":"m-000042","payload":"hello","priority":5,
 "enqueued_at":"2026-08-23T21:00:00.123456789Z",
 "available_at":"2026-08-23T21:05:00.123456789Z"}
```

### `DEQUEUE` record

```json
{"v":1,"op":"DEQUEUE","seq":43,"ts":"2026-08-23T21:05:01.5Z","id":"m-000042"}
```

### `ACK` record

```json
{"v":1,"op":"ACK","seq":44,"ts":"2026-08-23T21:05:02.9Z","id":"m-000042"}
```

### Durability & recovery rules

- **WAL-first invariant:** a record is appended and `file.Sync()`ed *before*
  the corresponding in-memory mutation and before the HTTP response.
- **Replay on startup:** scan the log top-to-bottom and apply each record:
  - `ENQUEUE` → insert message as `PENDING`.
  - `DEQUEUE` → move to `INFLIGHT`. Recovery policy: in-flight messages with
    no subsequent `ACK` are **requeued as `PENDING`** (at-least-once
    delivery).
  - `ACK` → delete the message.
- **Idempotent replay:** applying a record whose effect is already present
  (duplicate `ENQUEUE` of a known ID, `ACK` of an already-deleted ID) is a
  no-op, never an error.
- **Torn writes:** a final partial line (crash mid-append) fails JSON
  parsing and is truncated/ignored; every prior line is intact because each
  append is a single `write` followed by `Sync`.
- **Compaction (future):** rewrite the log keeping only live (`PENDING` /
  `INFLIGHT`) messages, then atomically `rename(2)` over the old file.

---

## 4. In-Memory Scheduling & Tie-Breaking

`Dequeue` selects exactly one message using this algorithm, executed under
the queue mutex:

1. **Visibility filter.** Exclude every message where `AvailableAt > now`.
   Implementation: delayed messages live in a min-heap ordered by
   `AvailableAt`; at the top of each dequeue, pop all heap entries with
   `AvailableAt <= now` and promote them into the ready index. Messages in
   `INFLIGHT` status are never candidates.
2. **Priority selection.** Among visible `PENDING` messages, restrict to the
   group with the **highest `Priority`** (larger integer wins).
3. **Tie-breaking within the priority group:**
   - **FIFO:** pick the message with the **smallest** `Seq`
     (equivalently, earliest `EnqueuedAt` — `Seq` is authoritative because it
     is unique even when timestamps collide).
   - **LIFO:** pick the message with the **largest** `Seq`
     (latest `EnqueuedAt`).

### Ready-index data structure

The ready set is a single binary heap (`container/heap`) whose comparator
encodes both rules:

```
less(a, b):
    if a.Priority != b.Priority:  return a.Priority > b.Priority   # max-priority first
    if mode == FIFO:              return a.Seq < b.Seq             # oldest first
    else (LIFO):                  return a.Seq > b.Seq             # newest first
```

This yields `O(log n)` enqueue/promote/dequeue and `O(1)` peek. Because
`Seq` is a strictly monotonic `uint64` assigned under the mutex, ordering is
total and deterministic — two messages can never compare equal. As a
defensive measure the engine also carries an internal arrival counter per
item and falls back to it (in the same mode-appropriate direction) if two
messages ever share a `Seq`, so ordering stays total even under caller
misuse.

Both heaps and the ID index are implemented by `pkg/engine`'s
`FrankensteinQueue`, a self-contained, internally thread-safe structure with
no knowledge of HTTP, the WAL, or message status. Its time-dependent methods
(`Pop`, `Peek`, `AvailableLen`) take `now` explicitly, and callers must pass
non-decreasing `now` values across calls: promotion out of the delayed heap
is permanent, matching real clocks, and explicit time injection keeps delay
semantics deterministically testable.

### Delay promotion correctness

Promotion happens lazily on each dequeue (and optionally via a background
ticker to keep `stats` accurate). Lazy promotion is sufficient for
correctness: a delayed message can only be returned by a dequeue call, and
every dequeue call promotes first.

---

## 5. Concurrency Model

- The service's `sync.Mutex` guards cross-component invariants (`inflight`,
  `nextSeq`, and the WAL-then-memory write protocol). WAL appends happen
  **while holding the lock** so that log order always equals state-mutation
  order — this is what makes replay deterministic.
- The engine's `FrankensteinQueue` carries its own internal mutex and is a
  strict **leaf lock**: engine methods never call out into other components,
  so it can never participate in a lock cycle, and the structure is safe to
  use (and race-test) standalone.
- Handlers are non-blocking: no handler ever waits for a message to become
  available while holding the lock; empty dequeues return immediately.
- Lock ordering, sync semantics, and race-testing requirements are codified
  in `SKILLS.md` and are mandatory for all contributions.

## 6. Package Layout

```
queuemaxxing/
├── go.mod
├── ARCHITECTURE.md
├── SKILLS.md
├── cmd/
│   └── queuemaxxing/        # main: flag parsing, wiring, HTTP server
├── pkg/
│   ├── model/               # core structs (Message, QueueMode, DTOs)     ← exists
│   ├── engine/              # FrankensteinQueue: in-memory heaps/scheduling ← exists
│   ├── storage/             # WAL: append-only JSONL log, crash recovery  ← exists
│   ├── testutil/            # shared TestMain green ✅ / red ❌ reporter
│   ├── queue/               # QueueService: lock layer, WAL-first orchestration
│   ├── wal/                 # append-only log: writer, replayer, records
│   └── httpapi/             # HTTP handlers and routing
```

---

## 7. Test Output Format

Package tests do not use Go's default `--- PASS:` / `--- FAIL:` listing.
Each test package's `TestMain` calls `testutil.Run` (`pkg/testutil`), which
rewrites the stream so humans and review logs can scan results:

| Result | Mark | Color          |
|--------|------|----------------|
| pass   | ✅    | green (`\033[32m`) |
| fail   | ❌    | red (`\033[31m`)   |

```
✅ WriteEnqueue_AppendAndReadBack (0.00s)
❌ Recover_CorruptMiddleLineIsError (0.00s)
    recovered IDs = [m-1], want [m-1 m-3 m-5]

============================
Results: 7/8 passing, 1/8 failing
============================
```

This is presentation only. Correctness still requires `go test -v ./...` and
`go test -race -v ./...` as defined in `SKILLS.md` §1. Failure details from
`t.Errorf` / `t.Fatalf` are forwarded uncolored. New test packages must
install the same `TestMain`; do not invent a second formatter.
