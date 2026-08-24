# Queuemaxxing

A durable, concurrent HTTP message queue in a **single Go binary** with
**zero external dependencies**. Every message is fsynced to an append-only
Write-Ahead Log before the server acknowledges it, so a hard crash - SIGKILL,
power loss, OOM - never loses an accepted message. Scheduling fuses three
behaviors that incumbent queues force you to pick between: **per-message
priority**, **per-message delay**, and **FIFO or LIFO** ordering, fused in
one heap - O(log n) per enqueue or dequeue after delay promotion.

```
producer ──POST /enqueue──▶ ┌──────────────────────────────┐
                            │  HTTP (pkg/api)              │
                            │  QueueService (pkg/service)  │──▶ queue.wal
consumer ◀─POST /dequeue─── │  FrankensteinQueue (engine)  │   (fsync per record)
                            └──────────────────────────────┘
     you ──────GET / ──────▶  web console (embedded; click-to-run scenarios, live heaps,
                                           WAL viewer, predicted drain order)
```

See [`ARCHITECTURE.md`](ARCHITECTURE.md) for the full design and
[`SKILLS.md`](SKILLS.md) for the engineering and testing conventions.

---

## Evaluating this in five minutes

Go 1.22+ is the only prerequisite - no `make`, no npm, no broker, no
database. Four commands, in order:

```bash
go test ./...
```

```bash
go run ./cmd/server
```

Open **<http://localhost:8080>**. The queue serves its own web console. Click
the five **scenarios** at the top left: each states the drain order it
expects *before* draining, then takes messages one at a time explaining every
pick, and ends with a pass/fail check of prediction against reality. That is
the whole feature set - priority, delay, FIFO/LIFO tie-break, and 20-producer
concurrency - proving itself in about ninety seconds.

Then, in a second terminal, run the worker application against it:

```bash
go run ./cmd/client
```

Finally, **prove durability**: press `Ctrl+C` in the server's terminal, start
it again with `go run ./cmd/server`, and reload the console. Every message is
back in the same order with delay timers still honored, rebuilt by replaying
the write-ahead log - which the console's bottom-right panel shows you record
by record.

If you have `make`, `make demo` does the server-plus-worker round trip in one
command against a throwaway queue, and `make help` lists the rest.

**Where to look in the code:** [`pkg/engine/queue.go`](pkg/engine/queue.go)
is the scheduler, [`pkg/storage/wal.go`](pkg/storage/wal.go) is the
durability layer, [`pkg/service/queue_service.go`](pkg/service/queue_service.go)
is the concurrency choke point, and the four
[Design Q&A](#design-qa) answers are at the bottom of this file.

---

## Highlights

- **Zero runtime dependencies.** Standard library only - `go.mod` lists no
  requirements. No broker cluster, no Erlang/JVM, no ZooKeeper, no external
  database. State is one JSONL file.
- **Crash-safe by construction.** WAL-first protocol: *write → fsync → mutate
  memory → respond*. Recovery replays the log; torn trailing writes are
  detected and dropped, mid-file corruption is a hard error.
- **Priority + Delay + FIFO/LIFO in a single queue.** Higher priority wins;
  delayed messages are invisible until `available_at`; ties break by
  monotonic sequence number in the direction the queue mode dictates.
- **Deterministic total order.** A strictly monotonic `seq` assigned under the
  service lock means two messages can never compare equal.
- **Non-blocking API.** Empty dequeues return `204` immediately; no handler
  ever parks a goroutine, with or without a lock held.
- **A web console in the binary.** `GET /` serves a single embedded HTML page
  built for a tester: one-click scenarios that predict the drain order and
  verify it live, both heaps rendered in real time, an explanation for every
  dequeue, and a live view of the write-ahead log itself. No build step, no
  npm, nothing to install.
- **Race-clean.** `go test -race ./...` is part of the definition of done,
  including an end-to-end test that hammers the HTTP API from 18 goroutines,
  simulates two hard crashes by abandoning the service without `Close()`,
  and proves the crash ledger has no losses or duplicates (the delivery
  contract remains at-most-once).

## Quick start

```bash
go run ./cmd/server
```

Then open **<http://localhost:8080>** - the console is served by the queue
process itself. The fastest way to understand the system is to run the five
**scenarios** at the top left of that page: each one enqueues a small scripted
scenario (priority, delay, FIFO/LIFO tie-break, the combined checkout burst,
and a 20-producer concurrency blast), states the predicted drain order, takes
the messages one by one while explaining every pick, and finishes with a
pass/fail check of prediction against reality. Then kill the server and start
it again: the queue comes back exactly as it was, and the console's WAL panel
shows the log that made that possible.

Configuration is by flag or environment variable:

```bash
go run ./cmd/server -port 9090 -mode LIFO -data /var/lib/qm/queue.wal
```

```bash
PORT=9090 QUEUE_MODE=LIFO DATA_PATH=/var/lib/qm/queue.wal go run ./cmd/server
```

| Flag    | Env          | Default            | Meaning                       |
|---------|--------------|--------------------|-------------------------------|
| `-port` | `PORT`       | `8080`             | HTTP listen port              |
| `-mode` | `QUEUE_MODE` | `FIFO`             | Ordering mode: `FIFO`/`LIFO`  |
| `-data` | `DATA_PATH`  | `./data/queue.wal` | WAL file path (dirs created)  |

Talk to it with curl:

```bash
curl -s -X POST localhost:8080/enqueue -d '{"payload":"send-invoice-42","priority":5}'
```

```bash
curl -s -X POST localhost:8080/enqueue -d '{"payload":"retry-webhook","priority":0,"delay_seconds":90}'
```

```bash
curl -s -X POST localhost:8080/dequeue
```

```bash
curl -s localhost:8080/messages
```

```bash
curl -s localhost:8080/wal
```

There is also a checkout-dispatch worker (`cmd/client`). It is the simple
application that *uses* the queue: a storefront burst of five jobs (payment
captures, a receipt email, two webhook retries on backoff), then a worker
loop that dequeues and processes them, narrating why each job won its turn.

```bash
go run ./cmd/client
```

The worker reads the server's ordering mode from `GET /messages` and derives
its expected order by replaying the engine's own three rules over the
server's snapshot. So it is correct wherever you point it: on a FIFO server
it runs `B → D → A → E → C` (money before email, backoff respected, oldest
capture first); on a LIFO server the tie-break flips and it runs
`D → B → A → E → C`, and reports that as a pass. If the queue already holds
other traffic, the worker says so, scores only the jobs it enqueued, and
folds the existing backlog into its prediction. **A `MISMATCH` from this
worker means the engine genuinely disagreed with its own rules** - it is a
real assertion, not a hardcoded expectation.

It targets `http://127.0.0.1:8080` by default; point it elsewhere with `-url`
or the `QUEUE_URL` environment variable. Output is colorized only when stdout
is a terminal; `-color never` or `NO_COLOR=1` forces plain text.

## HTTP API

| Method & path    | Body                                     | Success                        | Notes |
|------------------|------------------------------------------|--------------------------------|-------|
| `POST /enqueue`  | `{"payload","priority","delay_seconds"}` | `201` + full message JSON      | `payload` required; `delay_seconds` in `[0, 315360000]`; body capped at 1 MiB |
| `POST /dequeue`  | -                                        | `200` + message, or `204`      | `GET` also accepted; destructive - delivery *is* consumption (see below) |
| `GET /messages`  | -                                        | `200` + queue snapshot         | Read-only; `?limit=` in `[1,1000]`, default `100` |
| `GET /wal`       | -                                        | `200` + WAL tail               | Read-only; newest log records verbatim from disk; `?limit=` in `[1,500]`, default `50` |
| `GET /stats`     | -                                        | `200` `{total,ready,delayed}`  | Point-in-time counts |
| `GET /health`    | -                                        | `200` `{"status":"healthy"}`   | Liveness probe |
| `GET /`          | -                                        | `200` `text/html`              | The embedded web console |

Handler-generated errors are JSON (`{"error":"..."}`): `400` malformed or
invalid request, `413` oversized body, `500` durability failure (the WAL could
not be synced - the operation did **not** happen), `503` service closed.
Routing errors come from `net/http` itself and are plain text: `404` for an
unknown path, `405` with an `Allow` header for a known path and wrong method.

A message object looks like:

```json
{
  "id": "76df0f53-9b49-43fe-89c9-fc6e5b4aad6b",
  "seq": 5,
  "payload": "send-invoice-42",
  "priority": 5,
  "enqueued_at": "2026-08-23T21:00:00.123456789Z",
  "available_at": "2026-08-23T21:01:30.123456789Z",
  "status": "PENDING"
}
```

`GET /messages` returns both heaps in presentation order - `ready[0]` is
exactly what the next dequeue will return - alongside the counts and the
server's own clock (UTC), so a client can render delay countdowns without
trusting its local time. Empty heaps are JSON arrays (`[]`), not `null`:

```json
{
  "mode": "FIFO",
  "now": "2026-08-23T21:00:05.512Z",
  "stats": { "total": 4, "ready": 2, "delayed": 2 },
  "ready":   [ { "seq": 4, "priority": 5, "payload": "…" } ],
  "delayed": [ { "seq": 5, "priority": 10, "available_at": "…" } ],
  "ready_truncated": false,
  "delayed_truncated": false
}
```

`GET /wal` is the same idea for the durability layer: it returns the newest
log records verbatim (the log is JSONL, so each record is already a JSON
object), plus the file's path and size, without ever mutating the log. It
reads a bounded window from the end of the file, so its cost does not grow
with history. This is what the console's "on disk" panel polls:

```json
{
  "path": "data/queue.wal",
  "size_bytes": 1968,
  "truncated": false,
  "records": [
    { "v": 1, "op": "ENQUEUE", "seq": 1, "id": "…", "msg_seq": 1, "payload": "…", "priority": 5, "...": "…" },
    { "v": 1, "op": "DEQUEUE", "seq": 2, "id": "…", "ts": "…" }
  ]
}
```

### Delivery contract: at-most-once

Delivery is consumption. There is no `/ack`: once `POST /dequeue` returns
`200`, the message's tombstone is already durable and the message is gone. A
consumer that crashes after receiving a message has lost it.

This is a deliberate v1 boundary. It keeps the recovery invariant to one
sentence - *the live set is every `ENQUEUE` without a matching `DEQUEUE`* -
which is what lets the crash tests assert exact survivor sets. The lease-based
upgrade to at-least-once is designed in [Q&A #3](#3-if-you-had-more-time-what-other-features-would-you-add)
and is not implemented.

## The web console

`GET /` serves a single HTML page compiled into the binary with `go:embed`.
Keeping it inside the executable is the point: a console that needed a
separate asset directory, an npm install, or a build step would undo the
"one binary, one file on disk" property that the rest of the design exists to
protect. It calls only the public HTTP API above - nothing it shows is
privileged, so everything it demonstrates can be reproduced with curl.

The page is built so a tester can understand the whole system without reading
code:

- **The three rules, stated up front.** A card spells out the selection rule
  the engine applies - (1) delayed messages are invisible until due, (2)
  highest priority wins, (3) ties break by the server's FIFO/LIFO mode - and
  every event the page logs refers back to those rules by number.
- **Click-to-run scenarios.** Five numbered scenarios sit at the top of the
  page, all set in the same online shop, so a tester meets one cast of jobs
  rather than five unrelated worlds:

  | # | Scenario | What it proves |
  |---|----------|----------------|
  | 1 | Payments jump the line | priority beats arrival order |
  | 2 | The backoff timer | a delayed job is invisible even at priority 9 |
  | 3 | Three identical refunds | the FIFO/LIFO tie-break decides |
  | 4 | Black Friday checkout | all three rules at once - the same five jobs `cmd/client` runs |
  | 5 | Twenty storefronts at once | 20 parallel enqueues, every one sequenced and durable |

  Each one enqueues its script, **predicts the drain order** from the server's
  own snapshot, then dequeues step by step with an explanation for every pick,
  and ends with a pass/fail comparison of prediction against what actually
  came out. The prediction models the pace the scenario drains at, because a
  backoff matures on the wall clock - a consumer that pauses between takes can
  legitimately reach a different order than one draining flat out.
- **"Up next" strip.** The exact order the queue will drain in - ready
  messages plus delayed ones with countdown badges - computed client-side
  with the same comparator the engine uses. A **what-if toggle** re-runs the
  simulation under the opposite ordering mode, so FIFO vs LIFO can be
  compared without restarting the server.
- **Live heaps.** Ready in dequeue order with the next message marked;
  Waiting with per-message countdowns. The page polls `GET /messages`, which
  never consumes anything, so watching the queue does not drain it. When a
  timer expires the activity log notes the message moving Waiting → Ready.
- **The WAL, live.** A panel polls `GET /wal` and renders the newest records
  of the on-disk log - every `ENQUEUE` and `DEQUEUE` tombstone, with a
  raw-JSONL toggle - under the reminder that each line was fsynced before its
  HTTP response was sent. Paired with the crash-test recipe on the page
  (kill the server, restart, reload), it makes the durability requirement
  visible instead of asserted.

## How it works

**Write path.** `QueueService` (the single concurrency choke point) assigns
`id` + monotonic `seq`, appends an `ENQUEUE` record to the WAL, calls
`file.Sync()`, and only then inserts the message into the in-memory engine and
answers `201`. A failed sync is a hard `500` and memory is untouched.

**Scheduling.** The engine (`FrankensteinQueue`) keeps two binary heaps: a
*delayed* min-heap keyed on `available_at` and a *ready* heap ordered by the
selection rule. Each dequeue first promotes every matured delayed message,
then pops the ready heap:

```
less(a, b):
    if a.priority != b.priority:  a.priority > b.priority   # higher first
    if mode == FIFO:              a.seq < b.seq             # oldest first
    else:                         a.seq > b.seq             # newest first
```

**Dequeue path.** The winning message's `DEQUEUE` record is appended and
synced *before* the message is handed to the consumer, so the log always
agrees with what left the queue.

**Recovery.** On startup the WAL is replayed as a pure, idempotent fold:
`ENQUEUE` inserts (duplicate IDs are no-ops), `DEQUEUE` deletes (missing IDs
are no-ops), unknown ops are skipped. The **last** line of the file is the only
one allowed to be unparseable - that is the signature of a crash caught
mid-append - and it is dropped; a line that fails to parse anywhere earlier is
a hard error, because it implies bytes were lost rather than never written.
(The rule keys off position, not shape: a complete-but-corrupt final record is
discarded just like a truncated one. Since every record is fsynced before the
call returns, damage can only ever land at the tail.) Deciding this takes one
line of lookahead, so recovery holds a single pending record rather than
buffering the file. What remains is exactly the set of
accepted-but-undelivered messages, sorted by `msg_seq` and re-inserted with
their original sequence, priority, and `available_at`, so post-recovery
ordering is identical to what it would have been without the crash.

**The log is readable.** It is JSONL, so `jq` works on it directly:

```bash
jq -r 'select(.op=="ENQUEUE") | "\(.msg_seq)\tp\(.priority)\t\(.payload)"' data/queue.wal
```

While the server is running, the same records are one HTTP call away:
`GET /wal` returns the newest ones verbatim, which is how the console's
durability panel works.

## Project layout

```
queuemaxxing/
├── go.mod                  # module queuemaxxing - no requires, no go.sum
├── README.md               # this file
├── ARCHITECTURE.md         # full system design
├── SKILLS.md               # engineering & testing conventions
├── Makefile                # optional convenience targets (make help)
├── .gitattributes          # pins text files to LF so gofmt is clean on every OS
├── .gitignore
├── cmd/
│   ├── server/             # HTTP server binary
│   └── client/             # checkout-dispatch worker (uses the HTTP API)
├── pkg/
│   ├── model/              # Message, QueueMode, DTOs (stdlib only)
│   ├── engine/             # FrankensteinQueue: ready/delayed heaps
│   ├── storage/            # WAL: append-only JSONL, fsync, replay
│   ├── service/            # QueueService: lock layer, WAL-first protocol
│   ├── api/                # HTTP handlers, routing, status mapping
│   │   └── ui/console.html # web console, embedded via go:embed
│   └── testutil/           # shared TestMain reporter (✅/❌ output)
└── test/
    └── e2e_recovery_test.go  # end-to-end hard-crash recovery tests
```

Each of `pkg/api`, `pkg/engine`, `pkg/service`, `pkg/storage`, and `test` also
holds a small `main_test.go` wiring `TestMain` to the reporter.

## Testing

```bash
gofmt -l .              # must print nothing
```

```bash
go vet ./...            # must report nothing
```

```bash
go test -v ./...        # colored ✅/❌ reporter via pkg/testutil
```

`-v` is what surfaces the marks. `testutil.Run` appends `-test.v` to the test
*binary*, but the `go test` driver discards a passing package's output unless
`-v` is given to `go test` itself - so a plain `go test ./...` prints the usual
one-line package summaries and shows marks only for packages that fail.

```bash
go test -race ./...     # non-negotiable; concurrency tests target this
```

`-race` requires cgo. On Linux and macOS that works out of the box; on Windows
it needs a C toolchain such as MinGW-w64 on `PATH` and `CGO_ENABLED=1`, or Go
refuses with `-race requires cgo`.

The end-to-end crash test ([`test/e2e_recovery_test.go`](test/e2e_recovery_test.go))
runs the full stack over real HTTP sockets and a real WAL on disk,
table-driven over FIFO and LIFO:

1. enqueues **100** messages with mixed priorities (0-9) and delays (60 at 0s,
   20 at 60s, 20 at 120s),
2. dequeues **30** and verifies even those came out in perfect order,
3. simulates a **hard crash**: the service is abandoned without `Close()` and
   a torn half-record is appended to the WAL, exactly what a process killed
   mid-append leaves behind,
4. re-initializes a **brand-new `QueueService` on the same WAL file**,
5. verifies **exactly 70** messages survived and drain in perfect
   priority/delay order as an injected clock steps across each delay boundary,
   including "still invisible one second early" checks.

A second e2e test hammers the API with 10 producers and 8 consumers, abandons
the service mid-workload without `Close()` (twice), and proves the ledger
balances: no message is lost or duplicated across the crashes, and far-future
delayed messages survive both. The product contract is still at-most-once -
a consumer that crashes after `200` has lost that message. Delay semantics are
tested with an injected clock
(`service.WithNow`) - never `time.Sleep` (SKILLS.md §1.4).

## Known limitations

Stated plainly, because each one is a deliberate v1 boundary rather than an
oversight, and each has a worked-out path forward in the Q&A below:

- **At-most-once delivery.** No ACK, no visibility timeout. → Q&A #3.
- **Single node.** No replication, no failover. → Q&A #3.
- **The log never compacts.** Disk usage and startup scan time grow with total
  history rather than with the live set. → Q&A #3.
- **Throughput is bounded by serialized `fsync`.** One `fsync` per record,
  under the service lock. Group commit is the fix. → Q&A #3.
- **The queue mode is not recorded in the log.** Reopening an existing WAL
  with a different `-mode` is accepted silently and reorders every surviving
  message. → Q&A #3.
- **No auth, no TLS.** Bind it to localhost or put it behind something.

---

# Design Q&A

## 1. How do you handle replay messages?

**What the system does today: destructive dequeues over an append-only log.**
The WAL itself is immutable - records are only ever appended
(`O_APPEND|O_CREATE`, one mutex-guarded writer, never edited in place) - but
*consumption is logically destructive*. `POST /dequeue` durably appends a
`DEQUEUE` tombstone before the message is handed out, and recovery
(`storage.WAL.Recover`) is a pure fold: `ENQUEUE` inserts, `DEQUEUE` deletes.
A consumed message is therefore gone forever; "replay" today means **state
reconstruction after a crash**, not redelivery of history. That replay is
idempotent (duplicate `ENQUEUE`s and orphan `DEQUEUE`s are no-ops), so
replaying the log twice produces the same state as replaying it once, and a
torn trailing record is dropped while mid-file corruption is a hard error.

**How I would evolve it to support true message replay** (redelivering
already-consumed history to a consumer):

- **Stop deleting; move a cursor instead.** Keep the log exactly as
  append-only as it is now, but reinterpret consumption: instead of a
  `DEQUEUE` tombstone meaning "this message no longer exists," an
  `OP_ACK {group, seq}` record means "consumer group *g* is done with message
  *seq*." The message body stays in the log. Replay then costs nothing to
  implement: it is just rewinding a cursor, because the data was never
  destroyed.

- **WAL offset-based indexing.** Replay from an arbitrary point requires
  random access into the log. Each record already carries a strictly
  increasing `seq`; the missing piece is a **sparse index** mapping
  `seq → byte offset` (one entry every N records or every 4 KiB block,
  maintained in memory and checkpointed to a side file). Locating "message
  seq S" becomes a binary search over the sparse index plus a short forward
  scan - O(log n + K) instead of a full-file scan. Because records are
  appended in timestamp order, the same index supports "replay from timestamp
  T" via binary search on `ts`. Segmenting the log (e.g. 64 MiB chunks) keeps
  each index small and makes retention cheap: drop whole segments.

- **Consumer group cursors.** Each named consumer group gets durable position
  state: a **low-watermark cursor** (`everything ≤ W is consumed`) plus an
  **exception set** of seqs acked above the watermark. The exception set is
  required specifically because this queue delivers in *priority* order, not
  log order - a high-priority late message is consumed before an older
  low-priority one, so acks arrive out of log order and the watermark can only
  advance when the holes below it fill. Cursor updates are themselves WAL
  records (`OP_ACK`, periodically compacted into an
  `OP_CURSOR {group, watermark, exceptions}` checkpoint), so group positions
  survive crashes exactly like messages do. Replay for one group is then an
  administrative record - `OP_SEEK {group, to: seq|timestamp}` - that rewinds
  that group's cursor without touching any other group.

- **Trade-offs, honestly.** Destructive dequeues (the current design) keep
  recovery state minimal - live set only - and reclaim space naturally via
  compaction; but they can never redeliver, audit, or fan out. An immutable
  log with cursors buys replay, multiple independent consumers, and
  debuggability (`jq` over history), at the cost of retention policy becoming
  a real decision (time/size-based, or "min cursor across groups"), storage
  growing until retention triggers, and recovery having to scan retained
  history rather than just the live set - which is why this feature pairs with
  snapshots and compaction (Q&A #3). The v1 choice of destructive dequeues was
  deliberate: it makes the recovery invariant trivial to state and to test.
  The migration path above is incremental because the log format already
  tolerates unknown ops - old logs replay fine under new code, and new logs
  replay under old code minus the features they encode.

## 2. How would you refactor your queue into a Pub/Sub?

Today Queuemaxxing is point-to-point: one implicit queue, and a dequeue
consumes the message for everyone. Pub/Sub decouples *what happened* (a
topic's stream of events) from *who needs it* (each subscription's own
delivery state). The refactor keeps every existing layer and slots two new
concepts between them:

- **Topics.** A named, immutable, append-only message log - which is exactly
  what the WAL already is, plus a `topic` field on each record and a registry
  (`map[string]*Topic` under an RWMutex, mirrored by durable
  `OP_CREATE_TOPIC` records). Publishing appends to the topic's log:
  `POST /topics/{topic}/publish`.

- **Individual consumer subscription queues.** Each subscription (consumer
  group) on a topic owns its **own `FrankensteinQueue` instance** - its own
  ready/delayed heaps, its own cursor, its own pending set. This is the key
  structural move: the engine is already a self-contained, internally locked,
  HTTP-unaware data structure, so instantiating one per subscription costs
  nothing architecturally and buys each subscriber *independent*
  priority/delay/FIFO-or-LIFO semantics and independent pacing. A slow
  subscriber backs up only its own queue. Subscriptions are durable
  (`OP_SUBSCRIBE {topic, group, from: earliest|latest|seq}`) and recovery
  rebuilds each one's pending view from the topic log minus its acked set.
  API sketch: `POST /subscriptions {topic, name, from}`, then
  `POST /subscriptions/{name}/dequeue` and `/ack` - the current endpoints
  become sugar for a default topic with a single default subscription, which
  is the backward-compatibility story.

- **Fan-out routing.** On publish, the router makes the message visible to
  every subscription of the topic. Two strategies with very different cost
  profiles:
  - *Eager cloning:* insert a full copy into every subscription queue at
    publish time. Simple, great isolation, no indirection at delivery - but
    O(S) write and memory amplification per message across S subscriptions,
    and a subscription created later cannot backfill history.
  - *Lazy referencing:* store the payload **once** in the topic log; each
    subscription queue holds only a lightweight reference - `{seq, priority,
    available_at}`, a few dozen bytes - and the body is fetched by seq through
    the sparse offset index (Q&A #1) at delivery time. Storage stays O(1) per
    message regardless of fan-out, and `from: earliest` subscriptions work for
    free.

- **Message cloning vs. referencing, resolved.** The right split is
  *immutable shared body, per-subscription mutable envelope*. The payload is
  written once and never mutated (safe to share by reference, refcounted).
  Everything a subscription needs to mutate - delivery attempts, lease
  deadline, per-subscriber delay overrides, status - lives in that
  subscription's small envelope, which is what actually sits in its
  `FrankensteinQueue`. Garbage collection falls out naturally: a message's
  storage is reclaimable once the minimum cursor across all subscriptions has
  passed it (and retention allows), which composes with segment-based
  compaction. The slow-consumer pathology - one dead subscription pinning the
  whole log - is bounded by retention policy plus dead-lettering (Q&A #3).

Concurrency-wise this preserves the house lock discipline (SKILLS.md §2.2):
the topic registry lock orders before per-subscription engine locks, engine
and WAL stay leaf locks, and publish touches subscription queues one at a
time - no lock cycles.

## 3. If you had more time, what other features would you add?

In priority order, because each one unlocks the next:

- **Consumer ACKs and visibility timeouts with heartbeat leases.** Today a
  dequeue is destructive at delivery time, which makes the post-delivery
  contract *at-most-once*: if the consumer crashes after `200 OK`, that
  message is gone. The fix is to make dequeue grant a **lease**:
  `OP_LEASE {id, consumer, deadline = now + visibility_timeout}` is logged,
  the message moves to an in-memory inflight table keyed by a min-heap of
  lease deadlines, and it is invisible to other consumers while leased. The
  consumer must `POST /ack {id}` (an `OP_ACK` record - the true point of
  consumption) before the deadline. Long-running jobs send heartbeats
  (`POST /heartbeat {id}` → `OP_EXTEND {id, new_deadline}`): a healthy-but-slow
  consumer keeps its lease alive indefinitely, while a dead consumer's lease
  lapses and an expiry scanner (a ticker goroutine with a proper shutdown
  path, per SKILLS.md §2.3) requeues the message with `attempts + 1`. Crash
  recovery composes cleanly: replay re-arms unexpired leases and immediately
  requeues expired ones - upgrading the whole system to genuine at-least-once
  delivery.

- **Dead Letter Queues (DLQ).** With attempts tracked by the lease machinery,
  poison pills become detectable: when `attempts` exceeds a per-queue
  `max_delivery_attempts`, the message is moved - via a durable
  `OP_DEADLETTER {id, reason, attempts, last_error}` - to a companion queue
  (`<queue>.dlq`) instead of being retried forever and starving the main
  queue. The DLQ is itself an ordinary queue, so the existing dequeue/stats
  API inspects it for free, and a `POST /dlq/redrive` endpoint moves messages
  back (attempts reset, optionally rate-limited) after the downstream bug is
  fixed. Failure metadata on the dead-letter record is what makes 3 a.m.
  debugging tolerable.

- **WAL compaction and snapshots.** The log currently grows without bound and
  recovery time is O(every record ever written). Two composable fixes:
  periodically write a **snapshot** - all live messages plus cursors and
  leases, tagged with the log sequence number (LSN) it covers - using the
  crash-safe dance *write temp → fsync → rename → fsync dir*, then delete log
  **segments** entirely below that LSN (segmented logs make deletion an
  `unlink`, not a rewrite). Recovery becomes "load newest valid snapshot,
  replay the tail," bounding both disk usage and startup time by the live set
  plus write rate rather than by history. A crash at any point during
  compaction leaves either the old state or the new state, never a mix.

- **Raft-based distributed clustering.** The single node is the honest
  limitation of the current design. The path to HA is mechanical precisely
  because of two decisions already in place: every state change is a single
  WAL record, and replay is a pure idempotent fold - which is exactly the
  shape of a Raft state machine. Each WAL record becomes a Raft log entry;
  "fsync to one disk" is replaced by "commit to a quorum of disks"; the leader
  serves enqueue/dequeue linearizably, followers replay identically, and
  failover promotes a follower that already has the full log. The snapshots
  above double as Raft snapshots for lagging and joining peers. Scale-out
  follows by partitioning per topic/queue into independent Raft groups
  (multi-raft). The latency cost - a network round trip per record - is
  amortized the same way single-node fsync cost should be: **group commit**,
  batching concurrent appends into one sync and one replication round, which
  is worth building even before clustering.

Smaller items on the same list: recording the queue mode in the log and
refusing a mismatched reopen (today `-mode` is trusted blindly against an
existing WAL); a Prometheus metrics endpoint; batch enqueue and dequeue;
long-polling dequeue that waits *outside* the critical section; message
cancellation, which the engine's unused `Remove` already supports; payload
size and schema validation hooks; and TLS plus authentication.

## 4. Why would users choose Queuemaxxing over incumbents like Amazon SQS, RabbitMQ, or Apache Pulsar?

**Zero runtime dependencies.** Queuemaxxing is one statically linked Go binary
and one file on disk. RabbitMQ brings the Erlang/OTP runtime and cluster
management; Pulsar brings JVM brokers *plus* Apache BookKeeper *plus*
ZooKeeper or etcd - three distributed systems to operate before your first
message; SQS requires an AWS account, network connectivity, IAM policy work,
and per-request billing. `scp` the Queuemaxxing binary to a box, point `-data`
at a path, and you have a durable queue with a web console. That is the entire
deployment story - no orchestration, no license, no cloud bill.

**Low memory footprint - edge and embedded computing.** The process is a Go
binary with two heaps and a map; each pending message costs its payload plus a
small, constant amount of bookkeeping, and recovery streams the log rather than
loading it. That profile fits Raspberry Pi-class ARM gateways, point-of-sale
terminals, industrial controllers, and air-gapped networks - environments where
a multi-hundred-megabyte JVM broker heap is disqualifying and a cloud-only
service like SQS simply does not exist when the uplink drops. A factory-floor
gateway can accept sensor events during a WAN outage, survive a power cut
mid-write (torn-tail recovery is tested), and drain the backlog when
connectivity returns.

**Local latency with the network removed.** An enqueue is one mutex, one heap
push, one `write(2)`, and one `fsync` to local storage; a dequeue is the same.
The floor is therefore local disk sync latency - sub-millisecond on NVMe.
Compare the floor of each alternative: SQS is an HTTPS round trip to a regional
service; RabbitMQ and Pulsar put a broker and the network in the loop by
design, even when perfectly tuned. For co-located producer/consumer
topologies - sidecars, job runners, CLI pipelines, edge nodes - Queuemaxxing
removes the network from the hot path entirely. (These are architectural floors
rather than benchmark results; this repository ships no performance suite, and
the honest caveat is below.)

**The single-queue fusion of Priority + Delay + LIFO.** This combination does
not exist natively in any of the incumbents:

| Capability            | Queuemaxxing | Amazon SQS | RabbitMQ | Apache Pulsar |
|-----------------------|--------------|------------|----------|----------------|
| Per-message priority  | Native (any int, one queue) | None - emulate with multiple queues + weighted polling | `x-max-priority` queues (declared up front, one sub-queue per level) | No native priority |
| Per-message delay     | Native, up to 10 years | Max 15 min; on FIFO queues delay is queue-level only, not per-message | Plugin (`delayed-message-exchange`) or TTL+DLX contortions with head-of-line pitfalls | Supported |
| LIFO ordering         | Native mode | Not supported | Not supported | Not supported |
| All three combined    | **One heap comparator** | No | No | No |
| Runtime dependencies  | None | AWS cloud | Erlang/OTP | JVM + BookKeeper + ZooKeeper |
| Built-in web console  | Yes, in the binary | AWS console | Management plugin | Separate UI deployment |

In Queuemaxxing every message carries `{priority, delay_seconds}` and the queue
mode picks FIFO or LIFO tie-breaking - one data structure, no plugins, no
emulation topology of N queues glued together in client code. "Urgent work
first, retries backed off into the future, newest-first when freshness
matters" is a single enqueue call, and the exact ordering is deterministic
(`seq` is a total order) and crash-stable (proven by the e2e recovery test).

**The honest scope.** Queuemaxxing is single-node; its delivery contract is
at-most-once until the ACK/lease work in Q&A #3 lands; fsync-per-record bounds
throughput to what one disk sustains under a serialized writer (group commit
is the planned fix); and the log does not yet compact. Teams needing
multi-region replication, exactly-once pipelines, or six-figure
messages/second should reach for the incumbents - that is what their
operational weight buys. Queuemaxxing wins where the weight is the problem:
edge and embedded deployments, single-box services, development and CI
environments, sidecar work queues - anywhere you want fsync-grade durability
with the operational footprint of SQLite.
