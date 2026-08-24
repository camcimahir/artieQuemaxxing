# Queuemaxxing

A durable, high-concurrency HTTP message queue in a **single Go binary** with
**zero external dependencies**. Every message is fsynced to an append-only
Write-Ahead Log before the server acknowledges it, so a hard crash — SIGKILL,
power loss, OOM — never loses an accepted message. Scheduling fuses three
behaviors that incumbent queues force you to pick between: **per-message
priority**, **per-message delay**, and **FIFO or LIFO** ordering, all in one
O(log n) queue.

```
producer ──POST /enqueue──▶ ┌──────────────────────────────┐
                            │  HTTP (pkg/api)              │
                            │  QueueService (pkg/service)  │──▶ wal.log
consumer ◀─POST /dequeue─── │  FrankensteinQueue (engine)  │   (fsync per record)
                            └──────────────────────────────┘
```

See [`ARCHITECTURE.md`](ARCHITECTURE.md) for the full design and
[`SKILLS.md`](SKILLS.md) for the engineering/testing conventions.

---

## Highlights

- **Zero runtime dependencies.** Standard library only. No broker cluster, no
  Erlang/JVM, no ZooKeeper, no external database. State is one JSONL file.
- **Crash-safe by construction.** WAL-first protocol: *write → fsync → mutate
  memory → respond*. Recovery replays the log; torn trailing writes are
  detected and dropped.
- **Priority + Delay + FIFO/LIFO in a single queue.** Higher priority wins;
  delayed messages are invisible until `available_at`; ties break by
  monotonic sequence number in the direction the queue mode dictates.
- **Deterministic total order.** A strictly monotonic `seq` assigned under
  the service lock means two messages can never compare equal.
- **Non-blocking API.** Empty dequeues return `204` immediately; no handler
  ever parks a goroutine while holding a lock.
- **Race-clean.** `go test -race ./...` is part of the definition of done,
  including an end-to-end crash test that hammers the HTTP API from 18
  goroutines and then kills the process mid-workload.

## Quick start

```bash
# Start a FIFO queue on :8080 with the WAL at ./data/queue.wal
go run ./cmd/server

# Or customize (flags or env vars):
go run ./cmd/server -port 9090 -mode LIFO -data /var/lib/qm/queue.wal
PORT=9090 QUEUE_MODE=LIFO DATA_PATH=/var/lib/qm/queue.wal go run ./cmd/server
```

Talk to it with curl:

```bash
# Enqueue: priority 5, visible immediately
curl -s -X POST localhost:8080/enqueue \
  -d '{"payload":"send-invoice-42","priority":5}'

# Enqueue: low priority, hidden for 90 seconds
curl -s -X POST localhost:8080/enqueue \
  -d '{"payload":"retry-webhook","priority":0,"delay_seconds":90}'

# Dequeue the best visible message (204 if nothing is visible yet)
curl -s -X POST localhost:8080/dequeue

# Occupancy snapshot
curl -s localhost:8080/stats
```

There is also a scripted demo client that enqueues five messages with mixed
priorities and delays against a running server and narrates why each dequeue
wins (`B → D → A → E → C`):

```bash
go run ./cmd/server          # terminal 1, fresh WAL
go run ./cmd/client          # terminal 2
```

## HTTP API

| Method & path     | Body                                        | Success                     | Notes |
|-------------------|---------------------------------------------|-----------------------------|-------|
| `POST /enqueue`   | `{"payload","priority","delay_seconds"}`    | `201` + full message JSON   | `payload` required; body capped at 1 MiB (`413` beyond) |
| `POST /dequeue`   | —                                           | `200` + message, or `204`   | `GET` also accepted; destructive (see Q&A #1) |
| `GET /stats`      | —                                           | `200` `{total,ready,delayed}` | Point-in-time snapshot |
| `GET /health`     | —                                           | `200` `{"status":"healthy"}` | Liveness probe |

Errors are JSON (`{"error":"..."}`): `400` malformed/invalid request, `413`
oversized body, `500` durability failure (the WAL could not be synced — the
operation did **not** happen), `503` service closed.

A message object looks like:

```json
{
  "id": "0d1e6a3f-9b0c-4a1e-8f2d-7c5b4a3e2d1f",
  "seq": 42,
  "payload": "send-invoice-42",
  "priority": 5,
  "enqueued_at": "2026-08-23T21:00:00.123456789Z",
  "available_at": "2026-08-23T21:01:30.123456789Z",
  "status": "PENDING"
}
```

## How it works

**Write path.** `QueueService` (the single concurrency choke point) assigns
`id` + monotonic `seq`, appends an `ENQUEUE` record to the WAL, calls
`file.Sync()`, and only then inserts the message into the in-memory engine
and answers `201`. A failed sync is a hard `500` and memory is untouched.

**Scheduling.** The engine (`FrankensteinQueue`) keeps two binary heaps: a
*delayed* min-heap keyed on `available_at` and a *ready* heap ordered by the
selection rule. Each dequeue first promotes every matured delayed message,
then pops the ready heap:

```
less(a, b):
    if a.priority != b.priority:  a.priority > b.priority   # higher first
    if mode == FIFO:              a.seq < b.seq              # oldest first
    else:                         a.seq > b.seq              # newest first
```

**Dequeue path.** The winning message's `DEQUEUE` record is appended and
synced *before* the message is handed to the consumer, so the log always
agrees with what left the queue.

**Recovery.** On startup the WAL is replayed as a pure, idempotent fold:
`ENQUEUE` inserts (duplicate IDs are no-ops), `DEQUEUE` deletes (missing IDs
are no-ops). A partial final line — a crash caught mid-append — fails JSON
parsing and is dropped; a corrupt line anywhere earlier is a hard error
because it implies silent data loss. What remains is exactly the set of
accepted-but-undelivered messages, re-inserted with their original `seq`,
priority, and `available_at`, so post-recovery ordering is identical to what
it would have been without the crash.

## Project layout

```
queuemaxxing/
├── ARCHITECTURE.md         # full system design
├── SKILLS.md               # engineering & testing conventions (mandatory)
├── cmd/
│   ├── server/             # HTTP server binary
│   └── client/             # scripted demo/verification client
├── pkg/
│   ├── model/              # Message, QueueMode, DTOs (stdlib only)
│   ├── engine/             # FrankensteinQueue: ready/delayed heaps
│   ├── storage/            # WAL: append-only JSONL, fsync, replay
│   ├── service/            # QueueService: lock layer, WAL-first protocol
│   ├── api/                # HTTP handlers, routing, status mapping
│   └── testutil/           # shared TestMain reporter (✅/❌ output)
├── test/
│   └── e2e_recovery_test.go  # end-to-end hard-crash recovery tests
└── sandbox.go              # runnable scratchpad for the engine
```

## Testing

```bash
gofmt -l .              # must print nothing
go vet ./...            # must report nothing
go test -v ./...        # colored ✅/❌ reporter via pkg/testutil
go test -race -v ./...  # non-negotiable; concurrency tests target this
```

The end-to-end crash test (`test/e2e_recovery_test.go`) runs the full stack
over real HTTP sockets and a real WAL on disk, table-driven over FIFO and
LIFO:

1. enqueues **100** messages with mixed priorities (0–9) and delays (0s /
   60s / 120s),
2. dequeues **30** and verifies even those came out in perfect order,
3. simulates a **hard crash**: the service is abandoned without `Close()`
   and a torn half-record is appended to the WAL, exactly what a process
   killed mid-append leaves behind,
4. re-initializes a **brand-new `QueueService` on the same WAL file**,
5. verifies **exactly 70** messages survived and drain in perfect
   priority/delay order as an injected clock steps across each delay
   boundary (including "still invisible one second early" checks).

A second e2e test hammers the API with 10 producers and 8 consumers, crashes
the process mid-workload (twice), and proves the ledger balances: every
message is delivered exactly once across the crashes and far-future delayed
messages survive both. Delay semantics are tested with an injected clock
(`service.WithNow`) — never `time.Sleep` (SKILLS.md §1.4).

---

# Design Q&A

## 1. How do you handle replay messages?

**What the system does today: destructive dequeues over an append-only log.**
The WAL itself is immutable — records are only ever appended
(`O_APPEND|O_CREATE`, one mutex-guarded writer, never edited in place) — but
*consumption is logically destructive*. `POST /dequeue` durably appends an
`OP_DEQUEUE` tombstone before the message is handed out, and recovery
(`storage.WAL.Recover`) is a pure fold: `ENQUEUE` inserts, `DEQUEUE` deletes.
A consumed message is therefore gone forever; "replay" today means **state
reconstruction after a crash**, not redelivery of history. That replay is
idempotent (duplicate `ENQUEUE`s and orphan `DEQUEUE`s are no-ops), so
replaying the log twice produces the same state as replaying it once, and a
torn trailing record is dropped while mid-file corruption is a hard error.

**How I would evolve it to support true message replay** (redelivering
already-consumed history to a consumer):

- **Stop deleting; move a cursor instead.** Keep the log exactly as
  append-only as it is now, but reinterpret consumption: instead of an
  `OP_DEQUEUE` tombstone meaning "this message no longer exists," an
  `OP_ACK {group, seq}` record means "consumer group *g* is done with
  message *seq*." The message body stays in the log. Replay then costs
  nothing to implement: it is just rewinding a cursor, because the data was
  never destroyed.

- **WAL offset-based indexing.** Replay from an arbitrary point requires
  random access into the log. Each record already carries a strictly
  increasing `seq`; the missing piece is a **sparse index** mapping
  `seq → byte offset` (one entry every N records or every 4 KiB block,
  maintained in memory and checkpointed to a side file). Locating "message
  seq S" becomes a binary search over the sparse index plus a short forward
  scan — O(log n + K) instead of a full-file scan. Because records are
  appended in timestamp order, the same index supports "replay from
  timestamp T" via binary search on `ts`. Segmenting the log (e.g. 64 MiB
  chunks) keeps each index small and makes retention cheap (drop whole
  segments).

- **Consumer group cursors.** Each named consumer group gets durable
  position state: a **low-watermark cursor** (`everything ≤ W is consumed`)
  plus an **exception set** of seqs acked above the watermark. The exception
  set is required specifically because this queue delivers in *priority*
  order, not log order — a high-priority late message is consumed before an
  older low-priority one, so acks arrive out of log order and the watermark
  can only advance when the holes below it fill. Cursor updates are
  themselves WAL records (`OP_ACK`, periodically compacted into an
  `OP_CURSOR {group, watermark, exceptions}` checkpoint), so group positions
  survive crashes exactly like messages do. Replay for one group is then an
  administrative record — `OP_SEEK {group, to: seq|timestamp}` — that
  rewinds that group's cursor without touching any other group.

- **Trade-offs, honestly.** Destructive dequeues (current design) keep
  recovery state minimal — live set only — and reclaim space naturally via
  compaction; but they can never redeliver, audit, or fan out. An immutable
  log with cursors buys replay, multiple independent consumers, and
  debuggability (`jq` over history), at the cost of retention policy
  becoming a real decision (time/size-based, or "min cursor across groups"),
  storage growing until retention triggers, and recovery having to scan
  retained history rather than just the live set — which is why this feature
  pairs with snapshots/compaction (Q&A #3). The v1 choice of destructive
  dequeues was deliberate: it makes the recovery invariant trivial to state
  and test. The migration path above is incremental because the log format
  already tolerates unknown ops — old logs replay fine under new code.

## 2. How would you refactor your queue into a Pub/Sub?

Today Queuemaxxing is point-to-point: one implicit queue, and a dequeue
consumes the message for everyone. Pub/Sub decouples *what happened* (a
topic's stream of events) from *who needs it* (each subscription's own
delivery state). The refactor keeps every existing layer and slots two new
concepts between them:

- **Topics.** A named, immutable, append-only message log — which is
  exactly what the WAL already is, plus a `topic` field on each record and a
  registry (`map[string]*Topic` under an RWMutex, mirrored by durable
  `OP_CREATE_TOPIC` records). Publishing appends to the topic's log:
  `POST /topics/{topic}/publish`.

- **Individual consumer subscription queues.** Each subscription (consumer
  group) on a topic owns its **own `FrankensteinQueue` instance** — its own
  ready/delayed heaps, its own cursor, its own pending set. This is the key
  structural move: the engine is already a self-contained, internally
  locked, HTTP-unaware data structure, so instantiating one per subscription
  costs nothing architecturally and buys each subscriber *independent*
  priority/delay/FIFO-or-LIFO semantics and independent pacing. A slow
  subscriber backs up only its own queue. Subscriptions are durable
  (`OP_SUBSCRIBE {topic, group, from: earliest|latest|seq}`) and recovery
  rebuilds each one's pending view from the topic log minus its acked set.
  API sketch: `POST /subscriptions {topic, name, from}`, then
  `POST /subscriptions/{name}/dequeue` and `/ack` — the current endpoints
  become sugar for a default topic with a single default subscription, which
  is the backward-compatibility story.

- **Fan-out routing.** On publish, the router makes the message visible to
  every subscription of the topic. Two strategies with very different cost
  profiles:
  - *Eager cloning:* insert a full copy into every subscription queue at
    publish time. Simple, great isolation, no indirection at delivery — but
    O(S) write and memory amplification per message across S subscriptions,
    and a subscription created later cannot backfill history.
  - *Lazy referencing:* store the payload **once** in the topic log; each
    subscription queue holds only a lightweight reference — `{seq, priority,
    available_at}`, a few dozen bytes — and the body is fetched by seq
    through the sparse offset index (Q&A #1) at delivery time. Storage stays
    O(1) per message regardless of fan-out, and `from: earliest`
    subscriptions work for free.

- **Message cloning vs. referencing, resolved.** The right split is
  *immutable shared body, per-subscription mutable envelope*. The payload is
  written once and never mutated (safe to share by reference, refcounted).
  Everything a subscription needs to mutate — delivery attempts, lease
  deadline, per-subscriber delay overrides, status — lives in that
  subscription's small envelope, which is what actually sits in its
  `FrankensteinQueue`. Garbage collection falls out naturally: a message's
  storage is reclaimable once the minimum cursor across all subscriptions
  has passed it (and retention allows), which composes with segment-based
  compaction. The slow-consumer pathology (one dead subscription pinning the
  whole log) is bounded by retention policy plus dead-lettering (Q&A #3).

Concurrency-wise this preserves the house lock discipline (SKILLS.md §2.2):
the topic registry lock orders before per-subscription engine locks, engine
and WAL stay leaf locks, and publish touches subscription queues one at a
time — no lock cycles.

## 3. If you had more time, what other features would you add?

In priority order, because each one unlocks the next:

- **Consumer ACKs and visibility timeouts with heartbeat leases.** Today a
  dequeue is destructive at delivery time, which makes the post-delivery
  contract *at-most-once*: if the consumer crashes after `200 OK`, that
  message is gone. The fix is to make dequeue grant a **lease** instead:
  `OP_LEASE {id, consumer, deadline = now + visibility_timeout}` is logged,
  the message moves to an in-memory inflight table keyed by a min-heap of
  lease deadlines, and it is invisible to other consumers while leased. The
  consumer must `POST /ack {id}` (an `OP_ACK` record — the true point of
  consumption) before the deadline. Long-running jobs send heartbeats
  (`POST /heartbeat {id}` → `OP_EXTEND {id, new_deadline}`): a healthy-but-
  slow consumer keeps its lease alive indefinitely, while a dead consumer's
  lease lapses and an expiry scanner (a ticker goroutine with a proper
  shutdown path, per SKILLS.md §2.3) requeues the message with
  `attempts + 1`. Crash recovery composes cleanly: replay re-arms unexpired
  leases and immediately requeues expired ones — upgrading the whole system
  to genuine at-least-once delivery.

- **Dead Letter Queues (DLQ).** With attempts tracked by the lease
  machinery, poison pills become detectable: when `attempts` exceeds a
  per-queue `max_delivery_attempts`, the message is moved — via a durable
  `OP_DEADLETTER {id, reason, attempts, last_error}` — to a companion queue
  (`<queue>.dlq`) instead of being retried forever and starving the main
  queue. The DLQ is itself an ordinary queue, so the existing dequeue/stats
  API inspects it for free, and a `POST /dlq/redrive` endpoint moves
  messages back (attempts reset, optionally rate-limited) after the
  downstream bug is fixed. Failure metadata on the dead-letter record is
  what makes 3 a.m. debugging tolerable.

- **WAL compaction and snapshots.** The log currently grows without bound
  and recovery time is O(every record ever written). Two composable fixes:
  periodically write a **snapshot** — all live messages plus cursors/leases,
  tagged with the log sequence number (LSN) it covers — using the
  crash-safe dance *write temp → fsync → rename → fsync dir*, then delete
  log **segments** entirely below that LSN (segmented logs make deletion an
  `unlink`, not a rewrite). Recovery becomes "load newest valid snapshot,
  replay the tail," bounding both disk usage and startup time by the live
  set + write rate rather than by history. A crash at any point during
  compaction leaves either the old state or the new state, never a mix.

- **Raft-based distributed clustering.** The single node is the honest
  limitation of the current design. The path to HA is mechanical precisely
  because of two decisions already in place: every state change is a single
  WAL record, and replay is a pure idempotent fold — which is exactly the
  shape of a Raft state machine. Each WAL record becomes a Raft log entry;
  "fsync to one disk" is replaced by "commit to a quorum of disks"; the
  leader serves enqueue/dequeue linearizably, followers replay identically,
  and failover promotes a follower that already has the full log. The
  snapshots above double as Raft snapshots for lagging/joining peers.
  Scale-out follows by partitioning per topic/queue into independent Raft
  groups (multi-raft). The latency cost — a network round trip per record —
  is amortized the same way single-node fsync cost should be: **group
  commit** (batching concurrent appends into one sync/replication round),
  which is worth building even before clustering.

Smaller items on the same list: Prometheus metrics endpoint, batch
enqueue/dequeue, long-polling dequeue (waiting *outside* the critical
section), payload size/schema validation hooks, and TLS/auth.

## 4. Why would users choose Queuemaxxing over incumbents like Amazon SQS, RabbitMQ, or Apache Pulsar?

**Zero runtime dependencies.** Queuemaxxing is one statically linked Go
binary and one file on disk. RabbitMQ brings the Erlang/OTP runtime and
cluster management; Pulsar brings JVM brokers *plus* Apache BookKeeper
*plus* ZooKeeper/etcd — three distributed systems to operate before your
first message; SQS requires an AWS account, network connectivity, IAM
policy work, and per-request billing. `scp` the Queuemaxxing binary to a
box, point `-data` at a path, and you have a durable queue. That is the
entire deployment story — no orchestration, no license, no cloud bill.

**Low memory footprint — edge and embedded computing.** The process is a
Go binary with two heaps and a map; idle RSS is single-digit megabytes, and
each pending message costs its payload plus ~100 bytes of bookkeeping. That
fits Raspberry Pi-class ARM gateways, point-of-sale terminals, industrial
controllers, and air-gapped networks — environments where a multi-hundred-MB
JVM broker heap is disqualifying and a cloud-only service like SQS simply
does not exist when the uplink drops. A factory-floor gateway can accept
sensor events during a WAN outage, survive a power cut mid-write (torn-tail
recovery is tested), and drain the backlog when connectivity returns.

**Instant sub-millisecond local latency.** An enqueue is: one mutex, one
heap push, one `write(2)`, one `fsync` to local storage. On NVMe that is
comfortably sub-millisecond end to end, and a dequeue is the same. Compare
the floor of each alternative: SQS is an HTTPS round trip to a regional
service — tens of milliseconds typical; RabbitMQ and Pulsar are 1–5 ms over
a LAN plus broker hops even when perfectly tuned, because the network is in
the loop by design. For co-located producer/consumer topologies — sidecars,
job runners, CLI pipelines, edge nodes — Queuemaxxing removes the network
from the hot path entirely and is one to two orders of magnitude faster per
operation.

**The single-queue dynamic fusion of Priority + Delay + LIFO.** This
combination does not exist natively in any of the incumbents:

| Capability            | Queuemaxxing | Amazon SQS | RabbitMQ | Apache Pulsar |
|-----------------------|--------------|------------|----------|----------------|
| Per-message priority  | Native (any int, one queue) | None — emulate with multiple queues + weighted polling | `x-max-priority` queues (≤255 levels, per-level sub-queues, declared up front) | No native priority |
| Per-message delay     | Native, unbounded | Max 15 min; unsupported on FIFO queues | Plugin (`delayed-message-exchange`) or TTL+DLX contortions with head-of-line pitfalls | Supported |
| LIFO ordering         | Native mode | Impossible | Not supported | Not supported |
| All three combined    | **One heap comparator** | No | No | No |
| Runtime dependencies  | None | AWS cloud | Erlang/OTP | JVM + BookKeeper + ZooKeeper |
| Local p50 latency     | Sub-ms | ~10–70 ms (network) | ~1–5 ms (LAN) | ~1–5 ms (LAN) |

In Queuemaxxing every message carries `{priority, delay_seconds}` and the
queue mode picks FIFO or LIFO tie-breaking — one data structure, no plugins,
no emulation topology of N queues glued together in client code. "Urgent
work first, retries backed off into the future, newest-first when freshness
matters" is a single enqueue call, and the exact ordering is deterministic
(`seq` is a total order) and crash-stable (proven by the e2e recovery test).

**The honest scope.** Queuemaxxing is single-node today, its post-delivery
contract is at-most-once until the ACK/lease work in Q&A #3 lands, and
fsync-per-record bounds throughput to what one disk sustains (group commit
is the planned fix). Teams needing multi-region replication, exactly-once
pipelines, or six-figure messages/second should reach for the incumbents —
that is what their operational weight buys. Queuemaxxing wins where the
weight is the problem: edge and embedded deployments, single-box services,
development and CI environments, sidecar work queues — anywhere you want
SQS-grade durability semantics with the operational footprint of SQLite.
