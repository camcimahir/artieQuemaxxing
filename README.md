# Queuemaxxing

A durable HTTP message queue in one Go binary. No database, no broker, no
dependencies. Every change is fsynced to a local write-ahead log before the
server replies, so a crash does not lose an accepted message.

It combines **priority**, **delay**, and **FIFO or LIFO** in one structure, so
you can run a delay + priority LIFO queue or a priority FIFO from the same
code.

```bash
go test ./...
go run ./cmd/server          # then open http://localhost:8080
go run ./cmd/client          # sample worker, second terminal
```

Add a message in the UI, or use **For testing** at the bottom. **Take next**
consumes. Kill the server (Ctrl+C), start it again, reload: the queue is still
there, rebuilt from `queue.wal`.

```bash
go run ./cmd/server -mode LIFO -port 9090
curl -s -X POST localhost:8080/enqueue -d '{"payload":"charge card","priority":9}'
curl -s -X POST localhost:8080/dequeue
```

Flags: `-port` / `PORT` (8080), `-mode` / `QUEUE_MODE` (FIFO or LIFO),
`-data` / `DATA_PATH` (`./data/queue.wal`).

## How it works

Order is three rules: delayed messages stay hidden until due, then highest
priority wins, then FIFO (oldest) or LIFO (newest) breaks ties.

Delivery is at-most-once. No ACK. After `POST /dequeue` returns 200, the
message is gone. Recovery is then one sentence: the live set is every
`ENQUEUE` without a matching `DEQUEUE`.

```
producer ── POST /enqueue ──▶  api  →  service  →  engine  ──▶  queue.wal
consumer ◀─ POST /dequeue ──   HTTP    one lock    2 heaps      (fsync)
you      ── GET / ──────────▶  embedded web UI
```

| Package | Job |
|---------|-----|
| `pkg/api` | HTTP, JSON, validation. No queue logic. |
| `pkg/service` | One lock. IDs and seq. Write log, fsync, then memory. |
| `pkg/engine` | Ready heap (priority + FIFO/LIFO) and delayed heap. |
| `pkg/storage` | Append-only JSONL. Replay on boot. |

Enqueue: lock, assign id/seq, append, fsync, insert, 201. Failed sync leaves
memory untouched (500). Dequeue: promote due messages, pop, append tombstone,
fsync, return the message (or 204). Handlers never wait.

A torn last line in the log (crash mid-write) is dropped. A bad line anywhere
earlier is a hard error. All mutations take one `RWMutex`. `go test -race ./...`
includes an HTTP hammer that crashes the service twice and checks the ledger.

API: `POST /enqueue`, `POST /dequeue`, `GET /messages` (read-only), `GET /wal`,
`GET /stats`, `GET /health`, `GET /` (UI).

`cmd/client` enqueues a checkout burst and drains it using the same three
rules. A mismatch is a real bug. The e2e test in `test/` does the same over a
torn WAL: 100 in, 30 out, crash, 70 survivors in the right order.

More detail: [`ARCHITECTURE.md`](ARCHITECTURE.md).

## Additional questions

**Replay.** Today dequeue deletes. Replay means rebuild after a crash
(ENQUEUE inserts, DEQUEUE deletes), not redeliver history. True replay would
keep the body in the log and move a per-consumer cursor, plus a sparse
seq-to-offset index so you can seek without a full scan.

**Pub/Sub.** Topics are named logs. Each subscription gets its own
`FrankensteinQueue` (own heaps, delay, FIFO/LIFO). Share the payload once;
give each subscriber a small envelope. Today's API becomes a default topic
with one subscription.

**If I had more time.** Leases and ACKs (at-least-once, then a DLQ), log
compaction, group commit (one fsync per batch), then Raft. The WAL is already
shaped like a Raft log. Also: persist `-mode` in the file, TLS, batch
enqueue/dequeue.

**Why not SQS / RabbitMQ / Pulsar.** Not for multi-region HA. For one binary
and one file, and for priority + delay + LIFO in a single queue (SQS has no
per-message priority; none of them do LIFO). Limits are honest: single node,
at-most-once, fsync per record, log does not compact yet.

## Code

`pkg/engine/queue.go` scheduler · `pkg/storage/wal.go` durability ·
`pkg/service/queue_service.go` lock and WAL-first writes ·
`test/e2e_recovery_test.go` crash tests. Standard library only.
