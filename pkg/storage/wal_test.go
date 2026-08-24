// Package storage_test exercises the exported WAL API per SKILLS.md: every
// test opens its own log under t.TempDir(), durability and replay are checked
// by reopening the file rather than by inspecting internals, and crash cases
// are produced by writing real bytes to disk rather than by mocking.
package storage_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"queuemaxxing/pkg/model"
	"queuemaxxing/pkg/storage"
)

var base = time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)

func openWAL(t *testing.T) (*storage.WAL, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := storage.NewWAL(path)
	if err != nil {
		t.Fatalf("NewWAL: unexpected error: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w, path
}

func reopenWAL(t *testing.T, path string) *storage.WAL {
	t.Helper()
	w, err := storage.NewWAL(path)
	if err != nil {
		t.Fatalf("NewWAL(%q): unexpected error: %v", path, err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func mkMsg(id string, seq uint64, priority int) model.Message {
	return model.Message{
		ID:          id,
		Seq:         seq,
		Payload:     "payload:" + id,
		Priority:    priority,
		EnqueuedAt:  base,
		AvailableAt: base.Add(time.Duration(priority) * time.Second),
		Status:      model.StatusPending,
	}
}

func idsOf(msgs []model.Message) []string {
	ids := make([]string, len(msgs))
	for i, m := range msgs {
		ids[i] = m.ID
	}
	return ids
}

func messagesEqual(t *testing.T, got, want model.Message) {
	t.Helper()
	if got.ID != want.ID || got.Seq != want.Seq || got.Payload != want.Payload ||
		got.Priority != want.Priority || got.Status != want.Status {
		t.Fatalf("message mismatch:\n got %+v\nwant %+v", got, want)
	}
	if !got.EnqueuedAt.Equal(want.EnqueuedAt) {
		t.Fatalf("EnqueuedAt: got %v, want %v", got.EnqueuedAt, want.EnqueuedAt)
	}
	if !got.AvailableAt.Equal(want.AvailableAt) {
		t.Fatalf("AvailableAt: got %v, want %v", got.AvailableAt, want.AvailableAt)
	}
}

func TestWriteEnqueue_AppendAndReadBack(t *testing.T) {
	t.Parallel()
	w, _ := openWAL(t)

	want := []model.Message{
		mkMsg("m-1", 1, 0),
		mkMsg("m-2", 2, 5),
		mkMsg("m-3", 3, 1),
	}
	for _, msg := range want {
		if err := w.WriteEnqueue(msg); err != nil {
			t.Fatalf("WriteEnqueue(%s): %v", msg.ID, err)
		}
	}

	got, err := w.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("Recover returned %d messages, want %d (%v)", len(got), len(want), idsOf(got))
	}
	for i := range want {
		messagesEqual(t, got[i], want[i])
	}
}

func TestRecover_CrashSimulation_ReturnsUnconsumedMessages(t *testing.T) {
	t.Parallel()
	w, path := openWAL(t)

	msgs := []model.Message{
		mkMsg("m-1", 1, 0),
		mkMsg("m-2", 2, 1),
		mkMsg("m-3", 3, 2),
		mkMsg("m-4", 4, 3),
		mkMsg("m-5", 5, 4),
	}
	for _, msg := range msgs {
		if err := w.WriteEnqueue(msg); err != nil {
			t.Fatalf("WriteEnqueue(%s): %v", msg.ID, err)
		}
	}
	for _, id := range []string{"m-2", "m-4"} {
		if err := w.WriteDequeue(id); err != nil {
			t.Fatalf("WriteDequeue(%s): %v", id, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Simulate process crash: a fresh WAL handle on the same durable file.
	w2 := reopenWAL(t, path)
	got, err := w2.Recover()
	if err != nil {
		t.Fatalf("Recover after reopen: %v", err)
	}

	wantIDs := []string{"m-1", "m-3", "m-5"}
	if gotIDs := idsOf(got); !slices.Equal(gotIDs, wantIDs) {
		t.Fatalf("recovered IDs = %v, want %v", gotIDs, wantIDs)
	}
	want := []model.Message{msgs[0], msgs[2], msgs[4]}
	for i := range want {
		messagesEqual(t, got[i], want[i])
	}
}

func TestRecover_IgnoresTornTrailingLine(t *testing.T) {
	t.Parallel()
	w, path := openWAL(t)

	intact := []model.Message{
		mkMsg("m-1", 1, 0),
		mkMsg("m-2", 2, 1),
	}
	for _, msg := range intact {
		if err := w.WriteEnqueue(msg); err != nil {
			t.Fatalf("WriteEnqueue(%s): %v", msg.ID, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open for torn append: %v", err)
	}
	// Partial JSON with no trailing newline: a crash mid-write.
	if _, err := f.Write([]byte(`{"v":1,"op":"ENQUEUE","seq":99,"id":"m-torn","payload":"partial`)); err != nil {
		t.Fatalf("writing torn tail: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close torn append: %v", err)
	}

	w2 := reopenWAL(t, path)
	got, err := w2.Recover()
	if err != nil {
		t.Fatalf("Recover with torn tail: unexpected error: %v", err)
	}
	if gotIDs := idsOf(got); !slices.Equal(gotIDs, []string{"m-1", "m-2"}) {
		t.Fatalf("recovered IDs = %v, want [m-1 m-2] (torn tail must be dropped)", gotIDs)
	}
	for i := range intact {
		messagesEqual(t, got[i], intact[i])
	}
}

func TestRecover_CorruptMiddleLineIsError(t *testing.T) {
	t.Parallel()
	w, path := openWAL(t)

	if err := w.WriteEnqueue(mkMsg("m-1", 1, 0)); err != nil {
		t.Fatalf("WriteEnqueue: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	corrupt := append([]byte{}, raw...)
	corrupt = append(corrupt, []byte("this is not json\n")...)
	// A well-formed record after the corrupt line proves the failure is
	// in the middle of the file, not a torn tail.
	tail, err := json.Marshal(map[string]any{
		"v": 1, "op": storage.OP_ENQUEUE, "seq": 2, "id": "m-2", "payload": "x",
	})
	if err != nil {
		t.Fatalf("marshal tail: %v", err)
	}
	corrupt = append(corrupt, tail...)
	corrupt = append(corrupt, '\n')
	if err := os.WriteFile(path, corrupt, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	w2 := reopenWAL(t, path)
	if _, err := w2.Recover(); err == nil {
		t.Fatal("Recover: expected error for corrupt middle line, got nil")
	}
}

func TestRecover_IdempotentReplay(t *testing.T) {
	t.Parallel()
	w, _ := openWAL(t)

	if err := w.WriteEnqueue(mkMsg("m-1", 1, 0)); err != nil {
		t.Fatalf("WriteEnqueue: %v", err)
	}
	if err := w.WriteEnqueue(mkMsg("m-2", 2, 1)); err != nil {
		t.Fatalf("WriteEnqueue: %v", err)
	}
	if err := w.WriteDequeue("m-1"); err != nil {
		t.Fatalf("WriteDequeue: %v", err)
	}

	first, err := w.Recover()
	if err != nil {
		t.Fatalf("Recover first: %v", err)
	}
	second, err := w.Recover()
	if err != nil {
		t.Fatalf("Recover second: %v", err)
	}
	if !reflect.DeepEqual(idsOf(first), idsOf(second)) {
		t.Fatalf("replay twice diverged: first %v, second %v", idsOf(first), idsOf(second))
	}
	if len(first) != 1 || first[0].ID != "m-2" {
		t.Fatalf("recovered IDs = %v, want [m-2]", idsOf(first))
	}
}

func TestWriteEnqueue_DuplicateIDIsNoopOnRecover(t *testing.T) {
	t.Parallel()
	w, _ := openWAL(t)

	orig := mkMsg("m-1", 1, 0)
	dup := orig
	dup.Payload = "replaced"
	dup.Priority = 9
	if err := w.WriteEnqueue(orig); err != nil {
		t.Fatalf("WriteEnqueue orig: %v", err)
	}
	if err := w.WriteEnqueue(dup); err != nil {
		t.Fatalf("WriteEnqueue dup: %v", err)
	}

	got, err := w.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("recovered %d messages, want 1", len(got))
	}
	messagesEqual(t, got[0], orig)
}

func TestWrite_EmptyID(t *testing.T) {
	t.Parallel()
	w, _ := openWAL(t)

	if err := w.WriteEnqueue(model.Message{Payload: "x"}); !errors.Is(err, storage.ErrEmptyID) {
		t.Fatalf("WriteEnqueue empty ID: got %v, want ErrEmptyID", err)
	}
	if err := w.WriteDequeue(""); !errors.Is(err, storage.ErrEmptyID) {
		t.Fatalf("WriteDequeue empty ID: got %v, want ErrEmptyID", err)
	}
}

func TestWrite_AfterCloseReturnsErrClosed(t *testing.T) {
	t.Parallel()
	w, _ := openWAL(t)
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := w.WriteEnqueue(mkMsg("m-1", 1, 0)); !errors.Is(err, storage.ErrClosed) {
		t.Fatalf("WriteEnqueue after close: got %v, want ErrClosed", err)
	}
	if _, err := w.Recover(); !errors.Is(err, storage.ErrClosed) {
		t.Fatalf("Recover after close: got %v, want ErrClosed", err)
	}
}

func TestWAL_ConcurrentWrites(t *testing.T) {
	t.Parallel()
	w, path := openWAL(t)

	const n = 32
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			msg := mkMsg(fmt.Sprintf("m-%d", i), uint64(i+1), i%5)
			if err := w.WriteEnqueue(msg); err != nil {
				t.Errorf("WriteEnqueue(%d): %v", i, err)
			}
		}()
	}
	wg.Wait()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2 := reopenWAL(t, path)
	got, err := w2.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(got) != n {
		t.Fatalf("recovered %d messages, want %d", len(got), n)
	}
	seen := make(map[string]struct{}, n)
	for _, m := range got {
		if _, dup := seen[m.ID]; dup {
			t.Fatalf("duplicate ID after concurrent writes: %s", m.ID)
		}
		seen[m.ID] = struct{}{}
	}
}

// TestRecord_SeparatesRecordOrderFromMessageOrder: the envelope's seq counts
// records in this file; msg_seq carries the queue's message ordering key.
// They coincide in a log of pure enqueues and diverge the moment a DEQUEUE is
// interleaved. Recovery must read the ordering key from msg_seq — taking it
// from seq would silently renumber every surviving message after each
// consumption, quietly changing FIFO/LIFO tie-breaking across a restart.
func TestRecord_SeparatesRecordOrderFromMessageOrder(t *testing.T) {
	t.Parallel()
	w, path := openWAL(t)

	for _, msg := range []model.Message{mkMsg("m-1", 1, 0), mkMsg("m-2", 2, 0), mkMsg("m-3", 3, 0)} {
		if err := w.WriteEnqueue(msg); err != nil {
			t.Fatalf("WriteEnqueue(%s): %v", msg.ID, err)
		}
	}
	if err := w.WriteDequeue("m-2"); err != nil { // record #4
		t.Fatalf("WriteDequeue: %v", err)
	}
	if err := w.WriteEnqueue(mkMsg("m-4", 4, 0)); err != nil { // record #5, message seq 4
		t.Fatalf("WriteEnqueue(m-4): %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if len(lines) != 5 {
		t.Fatalf("wrote %d lines, want 5", len(lines))
	}

	var records []map[string]any
	for i, line := range lines {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("line %d is not valid JSON: %v", i+1, err)
		}
		records = append(records, m)
		if got, want := m["seq"], float64(i+1); got != want {
			t.Fatalf("line %d: seq = %v, want %v (record numbers are the append order)", i+1, got, want)
		}
	}

	// The interleaved DEQUEUE pushed record numbers ahead of message numbers.
	last := records[4]
	if got, want := last["seq"], float64(5); got != want {
		t.Fatalf("final record seq = %v, want %v", got, want)
	}
	if got, want := last["msg_seq"], float64(4); got != want {
		t.Fatalf("final record msg_seq = %v, want %v — the two fields must not be the same number here", got, want)
	}

	// A tombstone is a tombstone: no message payload, no zero-value stamps
	// padding out the line. The log is meant to be readable with jq.
	tomb := records[3]
	if tomb["op"] != storage.OP_DEQUEUE {
		t.Fatalf("record 4 op = %v, want %v", tomb["op"], storage.OP_DEQUEUE)
	}
	for _, key := range []string{"payload", "msg_seq", "priority", "enqueued_at", "available_at", "status"} {
		if _, present := tomb[key]; present {
			t.Fatalf("DEQUEUE tombstone carries %q = %v; it should be omitted entirely", key, tomb[key])
		}
	}

	// Recovery must restore the message ordering keys, not the record numbers.
	w2 := reopenWAL(t, path)
	got, err := w2.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if gotIDs := idsOf(got); !slices.Equal(gotIDs, []string{"m-1", "m-3", "m-4"}) {
		t.Fatalf("recovered IDs = %v, want [m-1 m-3 m-4]", gotIDs)
	}
	wantSeqs := map[string]uint64{"m-1": 1, "m-3": 3, "m-4": 4}
	for _, msg := range got {
		if msg.Seq != wantSeqs[msg.ID] {
			t.Fatalf("%s recovered with Seq = %d, want %d (msg_seq, not the record number)", msg.ID, msg.Seq, wantSeqs[msg.ID])
		}
	}
}

// TestRecover_TimestampsSurviveTheRoundTrip guards the optional-timestamp
// encoding: EnqueuedAt/AvailableAt are pointers so tombstones can omit them,
// which would silently zero every recovered delay if the write path ever
// stopped setting them.
func TestRecover_TimestampsSurviveTheRoundTrip(t *testing.T) {
	t.Parallel()
	w, path := openWAL(t)

	want := model.Message{
		ID:          "m-delayed",
		Seq:         7,
		Payload:     "later",
		Priority:    3,
		EnqueuedAt:  base,
		AvailableAt: base.Add(90 * time.Minute),
		Status:      model.StatusPending,
	}
	if err := w.WriteEnqueue(want); err != nil {
		t.Fatalf("WriteEnqueue: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, err := reopenWAL(t, path).Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("recovered %d messages, want 1", len(got))
	}
	messagesEqual(t, got[0], want)
}

// TestEnqueueRecord_AlwaysCarriesPriorityEvenWhenZero: priority 0 is the
// documented default, so it is the most common value in a real log. Encoding
// it with a plain `int,omitempty` would drop the field from the majority of
// ENQUEUE records — a jq filter over `.priority` would render null exactly
// where the log is read most. The pointer encoding must keep the field
// present on ENQUEUE while still omitting it from tombstones.
func TestEnqueueRecord_AlwaysCarriesPriorityEvenWhenZero(t *testing.T) {
	t.Parallel()
	w, path := openWAL(t)

	zero := mkMsg("m-zero", 1, 0)
	zero.Priority = 0
	if err := w.WriteEnqueue(zero); err != nil {
		t.Fatalf("WriteEnqueue: %v", err)
	}
	if err := w.WriteDequeue("m-zero"); err != nil {
		t.Fatalf("WriteDequeue: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("wrote %d lines, want 2", len(lines))
	}

	var enq map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &enq); err != nil {
		t.Fatalf("ENQUEUE line is not valid JSON: %v", err)
	}
	got, present := enq["priority"]
	if !present {
		t.Fatalf("ENQUEUE record omits priority for a zero-priority message; line = %s", lines[0])
	}
	if got != float64(0) {
		t.Fatalf("priority = %v, want 0", got)
	}

	var tomb map[string]any
	if err := json.Unmarshal([]byte(lines[1]), &tomb); err != nil {
		t.Fatalf("DEQUEUE line is not valid JSON: %v", err)
	}
	if _, present := tomb["priority"]; present {
		t.Fatalf("DEQUEUE tombstone carries priority; it should be omitted. line = %s", lines[1])
	}

	// And a zero priority must survive the round trip as zero.
	if err := w2ReopenAndCheckZero(t, path); err != nil {
		t.Fatal(err)
	}
}

func w2ReopenAndCheckZero(t *testing.T, path string) error {
	t.Helper()
	w := reopenWAL(t, path)
	msgs, err := w.Recover()
	if err != nil {
		return fmt.Errorf("Recover: %w", err)
	}
	if len(msgs) != 0 {
		return fmt.Errorf("recovered %d messages, want 0 (the message was dequeued)", len(msgs))
	}
	return nil
}

// TestRecover_UnknownOpIsSkipped: unrecognized ops must not fail recovery and
// must not disturb live messages. This is the forward-compatibility rule
// ARCHITECTURE.md §3 and SKILLS.md §3.2 document — a log written by a newer
// build still replays under this one.
func TestRecover_UnknownOpIsSkipped(t *testing.T) {
	t.Parallel()
	w, path := openWAL(t)
	if err := w.WriteEnqueue(mkMsg("m-1", 1, 0)); err != nil {
		t.Fatalf("WriteEnqueue: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open for unknown-op append: %v", err)
	}
	if _, err := f.WriteString(`{"v":1,"op":"LEASE","seq":2,"id":"m-1","deadline":"2099-01-01T00:00:00Z"}` + "\n"); err != nil {
		t.Fatalf("writing unknown op: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close unknown-op append: %v", err)
	}

	got, err := reopenWAL(t, path).Recover()
	if err != nil {
		t.Fatalf("Recover with unknown op: %v", err)
	}
	if gotIDs := idsOf(got); !slices.Equal(gotIDs, []string{"m-1"}) {
		t.Fatalf("recovered IDs = %v, want [m-1] (LEASE must be skipped, not treated as a dequeue)", gotIDs)
	}
}

// TestRecover_OlderWriterRecords: a record with no msg_seq and no status is
// what an older build wrote before those fields existed. Recovery must take
// the envelope seq as the ordering key and default status to PENDING.
func TestRecover_OlderWriterRecords(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "wal.log")
	legacy := strings.Join([]string{
		`{"v":1,"op":"ENQUEUE","seq":4,"id":"legacy-b","payload":"b","priority":1}`,
		`{"v":1,"op":"ENQUEUE","seq":2,"id":"legacy-a","payload":"a","priority":0}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(legacy), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	got, err := reopenWAL(t, path).Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if gotIDs := idsOf(got); !slices.Equal(gotIDs, []string{"legacy-a", "legacy-b"}) {
		t.Fatalf("recovered IDs = %v, want [legacy-a legacy-b] in seq order (seq 2 then 4)", gotIDs)
	}
	if got[0].Seq != 2 || got[1].Seq != 4 {
		t.Fatalf("recovered seqs = %d, %d; want 2, 4 from the envelope seq fallback", got[0].Seq, got[1].Seq)
	}
	if got[0].Status != model.StatusPending || got[1].Status != model.StatusPending {
		t.Fatalf("recovered statuses = %q, %q; want PENDING for a missing status field", got[0].Status, got[1].Status)
	}
	if got[0].Priority != 0 {
		t.Fatalf("legacy-a priority = %d, want 0", got[0].Priority)
	}
}

// tailRecord is the envelope subset the Tail tests inspect. Tail returns
// raw on-disk JSON, so tests decode it the way any client would.
type tailRecord struct {
	Op  string `json:"op"`
	Seq uint64 `json:"seq"`
	ID  string `json:"id"`
}

func decodeTail(t *testing.T, tail model.WalTail) []tailRecord {
	t.Helper()
	out := make([]tailRecord, len(tail.Records))
	for i, raw := range tail.Records {
		if err := json.Unmarshal(raw, &out[i]); err != nil {
			t.Fatalf("record %d is not valid JSON: %v; raw=%s", i, err, raw)
		}
	}
	return out
}

func TestTail_ReturnsRecordsInLogOrder(t *testing.T) {
	t.Parallel()
	w, path := openWAL(t)

	for i := 1; i <= 3; i++ {
		if err := w.WriteEnqueue(mkMsg(fmt.Sprintf("m-%d", i), uint64(i), i)); err != nil {
			t.Fatalf("WriteEnqueue m-%d: %v", i, err)
		}
	}
	if err := w.WriteDequeue("m-2"); err != nil {
		t.Fatalf("WriteDequeue: %v", err)
	}

	tail, err := w.Tail(10)
	if err != nil {
		t.Fatalf("Tail: %v", err)
	}
	if tail.Path != path {
		t.Fatalf("path = %q, want %q", tail.Path, path)
	}
	if tail.SizeBytes <= 0 {
		t.Fatalf("size_bytes = %d, want > 0", tail.SizeBytes)
	}
	if tail.Truncated {
		t.Fatal("truncated = true, want false when the whole log fits the limit")
	}

	recs := decodeTail(t, tail)
	wantOps := []string{storage.OP_ENQUEUE, storage.OP_ENQUEUE, storage.OP_ENQUEUE, storage.OP_DEQUEUE}
	wantIDs := []string{"m-1", "m-2", "m-3", "m-2"}
	if len(recs) != len(wantOps) {
		t.Fatalf("returned %d records, want %d", len(recs), len(wantOps))
	}
	for i, rec := range recs {
		if rec.Op != wantOps[i] || rec.ID != wantIDs[i] {
			t.Fatalf("record %d = op %q id %q, want op %q id %q", i, rec.Op, rec.ID, wantOps[i], wantIDs[i])
		}
		if rec.Seq != uint64(i+1) {
			t.Fatalf("record %d seq = %d, want %d (records must come back in log order)", i, rec.Seq, i+1)
		}
	}
}

func TestTail_LimitKeepsNewestRecords(t *testing.T) {
	t.Parallel()
	w, _ := openWAL(t)

	for i := 1; i <= 5; i++ {
		if err := w.WriteEnqueue(mkMsg(fmt.Sprintf("m-%d", i), uint64(i), 0)); err != nil {
			t.Fatalf("WriteEnqueue m-%d: %v", i, err)
		}
	}

	tail, err := w.Tail(2)
	if err != nil {
		t.Fatalf("Tail: %v", err)
	}
	if !tail.Truncated {
		t.Fatal("truncated = false, want true when the limit cuts older records")
	}
	recs := decodeTail(t, tail)
	if len(recs) != 2 || recs[0].ID != "m-4" || recs[1].ID != "m-5" {
		t.Fatalf("records = %+v, want the two newest (m-4 then m-5)", recs)
	}
}

// TestTail_SkipsTornFinalLine: a torn trailing record — the signature of a
// crash mid-append — must not break the live inspection path. Tail skips
// what it cannot parse; deciding whether corruption is fatal is Recover's
// job, not Tail's.
func TestTail_SkipsTornFinalLine(t *testing.T) {
	t.Parallel()
	w, path := openWAL(t)

	for i := 1; i <= 2; i++ {
		if err := w.WriteEnqueue(mkMsg(fmt.Sprintf("m-%d", i), uint64(i), 0)); err != nil {
			t.Fatalf("WriteEnqueue m-%d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open for torn append: %v", err)
	}
	if _, err := f.Write([]byte(`{"v":1,"op":"ENQUEUE","seq":99,"id":"m-torn","payload":"partial`)); err != nil {
		t.Fatalf("writing torn tail: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close torn append: %v", err)
	}

	tail, err := reopenWAL(t, path).Tail(10)
	if err != nil {
		t.Fatalf("Tail with torn tail: unexpected error: %v", err)
	}
	recs := decodeTail(t, tail)
	if len(recs) != 2 || recs[0].ID != "m-1" || recs[1].ID != "m-2" {
		t.Fatalf("records = %+v, want just m-1 and m-2 (the torn line must be skipped)", recs)
	}
}

func TestTail_EmptyLog(t *testing.T) {
	t.Parallel()
	w, _ := openWAL(t)

	tail, err := w.Tail(10)
	if err != nil {
		t.Fatalf("Tail: %v", err)
	}
	if tail.Records == nil {
		t.Fatal("records is nil, want empty non-nil so the JSON shape stays an array")
	}
	if len(tail.Records) != 0 || tail.SizeBytes != 0 || tail.Truncated {
		t.Fatalf("tail = %+v, want zero records, zero size, not truncated", tail)
	}
}

func TestTail_ClosedReturnsErrClosed(t *testing.T) {
	t.Parallel()
	w, _ := openWAL(t)
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := w.Tail(10); !errors.Is(err, storage.ErrClosed) {
		t.Fatalf("Tail after Close: error = %v, want ErrClosed", err)
	}
}

// TestTail_ConcurrentWithWrites: readers polling the tail while writers
// append must never see an error, a torn record, or a data race. Tail and
// the write path share the WAL mutex, so a record is either fully visible
// or not yet visible — this is what lets the console poll GET /wal while
// producers are live. Run with -race (SKILLS.md §1.3.6).
func TestTail_ConcurrentWithWrites(t *testing.T) {
	t.Parallel()
	w, _ := openWAL(t)

	const (
		writers          = 8
		readers          = 4
		recordsPerWriter = 25
	)
	errCh := make(chan error, writers+readers)
	stop := make(chan struct{})

	var writersWG sync.WaitGroup
	for g := 0; g < writers; g++ {
		writersWG.Add(1)
		go func(g int) {
			defer writersWG.Done()
			for i := 0; i < recordsPerWriter; i++ {
				id := fmt.Sprintf("w%d-%d", g, i)
				if err := w.WriteEnqueue(mkMsg(id, uint64(g*recordsPerWriter+i+1), 0)); err != nil {
					errCh <- fmt.Errorf("WriteEnqueue %s: %w", id, err)
					return
				}
			}
		}(g)
	}

	var readersWG sync.WaitGroup
	for r := 0; r < readers; r++ {
		readersWG.Add(1)
		go func() {
			defer readersWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				tail, err := w.Tail(50)
				if err != nil {
					errCh <- fmt.Errorf("concurrent Tail: %w", err)
					return
				}
				for _, raw := range tail.Records {
					var rec tailRecord
					if err := json.Unmarshal(raw, &rec); err != nil {
						errCh <- fmt.Errorf("concurrent Tail returned unparseable record %q: %w", raw, err)
						return
					}
				}
			}
		}()
	}

	writersWG.Wait()
	close(stop)
	readersWG.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}

	tail, err := w.Tail(0)
	if err != nil {
		t.Fatalf("final Tail: %v", err)
	}
	if len(tail.Records) != writers*recordsPerWriter {
		t.Fatalf("final record count = %d, want %d (no record may be lost or doubled)", len(tail.Records), writers*recordsPerWriter)
	}
}
