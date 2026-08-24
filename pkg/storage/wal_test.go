package storage_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
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
