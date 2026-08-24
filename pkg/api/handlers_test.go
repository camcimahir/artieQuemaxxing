package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"queuemaxxing/pkg/model"
	"queuemaxxing/pkg/service"
)

var modes = []model.QueueMode{model.ModeFIFO, model.ModeLIFO}

func newTestHandler(t *testing.T, mode model.QueueMode) (*Handler, *service.QueueService) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "queue.wal")
	svc, err := service.NewQueueService(path, mode)
	if err != nil {
		t.Fatalf("NewQueueService: %v", err)
	}
	t.Cleanup(func() { _ = svc.Close() })
	return NewHandler(svc), svc
}

func doRequest(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, r)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decodeJSON(t *testing.T, rec *httptest.ResponseRecorder, dest any) {
	t.Helper()
	ct := rec.Header().Get("Content-Type")
	if !strings.Contains(ct, "application/json") {
		t.Fatalf("Content-Type = %q, want application/json; body=%s", ct, rec.Body.Bytes())
	}
	if err := json.Unmarshal(rec.Body.Bytes(), dest); err != nil {
		t.Fatalf("decode JSON: %v; body=%s", err, rec.Body.Bytes())
	}
}

func TestHealth_ReturnsHealthy(t *testing.T) {
	t.Parallel()
	h, _ := newTestHandler(t, model.ModeFIFO)

	rec := doRequest(h, http.MethodGet, "/health", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.Bytes())
	}
	var got HealthResponse
	decodeJSON(t, rec, &got)
	if got.Status != "healthy" {
		t.Fatalf("status field = %q, want healthy", got.Status)
	}
	body := rec.Body.String()
	if !strings.HasSuffix(body, "\n") {
		t.Fatal("JSON body must end with a newline so a shell prompt stays on its own line")
	}
	if !strings.Contains(body, "\n  ") {
		t.Fatalf("JSON body is not indented; got %q", body)
	}
}

func TestEnqueue_CreatedWithMessageObject(t *testing.T) {
	t.Parallel()
	h, _ := newTestHandler(t, model.ModeFIFO)

	rec := doRequest(h, http.MethodPost, "/enqueue", `{"payload":"hello","priority":5,"delay_seconds":0}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", rec.Code, rec.Body.Bytes())
	}
	var msg model.Message
	decodeJSON(t, rec, &msg)
	if msg.ID == "" {
		t.Fatal("expected non-empty id")
	}
	if msg.Seq == 0 {
		t.Fatal("expected non-zero seq")
	}
	if msg.Payload != "hello" {
		t.Fatalf("payload = %q, want hello", msg.Payload)
	}
	if msg.Priority != 5 {
		t.Fatalf("priority = %d, want 5", msg.Priority)
	}
	if msg.Status != model.StatusPending {
		t.Fatalf("status = %q, want PENDING", msg.Status)
	}
	if msg.EnqueuedAt.IsZero() || msg.AvailableAt.IsZero() {
		t.Fatalf("expected timestamps, got enqueued_at=%v available_at=%v", msg.EnqueuedAt, msg.AvailableAt)
	}
}

func TestEnqueue_RejectsInvalidJSON(t *testing.T) {
	t.Parallel()
	h, _ := newTestHandler(t, model.ModeFIFO)

	rec := doRequest(h, http.MethodPost, "/enqueue", `{not json`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.Bytes())
	}
	var got ErrorResponse
	decodeJSON(t, rec, &got)
	if got.Error == "" {
		t.Fatal("expected error field")
	}
}

func TestEnqueue_RejectsEmptyPayloadAndNegativeDelay(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		body string
	}{
		{name: "empty payload", body: `{"payload":""}`},
		{name: "missing payload", body: `{"priority":1}`},
		{name: "negative delay", body: `{"payload":"x","delay_seconds":-1}`},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newTestHandler(t, model.ModeFIFO)
			rec := doRequest(h, http.MethodPost, "/enqueue", tc.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.Bytes())
			}
			var got ErrorResponse
			decodeJSON(t, rec, &got)
			if got.Error == "" {
				t.Fatal("expected error field")
			}
		})
	}
}

func TestEnqueue_WrongMethod(t *testing.T) {
	t.Parallel()
	h, _ := newTestHandler(t, model.ModeFIFO)

	rec := doRequest(h, http.MethodGet, "/enqueue", "")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405; body=%s", rec.Code, rec.Body.Bytes())
	}
	if allow := rec.Header().Get("Allow"); !strings.Contains(allow, http.MethodPost) {
		t.Fatalf("Allow = %q, want it to list POST (README: routing 405s carry Allow)", allow)
	}
}

func TestDequeue_EmptyReturnsNoContent(t *testing.T) {
	t.Parallel()
	for _, method := range []string{http.MethodPost, http.MethodGet} {
		method := method
		t.Run(method, func(t *testing.T) {
			t.Parallel()
			h, _ := newTestHandler(t, model.ModeFIFO)
			rec := doRequest(h, method, "/dequeue", "")
			if rec.Code != http.StatusNoContent {
				t.Fatalf("status = %d, want 204; body=%s", rec.Code, rec.Body.Bytes())
			}
			if rec.Body.Len() != 0 {
				t.Fatalf("expected empty body, got %s", rec.Body.Bytes())
			}
		})
	}
}

func TestDequeue_DelayedHigherPriorityStaysInvisible(t *testing.T) {
	t.Parallel()
	for _, mode := range modes {
		mode := mode
		t.Run(string(mode), func(t *testing.T) {
			t.Parallel()
			h, _ := newTestHandler(t, mode)

			if rec := doRequest(h, http.MethodPost, "/enqueue", `{"payload":"low-pri","priority":1,"delay_seconds":0}`); rec.Code != http.StatusCreated {
				t.Fatalf("enqueue low-pri: status = %d; body=%s", rec.Code, rec.Body.Bytes())
			}
			if rec := doRequest(h, http.MethodPost, "/enqueue", `{"payload":"high-pri-delayed","priority":10,"delay_seconds":3600}`); rec.Code != http.StatusCreated {
				t.Fatalf("enqueue high-pri-delayed: status = %d; body=%s", rec.Code, rec.Body.Bytes())
			}

			stats := doRequest(h, http.MethodGet, "/stats", "")
			var counts model.QueueStats
			decodeJSON(t, stats, &counts)
			if counts != (model.QueueStats{Total: 2, Available: 1, Delayed: 1}) {
				t.Fatalf("stats before delay elapsed = %+v, want total=2 ready=1 delayed=1", counts)
			}

			rec := doRequest(h, http.MethodPost, "/dequeue", "")
			if rec.Code != http.StatusOK {
				t.Fatalf("dequeue status = %d, want 200; body=%s", rec.Code, rec.Body.Bytes())
			}
			var got model.Message
			decodeJSON(t, rec, &got)
			if got.Payload != "low-pri" {
				t.Fatalf("dequeued payload = %q, want low-pri (delayed high-pri must stay invisible)", got.Payload)
			}
		})
	}
}

func TestDequeue_ReturnsEnqueuedMessage(t *testing.T) {
	t.Parallel()
	for _, mode := range modes {
		mode := mode
		t.Run(string(mode), func(t *testing.T) {
			t.Parallel()
			h, _ := newTestHandler(t, mode)

			created := doRequest(h, http.MethodPost, "/enqueue", `{"payload":"job","priority":3}`)
			if created.Code != http.StatusCreated {
				t.Fatalf("enqueue status = %d, want 201; body=%s", created.Code, created.Body.Bytes())
			}
			var enqueued model.Message
			decodeJSON(t, created, &enqueued)

			rec := doRequest(h, http.MethodPost, "/dequeue", "")
			if rec.Code != http.StatusOK {
				t.Fatalf("dequeue status = %d, want 200; body=%s", rec.Code, rec.Body.Bytes())
			}
			var got model.Message
			decodeJSON(t, rec, &got)
			if got.ID != enqueued.ID || got.Payload != "job" || got.Priority != 3 {
				t.Fatalf("dequeued %+v, want id=%s payload=job priority=3", got, enqueued.ID)
			}
		})
	}
}

func TestDequeue_PriorityThenModeTieBreak(t *testing.T) {
	t.Parallel()
	for _, mode := range modes {
		mode := mode
		t.Run(string(mode), func(t *testing.T) {
			t.Parallel()
			h, _ := newTestHandler(t, mode)
			for _, body := range []string{
				`{"payload":"a","priority":1}`,
				`{"payload":"b","priority":1}`,
				`{"payload":"hi","priority":9}`,
			} {
				rec := doRequest(h, http.MethodPost, "/enqueue", body)
				if rec.Code != http.StatusCreated {
					t.Fatalf("enqueue %s: status = %d; body=%s", body, rec.Code, rec.Body.Bytes())
				}
			}

			first := doRequest(h, http.MethodGet, "/dequeue", "")
			var msg model.Message
			decodeJSON(t, first, &msg)
			if msg.Payload != "hi" {
				t.Fatalf("first dequeue payload = %q, want hi", msg.Payload)
			}

			second := doRequest(h, http.MethodPost, "/dequeue", "")
			decodeJSON(t, second, &msg)
			want := "a"
			if mode == model.ModeLIFO {
				want = "b"
			}
			if msg.Payload != want {
				t.Fatalf("second dequeue payload = %q, want %s (%s)", msg.Payload, want, mode)
			}
		})
	}
}

func TestStats_EmptyQueue(t *testing.T) {
	t.Parallel()
	h, _ := newTestHandler(t, model.ModeFIFO)

	rec := doRequest(h, http.MethodGet, "/stats", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.Bytes())
	}
	var got model.QueueStats
	decodeJSON(t, rec, &got)
	if got != (model.QueueStats{}) {
		t.Fatalf("stats = %+v, want zeros", got)
	}
}

func TestStats_CountsReadyAndDelayed(t *testing.T) {
	t.Parallel()
	h, _ := newTestHandler(t, model.ModeFIFO)

	if rec := doRequest(h, http.MethodPost, "/enqueue", `{"payload":"later","delay_seconds":3600}`); rec.Code != http.StatusCreated {
		t.Fatalf("enqueue delayed: status = %d; body=%s", rec.Code, rec.Body.Bytes())
	}
	if rec := doRequest(h, http.MethodPost, "/enqueue", `{"payload":"now"}`); rec.Code != http.StatusCreated {
		t.Fatalf("enqueue ready: status = %d; body=%s", rec.Code, rec.Body.Bytes())
	}

	rec := doRequest(h, http.MethodGet, "/stats", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.Bytes())
	}
	var got model.QueueStats
	decodeJSON(t, rec, &got)
	want := model.QueueStats{Total: 2, Available: 1, Delayed: 1}
	if got != want {
		t.Fatalf("stats = %+v, want %+v", got, want)
	}

	raw := map[string]json.Number{}
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode raw stats: %v", err)
	}
	for _, key := range []string{"total", "ready", "delayed"} {
		if _, ok := raw[key]; !ok {
			t.Fatalf("stats JSON missing %q: %s", key, rec.Body.Bytes())
		}
	}
}

func TestEnqueueDequeue_UpdatesStats(t *testing.T) {
	t.Parallel()
	h, _ := newTestHandler(t, model.ModeFIFO)

	if rec := doRequest(h, http.MethodPost, "/enqueue", `{"payload":"x"}`); rec.Code != http.StatusCreated {
		t.Fatalf("enqueue: status = %d; body=%s", rec.Code, rec.Body.Bytes())
	}
	if rec := doRequest(h, http.MethodPost, "/dequeue", ""); rec.Code != http.StatusOK {
		t.Fatalf("dequeue: status = %d; body=%s", rec.Code, rec.Body.Bytes())
	}

	rec := doRequest(h, http.MethodGet, "/stats", "")
	var got model.QueueStats
	decodeJSON(t, rec, &got)
	if got != (model.QueueStats{}) {
		t.Fatalf("stats after drain = %+v, want zeros", got)
	}
}

func TestEnqueue_ClosedServiceReturns503(t *testing.T) {
	t.Parallel()
	h, svc := newTestHandler(t, model.ModeFIFO)
	if err := svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	rec := doRequest(h, http.MethodPost, "/enqueue", `{"payload":"x"}`)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body=%s", rec.Code, rec.Body.Bytes())
	}
	var got ErrorResponse
	decodeJSON(t, rec, &got)
	if got.Error == "" {
		t.Fatal("expected error field")
	}
}

func TestDequeue_ClosedServiceReturns503(t *testing.T) {
	t.Parallel()
	h, svc := newTestHandler(t, model.ModeFIFO)
	if err := svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	rec := doRequest(h, http.MethodPost, "/dequeue", "")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body=%s", rec.Code, rec.Body.Bytes())
	}
}

func TestEnqueue_ServiceErrorReturns500(t *testing.T) {
	t.Parallel()
	h := NewHandler(&fakeQueue{
		enqueueFn: func(model.EnqueueRequest) (model.Message, error) {
			return model.Message{}, errors.New("wal sync failed")
		},
	})
	rec := doRequest(h, http.MethodPost, "/enqueue", `{"payload":"x"}`)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body=%s", rec.Code, rec.Body.Bytes())
	}
	var got ErrorResponse
	decodeJSON(t, rec, &got)
	if got.Error != "wal sync failed" {
		t.Fatalf("error = %q, want wal sync failed", got.Error)
	}
}

func TestDequeue_ServiceErrorReturns500(t *testing.T) {
	t.Parallel()
	h := NewHandler(&fakeQueue{
		dequeueFn: func() (*model.Message, error) {
			return nil, errors.New("wal sync failed")
		},
	})
	rec := doRequest(h, http.MethodPost, "/dequeue", "")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body=%s", rec.Code, rec.Body.Bytes())
	}
}

func TestUnknownPath_Returns404(t *testing.T) {
	t.Parallel()
	h, _ := newTestHandler(t, model.ModeFIFO)
	rec := doRequest(h, http.MethodGet, "/nope", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", rec.Code, rec.Body.Bytes())
	}
	if ct := rec.Header().Get("Content-Type"); strings.Contains(ct, "application/json") {
		t.Fatalf("404 Content-Type = %q; routing errors must stay net/http plain text, not JSON", ct)
	}
}

func TestRecoverMiddleware_PanicBecomes500(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /boom", func(http.ResponseWriter, *http.Request) {
		panic("boom")
	})
	h := recoverMiddleware(mux)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/boom", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body=%s", rec.Code, rec.Body.Bytes())
	}
	var got ErrorResponse
	decodeJSON(t, rec, &got)
	if got.Error != "internal server error" {
		t.Fatalf("error = %q, want internal server error", got.Error)
	}
}

func TestEnqueue_Concurrent(t *testing.T) {
	t.Parallel()
	h, _ := newTestHandler(t, model.ModeFIFO)

	const n = 32
	var wg sync.WaitGroup
	errCh := make(chan string, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			body := fmt.Sprintf(`{"payload":"p-%d"}`, i)
			rec := doRequest(h, http.MethodPost, "/enqueue", body)
			if rec.Code != http.StatusCreated {
				errCh <- fmt.Sprintf("enqueue %d: status = %d body=%s", i, rec.Code, rec.Body.Bytes())
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for msg := range errCh {
		t.Error(msg)
	}

	rec := doRequest(h, http.MethodGet, "/stats", "")
	var got model.QueueStats
	decodeJSON(t, rec, &got)
	if got.Total != n || got.Available != n || got.Delayed != 0 {
		t.Fatalf("stats = %+v, want total=%d ready=%d delayed=0", got, n, n)
	}
}

type fakeQueue struct {
	enqueueFn  func(model.EnqueueRequest) (model.Message, error)
	dequeueFn  func() (*model.Message, error)
	statsFn    func() model.QueueStats
	snapshotFn func(limit int) model.QueueSnapshot
	walTailFn  func(limit int) (model.WalTail, error)
}

func (f *fakeQueue) Enqueue(req model.EnqueueRequest) (model.Message, error) {
	if f.enqueueFn != nil {
		return f.enqueueFn(req)
	}
	return model.Message{}, nil
}

func (f *fakeQueue) Dequeue() (*model.Message, error) {
	if f.dequeueFn != nil {
		return f.dequeueFn()
	}
	return nil, nil
}

func (f *fakeQueue) Stats() model.QueueStats {
	if f.statsFn != nil {
		return f.statsFn()
	}
	return model.QueueStats{}
}

func (f *fakeQueue) Snapshot(limit int) model.QueueSnapshot {
	if f.snapshotFn != nil {
		return f.snapshotFn(limit)
	}
	return model.QueueSnapshot{}
}

func (f *fakeQueue) WalTail(limit int) (model.WalTail, error) {
	if f.walTailFn != nil {
		return f.walTailFn(limit)
	}
	return model.WalTail{}, nil
}

func TestMessages_ReturnsBothHeapsWithoutConsuming(t *testing.T) {
	t.Parallel()
	for _, mode := range modes {
		mode := mode
		t.Run(string(mode), func(t *testing.T) {
			t.Parallel()
			h, _ := newTestHandler(t, mode)

			for _, body := range []string{
				`{"payload":"low","priority":1}`,
				`{"payload":"mid-first","priority":5}`,
				`{"payload":"mid-second","priority":5}`,
				`{"payload":"urgent-later","priority":9,"delay_seconds":3600}`,
			} {
				if rec := doRequest(h, http.MethodPost, "/enqueue", body); rec.Code != http.StatusCreated {
					t.Fatalf("enqueue %s: status = %d; body=%s", body, rec.Code, rec.Body.Bytes())
				}
			}

			rec := doRequest(h, http.MethodGet, "/messages", "")
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.Bytes())
			}
			var snap model.QueueSnapshot
			decodeJSON(t, rec, &snap)

			if snap.Mode != mode {
				t.Fatalf("mode = %q, want %q", snap.Mode, mode)
			}
			if snap.Now.IsZero() {
				t.Fatal("now is zero; the console needs a server timestamp to render countdowns")
			}
			want := model.QueueStats{Total: 4, Available: 3, Delayed: 1}
			if snap.Stats != want {
				t.Fatalf("stats = %+v, want %+v", snap.Stats, want)
			}
			if len(snap.Delayed) != 1 || snap.Delayed[0].Payload != "urgent-later" {
				t.Fatalf("delayed = %+v, want just urgent-later", snap.Delayed)
			}

			wantReady := []string{"mid-first", "mid-second", "low"}
			if mode == model.ModeLIFO {
				wantReady = []string{"mid-second", "mid-first", "low"}
			}
			got := make([]string, len(snap.Ready))
			for i, m := range snap.Ready {
				got[i] = m.Payload
			}
			if !slices.Equal(got, wantReady) {
				t.Fatalf("ready order = %v, want %v", got, wantReady)
			}

			// GET /messages must be read-only: the queue is untouched and the
			// next dequeue still returns the head the snapshot advertised.
			if rec := doRequest(h, http.MethodGet, "/stats", ""); rec.Code == http.StatusOK {
				var st model.QueueStats
				decodeJSON(t, rec, &st)
				if st.Total != 4 {
					t.Fatalf("total = %d after GET /messages, want 4 (the snapshot must not consume)", st.Total)
				}
			}
			deq := doRequest(h, http.MethodPost, "/dequeue", "")
			var msg model.Message
			decodeJSON(t, deq, &msg)
			if msg.Payload != wantReady[0] {
				t.Fatalf("dequeued %q, but /messages advertised %q as next out", msg.Payload, wantReady[0])
			}
		})
	}
}

func TestMessages_LimitValidation(t *testing.T) {
	t.Parallel()
	h, _ := newTestHandler(t, model.ModeFIFO)
	for i := 0; i < 5; i++ {
		if rec := doRequest(h, http.MethodPost, "/enqueue", fmt.Sprintf(`{"payload":"p-%d"}`, i)); rec.Code != http.StatusCreated {
			t.Fatalf("enqueue %d: status = %d", i, rec.Code)
		}
	}

	rec := doRequest(h, http.MethodGet, "/messages?limit=2", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.Bytes())
	}
	var snap model.QueueSnapshot
	decodeJSON(t, rec, &snap)
	if len(snap.Ready) != 2 || !snap.ReadyTruncated {
		t.Fatalf("ready = %d entries (truncated=%v), want 2 and true", len(snap.Ready), snap.ReadyTruncated)
	}
	if snap.Stats.Total != 5 {
		t.Fatalf("stats.total = %d under limit=2, want 5", snap.Stats.Total)
	}

	for _, bad := range []string{"0", "-1", "abc", "1001"} {
		rec := doRequest(h, http.MethodGet, "/messages?limit="+bad, "")
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("limit=%s: status = %d, want 400; body=%s", bad, rec.Code, rec.Body.Bytes())
		}
	}
}

func TestConsole_ServedAtRootOnly(t *testing.T) {
	t.Parallel()
	h, _ := newTestHandler(t, model.ModeFIFO)

	rec := doRequest(h, http.MethodGet, "/", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /: status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Fatalf("GET /: Content-Type = %q, want text/html", ct)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "<!doctype html>") {
		t.Fatalf("GET / did not return the embedded console page; got %d bytes", len(body))
	}
	// The console is useless if its data sources are not wired up.
	if !strings.Contains(body, "/messages") {
		t.Fatal("console page does not reference GET /messages")
	}
	if !strings.Contains(body, "/wal") {
		t.Fatal("console page does not reference GET /wal (the durability panel)")
	}

	// "/{$}" must match the root exactly and must not swallow unknown paths.
	if rec := doRequest(h, http.MethodGet, "/not-a-route", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("GET /not-a-route: status = %d, want 404 (the console route must not match everything)", rec.Code)
	}
}

// TestWal_ReturnsFsyncedRecords: GET /wal must show one record per durable
// operation, in log order, without consuming anything — it is the console's
// window into the durability layer.
func TestWal_ReturnsFsyncedRecords(t *testing.T) {
	t.Parallel()
	h, _ := newTestHandler(t, model.ModeFIFO)

	if rec := doRequest(h, http.MethodPost, "/enqueue", `{"payload":"pay order 1842","priority":5}`); rec.Code != http.StatusCreated {
		t.Fatalf("enqueue: status = %d; body=%s", rec.Code, rec.Body.Bytes())
	}
	if rec := doRequest(h, http.MethodPost, "/enqueue", `{"payload":"retry webhook","priority":9,"delay_seconds":3600}`); rec.Code != http.StatusCreated {
		t.Fatalf("enqueue delayed: status = %d; body=%s", rec.Code, rec.Body.Bytes())
	}
	deq := doRequest(h, http.MethodPost, "/dequeue", "")
	if deq.Code != http.StatusOK {
		t.Fatalf("dequeue: status = %d; body=%s", deq.Code, deq.Body.Bytes())
	}
	var delivered model.Message
	decodeJSON(t, deq, &delivered)

	rec := doRequest(h, http.MethodGet, "/wal", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /wal: status = %d, want 200; body=%s", rec.Code, rec.Body.Bytes())
	}
	var tail model.WalTail
	decodeJSON(t, rec, &tail)
	if tail.Path == "" || tail.SizeBytes <= 0 {
		t.Fatalf("tail metadata = path %q size %d, want a real file", tail.Path, tail.SizeBytes)
	}
	if tail.Truncated {
		t.Fatal("truncated = true, want false for a three-record log under the default limit")
	}
	if len(tail.Records) != 3 {
		t.Fatalf("records = %d, want 3 (two enqueues + one tombstone)", len(tail.Records))
	}
	type walRec struct {
		Op      string `json:"op"`
		ID      string `json:"id"`
		Payload string `json:"payload"`
	}
	var recs []walRec
	for i, raw := range tail.Records {
		var r walRec
		if err := json.Unmarshal(raw, &r); err != nil {
			t.Fatalf("record %d is not valid JSON: %v; raw=%s", i, err, raw)
		}
		recs = append(recs, r)
	}
	if recs[0].Op != "ENQUEUE" || recs[0].Payload != "pay order 1842" {
		t.Fatalf("record 0 = %+v, want the first ENQUEUE", recs[0])
	}
	if recs[1].Op != "ENQUEUE" || recs[1].Payload != "retry webhook" {
		t.Fatalf("record 1 = %+v, want the second ENQUEUE", recs[1])
	}
	if recs[2].Op != "DEQUEUE" || recs[2].ID != delivered.ID {
		t.Fatalf("record 2 = %+v, want the tombstone for %q", recs[2], delivered.ID)
	}

	// Reading the log must not consume: the delayed message is still stored.
	stats := doRequest(h, http.MethodGet, "/stats", "")
	var counts model.QueueStats
	decodeJSON(t, stats, &counts)
	if counts.Total != 1 {
		t.Fatalf("total = %d after GET /wal, want 1 (reading the log must not consume)", counts.Total)
	}
}

func TestWal_EmptyLogReturnsJSONArray(t *testing.T) {
	t.Parallel()
	h, _ := newTestHandler(t, model.ModeFIFO)

	rec := doRequest(h, http.MethodGet, "/wal", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.Bytes())
	}
	raw := map[string]json.RawMessage{}
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode: %v; body=%s", err, rec.Body.Bytes())
	}
	if string(raw["records"]) != "[]" {
		t.Fatalf("records = %s, want [] (not null) on a fresh log", raw["records"])
	}
}

func TestWal_LimitValidation(t *testing.T) {
	t.Parallel()
	h, _ := newTestHandler(t, model.ModeFIFO)
	for i := 0; i < 5; i++ {
		if rec := doRequest(h, http.MethodPost, "/enqueue", fmt.Sprintf(`{"payload":"p-%d"}`, i)); rec.Code != http.StatusCreated {
			t.Fatalf("enqueue %d: status = %d", i, rec.Code)
		}
	}

	rec := doRequest(h, http.MethodGet, "/wal?limit=2", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.Bytes())
	}
	var tail model.WalTail
	decodeJSON(t, rec, &tail)
	if len(tail.Records) != 2 || !tail.Truncated {
		t.Fatalf("records = %d entries (truncated=%v), want 2 and true", len(tail.Records), tail.Truncated)
	}

	for _, bad := range []string{"0", "-1", "abc", "501"} {
		rec := doRequest(h, http.MethodGet, "/wal?limit="+bad, "")
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("limit=%s: status = %d, want 400; body=%s", bad, rec.Code, rec.Body.Bytes())
		}
	}
}

func TestWal_ClosedServiceReturns503(t *testing.T) {
	t.Parallel()
	h, svc := newTestHandler(t, model.ModeFIFO)
	if err := svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	rec := doRequest(h, http.MethodGet, "/wal", "")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body=%s", rec.Code, rec.Body.Bytes())
	}
}

func TestEnqueue_RejectsDelayBeyondCap(t *testing.T) {
	t.Parallel()
	h, _ := newTestHandler(t, model.ModeFIFO)

	// 1e10 seconds overflows int64 nanoseconds when multiplied by
	// time.Second, which used to yield an AvailableAt in the past.
	rec := doRequest(h, http.MethodPost, "/enqueue", `{"payload":"x","delay_seconds":10000000000}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.Bytes())
	}
	var got ErrorResponse
	decodeJSON(t, rec, &got)
	if !strings.Contains(got.Error, "delay_seconds") {
		t.Fatalf("error = %q, want it to name delay_seconds", got.Error)
	}
	if rec := doRequest(h, http.MethodGet, "/stats", ""); rec.Code == http.StatusOK {
		var st model.QueueStats
		decodeJSON(t, rec, &st)
		if st.Total != 0 {
			t.Fatalf("total = %d, want 0 (a rejected enqueue must not reach the queue)", st.Total)
		}
	}
}

func TestEnqueue_RejectsOversizedBody(t *testing.T) {
	t.Parallel()
	h, _ := newTestHandler(t, model.ModeFIFO)

	body := `{"payload":"` + strings.Repeat("a", maxEnqueueBodyBytes) + `"}`
	rec := doRequest(h, http.MethodPost, "/enqueue", body)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413; body=%s", rec.Code, rec.Body.Bytes())
	}
	var got ErrorResponse
	decodeJSON(t, rec, &got)
	if got.Error == "" {
		t.Fatal("expected error field")
	}
}

func TestMessages_EmptyQueueJSONArrays(t *testing.T) {
	t.Parallel()
	h, _ := newTestHandler(t, model.ModeFIFO)

	rec := doRequest(h, http.MethodGet, "/messages", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.Bytes())
	}
	raw := map[string]json.RawMessage{}
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode: %v; body=%s", err, rec.Body.Bytes())
	}
	for _, key := range []string{"ready", "delayed"} {
		if string(raw[key]) != "[]" {
			t.Fatalf("%s = %s, want [] (not null) so the documented array shape holds on an empty queue", key, raw[key])
		}
	}
}
