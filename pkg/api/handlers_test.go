package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
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
			var counts StatsResponse
			decodeJSON(t, stats, &counts)
			if counts != (StatsResponse{Total: 2, Ready: 1, Delayed: 1}) {
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
	var got StatsResponse
	decodeJSON(t, rec, &got)
	if got != (StatsResponse{}) {
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
	var got StatsResponse
	decodeJSON(t, rec, &got)
	want := StatsResponse{Total: 2, Ready: 1, Delayed: 1}
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
	var got StatsResponse
	decodeJSON(t, rec, &got)
	if got != (StatsResponse{}) {
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
	var got StatsResponse
	decodeJSON(t, rec, &got)
	if got.Total != n || got.Ready != n || got.Delayed != 0 {
		t.Fatalf("stats = %+v, want total=%d ready=%d delayed=0", got, n, n)
	}
}

type fakeQueue struct {
	enqueueFn func(model.EnqueueRequest) (model.Message, error)
	dequeueFn func() (*model.Message, error)
	statsFn   func() model.QueueStats
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
