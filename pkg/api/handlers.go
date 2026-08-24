// Package api implements the HTTP handlers for Queuemaxxing: POST /enqueue,
// POST|GET /dequeue, GET /messages, GET /wal, GET /stats, GET /health, and
// the embedded web console at GET /.
//
// Handlers perform routing, JSON (de)serialization, request validation,
// and HTTP status mapping. Business logic lives in the service layer.
package api

import (
	_ "embed"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"

	"queuemaxxing/pkg/model"
	"queuemaxxing/pkg/service"
)

const (
	maxEnqueueBodyBytes = 1 << 20

	// defaultMessageLimit caps GET /messages when the caller does not ask
	// for a specific size. Snapshot sorts what it returns, so an unbounded
	// default would make a routine console refresh O(n log n) over the whole
	// queue.
	defaultMessageLimit = 100

	// maxMessageLimit is the largest accepted ?limit= value.
	maxMessageLimit = 1000

	// defaultWalLimit caps GET /wal when the caller does not ask for a
	// specific size. The console polls this endpoint, so the default keeps
	// a routine refresh small.
	defaultWalLimit = 50

	// maxWalLimit is the largest accepted GET /wal ?limit= value.
	maxWalLimit = 500
)

// consoleHTML is the single-page web console, compiled into the binary by
// go:embed. Keeping the UI inside the executable is deliberate: Queuemaxxing
// ships as one file with no runtime dependencies, and a console that needed
// a separate asset directory or a build step would undo that.
//
//go:embed ui/console.html
var consoleHTML []byte

// Queue is the service surface the HTTP handlers depend on.
// *service.QueueService implements this interface.
type Queue interface {
	Enqueue(req model.EnqueueRequest) (model.Message, error)
	Dequeue() (*model.Message, error)
	Stats() model.QueueStats
	Snapshot(limit int) model.QueueSnapshot
	WalTail(limit int) (model.WalTail, error)
}

// Handler routes HTTP requests to the queue service. It implements
// http.Handler.
type Handler struct {
	queue Queue
	mux   http.Handler
}

// NewHandler wires routes and recovery middleware around q.
func NewHandler(q Queue) *Handler {
	h := &Handler{queue: q}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /enqueue", h.enqueue)
	mux.HandleFunc("POST /dequeue", h.dequeue)
	mux.HandleFunc("GET /dequeue", h.dequeue)
	mux.HandleFunc("GET /messages", h.messages)
	mux.HandleFunc("GET /wal", h.wal)
	mux.HandleFunc("GET /stats", h.stats)
	mux.HandleFunc("GET /health", h.health)
	// "/{$}" matches the root path exactly. A bare "/" would match every
	// unrouted path and turn 404s into the console page.
	mux.HandleFunc("GET /{$}", h.console)
	h.mux = recoverMiddleware(mux)
	return h
}

// ServeHTTP dispatches to the configured routes.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

// HealthResponse is the JSON body of GET /health.
type HealthResponse struct {
	Status string `json:"status"`
}

// ErrorResponse is the JSON body returned for 4xx/5xx errors.
type ErrorResponse struct {
	Error string `json:"error"`
}

func (h *Handler) enqueue(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxEnqueueBodyBytes)
	defer r.Body.Close()

	var req model.EnqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if err := req.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	msg, err := h.queue.Enqueue(req)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, msg)
}

func (h *Handler) dequeue(w http.ResponseWriter, r *http.Request) {
	msg, err := h.queue.Dequeue()
	if err != nil {
		writeServiceError(w, err)
		return
	}
	if msg == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeJSON(w, http.StatusOK, msg)
}

// messages returns a non-destructive snapshot of queue contents. It is the
// read-only counterpart to dequeue: the console polls it to render both
// heaps without consuming anything.
func (h *Handler) messages(w http.ResponseWriter, r *http.Request) {
	limit := defaultMessageLimit
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > maxMessageLimit {
			writeError(w, http.StatusBadRequest, "limit must be an integer between 1 and "+strconv.Itoa(maxMessageLimit))
			return
		}
		limit = n
	}
	writeJSON(w, http.StatusOK, h.queue.Snapshot(limit))
}

// wal returns a read-only tail of the write-ahead log: the newest records
// exactly as they were fsynced to disk. It is what the console's durability
// panel polls, so a tester can watch each enqueue and dequeue land in the
// log before its HTTP response was even sent.
func (h *Handler) wal(w http.ResponseWriter, r *http.Request) {
	limit := defaultWalLimit
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > maxWalLimit {
			writeError(w, http.StatusBadRequest, "limit must be an integer between 1 and "+strconv.Itoa(maxWalLimit))
			return
		}
		limit = n
	}
	tail, err := h.queue.WalTail(limit)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, tail)
}

// console serves the embedded single-page web console.
func (h *Handler) console(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(consoleHTML); err != nil {
		log.Printf("writing console page: %v", err)
	}
}

// stats returns the occupancy counts. model.QueueStats is the wire shape
// directly: an api-local DTO would be a field-for-field copy whose only
// effect is a second place for the JSON names to drift.
func (h *Handler) stats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, h.queue.Stats())
}

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, HealthResponse{Status: "healthy"})
}

func writeServiceError(w http.ResponseWriter, err error) {
	if errors.Is(err, service.ErrClosed) {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeError(w, http.StatusInternalServerError, err.Error())
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, ErrorResponse{Error: msg})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	body, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		if _, werr := w.Write([]byte("{\n  \"error\": \"failed to encode response\"\n}\n")); werr != nil {
			log.Printf("writing encode-failure response: %v", werr)
		}
		return
	}
	body = append(body, '\n')
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(body); err != nil {
		log.Printf("writing JSON response: %v", err)
	}
}

// recoverMiddleware is a last-resort safety net. A recovered panic is a
// bug to fix, not a feature to rely on (SKILLS.md §4.2).
func recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("panic recovered: %v", rec)
				writeError(w, http.StatusInternalServerError, "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}
