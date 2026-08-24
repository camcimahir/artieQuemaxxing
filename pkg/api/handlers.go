// Package api implements the HTTP handlers for Queuemaxxing:
// POST /enqueue, POST|GET /dequeue, GET /stats, and GET /health.
//
// Handlers perform routing, JSON (de)serialization, request validation,
// and HTTP status mapping. Business logic lives in the service layer.
package api

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"queuemaxxing/pkg/model"
	"queuemaxxing/pkg/service"
)

const maxEnqueueBodyBytes = 1 << 20

// Queue is the service surface the HTTP handlers depend on.
// *service.QueueService implements this interface.
type Queue interface {
	Enqueue(req model.EnqueueRequest) (model.Message, error)
	Dequeue() (*model.Message, error)
	Stats() model.QueueStats
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
	mux.HandleFunc("GET /stats", h.stats)
	mux.HandleFunc("GET /health", h.health)
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

// StatsResponse is the JSON body of GET /stats.
type StatsResponse struct {
	Total   int `json:"total"`
	Ready   int `json:"ready"`
	Delayed int `json:"delayed"`
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

func (h *Handler) stats(w http.ResponseWriter, r *http.Request) {
	s := h.queue.Stats()
	writeJSON(w, http.StatusOK, StatsResponse{
		Total:   s.Total,
		Ready:   s.Available,
		Delayed: s.Delayed,
	})
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
