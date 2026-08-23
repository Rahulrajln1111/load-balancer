package server

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"load-balancer/internal/backend"
	"load-balancer/internal/proxy"
	"log"
	"net"
	"net/http"
	"sync/atomic"
)

// LB-level counters (spec: Total/Success/Failed/BackendErrors).
type LBMetrics struct {
	Total         atomic.Uint64
	Success       atomic.Uint64
	Failed        atomic.Uint64
	BackendErrors atomic.Uint64
}

type Server struct {
	scheduler Scheduler
	backends  []*backend.Backend
	metrics   LBMetrics
}

type attemptW struct {
	http.ResponseWriter
	wrote bool
}

func (a *attemptW) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := a.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("hijacker not supported")
	}
	a.wrote = true
	return h.Hijack()
}

func (a *attemptW) Flush() {
	if f, ok := a.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
func (a *attemptW) WriteHeader(code int) { a.wrote = true; a.ResponseWriter.WriteHeader(code) }
func (a *attemptW) Write(b []byte) (int, error) {
	a.wrote = true
	return a.ResponseWriter.Write(b)
}

func New(sch Scheduler, backends []*backend.Backend) *Server {
	return &Server{
		scheduler: sch,
		backends:  backends,
	}
}

func (s *Server) Register(mux *http.ServeMux) {
	mux.HandleFunc("/lb/health", s.handleLBHealth)
	mux.HandleFunc("/lb/status", s.handleStats)
	mux.HandleFunc("/lb/metrics", s.handleMetrics)
	mux.HandleFunc("/stats", s.handleStats)
	mux.HandleFunc("/", s.handleProxy)
}

func (s *Server) handleLBHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte("ok"))
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]uint64{
		"total":          s.metrics.Total.Load(),
		"success":        s.metrics.Success.Load(),
		"failed":         s.metrics.Failed.Load(),
		"backend_errors": s.metrics.BackendErrors.Load(),
	})
}

func (s *Server) handleProxy(w http.ResponseWriter, r *http.Request) {

	s.metrics.Total.Add(1)

	maxAttempt := 1

	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		maxAttempt = 2
	}

	holder := &proxy.ErrHolder{}

	r = r.WithContext(context.WithValue(r.Context(), proxy.CtxKey(), holder))

	for i := 0; i < maxAttempt; i++ {
		b := s.scheduler.Next()

		if b == nil {
			s.metrics.Failed.Add(1)
			http.Error(w, "no healthy backend available", http.StatusServiceUnavailable)
			return
		}

		holder.Err = nil

		b.IncInFlight()
		b.RecordRequests()
		aw := &attemptW{ResponseWriter: w}

		func() {
			defer b.DecInFlight()
			b.Proxy.ServeHTTP(aw, r)
		}()

		if holder.Err != nil {
			s.metrics.BackendErrors.Add(1)
		}

		if holder.Err == nil || aw.wrote {
			s.metrics.Success.Add(1)
			return
		}

		log.Printf("retrying %s: %v", b.URL, holder.Err)

	}
	s.metrics.Failed.Add(1)
	http.Error(w, "backend unavailable", http.StatusBadGateway)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	type row struct {
		URL      string `json:"url"`
		Alive    bool   `json:"alive"`
		InFlight int64  `json:"in_flight"`
		Requests uint64 `json:"requests_total"`
	}
	out := make([]row, 0, len(s.backends))
	for _, b := range s.backends {
		out = append(out, row{b.URL.String(), b.IsAlive(), b.ActiveRequest(), b.TotalRequests()})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}
