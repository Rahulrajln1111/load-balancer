package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"load-balancer/internal/backend"
	"load-balancer/internal/proxy"
	"log"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

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
	mux.HandleFunc("/message", s.handleMessage)
	mux.HandleFunc("/feed", s.handleFeed)
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

func (s *Server) handleMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.forwardToBackend(w, r)
}

func (s *Server) handleFeed(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.forwardToBackend(w, r)
}

func (s *Server) forwardToBackend(w http.ResponseWriter, r *http.Request) {
	s.metrics.Total.Add(1)

	// WebSocket upgrades must NOT count as in-flight requests or contribute
	// to latency metrics: a WS session lives for minutes, so recording its
	// duration as a "response time" poisons the scheduler's EWMA and pushes
	// the backend out of rotation (load score 1030 vs 900 threshold).
	isWS := strings.EqualFold(r.Header.Get("Upgrade"), "websocket")

	// Buffered-body retry: capture the full request body so failed attempts
	// can be replayed byte-identical to the next backend. Only /message is
	// retried — it is idempotent server-side (duplicate message IDs are
	// absorbed by ON CONFLICT DO NOTHING), so a retry can never create a
	// duplicate row. Turns a backend death from client-visible 502s into an
	// invisible failover.
	retryable := r.Method == http.MethodPost && r.URL.Path == "/message"
	var body []byte
	if retryable && r.Body != nil {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		body = b
		// Do NOT close the original body here: ReverseProxy's transport
		// reads r.Body on every attempt, and the http server closes it
		// when the handler returns. Each attempt gets a fresh reader below.
	}

	holder := &proxy.ErrHolder{}
	r = r.WithContext(context.WithValue(r.Context(), proxy.CtxKey(), holder))

	for attempt := 1; ; attempt++ {
		b := s.scheduler.Next()
		if b == nil {
			s.metrics.Failed.Add(1)
			http.Error(w, "no healthy backend available", http.StatusServiceUnavailable)
			return
		}
		if retryable && body != nil {
			// Fresh reader per attempt: ReverseProxy closes the body after
			// proxying, so a replay must never reuse a consumed/closed one.
			r.Body = io.NopCloser(bytes.NewReader(body))
		}

		holder.Err = nil
		if !isWS {
			b.IncInFlight()
		}
		b.RecordRequests()
		aw := &attemptW{ResponseWriter: w}

		start := time.Now()
		func() {
			if !isWS {
				defer b.DecInFlight()
			}
			b.Proxy.ServeHTTP(aw, r)
		}()
		if !isWS {
			b.RecordResponseTime(time.Since(start))
		}

		if holder.Err != nil {
			s.metrics.BackendErrors.Add(1)
		}

		if holder.Err == nil || aw.wrote {
			s.metrics.Success.Add(1)
			return
		}

		// Backend proved dead mid-flight: trip it now so the retry (and
		// every other in-flight request) skips it immediately.
		if isDeadBackendErr(holder.Err) {
			b.SetAlive(false)
		}

		if attempt >= 2 || !retryable {
			s.metrics.Failed.Add(1)
			http.Error(w, "backend unavailable", http.StatusBadGateway)
			return
		}
		log.Printf("retrying %s: %v", b.URL, holder.Err)
	}
}

// isDeadBackendErr reports whether a proxy error proves the backend is
// unreachable (connection refused/reset, host down, no response). Used to
// trip the backend's alive flag INSTANTLY instead of waiting for the 3-fail
// health-check threshold — otherwise the scheduler keeps handing out the
// corpse for seconds and clients see 502s.
func isDeadBackendErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EHOSTUNREACH) || errors.Is(err, syscall.ENETUNREACH) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func (s *Server) handleProxy(w http.ResponseWriter, r *http.Request) {
	s.metrics.Total.Add(1)

	maxAttempt := 1

	// Only retry read-only methods: POST/PUT/DELETE hit non-idempotent app
	// routes (room creation, messages) where a blind retry would duplicate
	// side effects. POST /message retries live in forwardToBackend with
	// body replay (idempotent there: ON CONFLICT DO NOTHING).
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

		start := time.Now()
		func() {
			defer b.DecInFlight()
			b.Proxy.ServeHTTP(aw, r)
		}()
		elapsed := time.Since(start)
		b.RecordResponseTime(elapsed)

		if holder.Err != nil {
			s.metrics.BackendErrors.Add(1)
		}

		if holder.Err == nil || aw.wrote {
			s.metrics.Success.Add(1)
			return
		}

		// Backend proved dead mid-flight: trip it now so the retry below
		// (and every other in-flight request) skips it immediately.
		if isDeadBackendErr(holder.Err) {
			b.SetAlive(false)
		}

		log.Printf("retrying %s: %v", b.URL, holder.Err)
	}
	s.metrics.Failed.Add(1)
	http.Error(w, "backend unavailable", http.StatusBadGateway)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	type row struct {
		URL             string `json:"url"`
		Alive           bool   `json:"alive"`
		InFlight        int64  `json:"in_flight"`
		Requests        uint64 `json:"requests_total"`
		AvgResponseTime int64  `json:"avg_response_time_ms"`
		LoadScore       int64  `json:"load_score"`
	}
	out := make([]row, 0, len(s.backends))
	for _, b := range s.backends {
		out = append(out, row{
			URL:             b.URL.String(),
			Alive:           b.IsAlive(),
			InFlight:        b.ActiveRequest(),
			Requests:        b.TotalRequests(),
			AvgResponseTime: b.AvgResponseTime(),
			LoadScore:       b.LoadScore(),
		})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}
