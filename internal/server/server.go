package server

import "net/http"

type Server struct {
	scheduler Scheduler
}

func New(sch Scheduler) *Server {
	return &Server{
		scheduler: sch,
	}
}

func (s *Server) Register(mux *http.ServeMux) {
	mux.HandleFunc("/", s.handleProxy)
}

func (s *Server) handleProxy(w http.ResponseWriter, r *http.Request) {

	b := s.scheduler.Next()

	if b == nil {
		http.Error(w, "no healthy backen", http.StatusServiceUnavailable)
		return
	}

	b.IncInFlight()
	defer b.DecInFlight()

	b.Proxy.ServeHTTP(w, r)
}
