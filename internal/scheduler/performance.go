package scheduler

import (
	"load-balancer/internal/backend"
	"math/rand"
	"sync/atomic"
)

// Threshold tuned for 2700 concurrent requests across backends
// Each backend can handle ~900 concurrent connections comfortably
const loadThreshold = 900

type PerformanceScheduler struct {
	backends  []*backend.Backend
	threshold atomic.Int64
	maxConns  atomic.Int64 // Global max concurrent connections
}

func NewPerformanceScheduler(backends []*backend.Backend) *PerformanceScheduler {
	s := &PerformanceScheduler{
		backends: backends,
	}
	s.threshold.Store(loadThreshold)
	s.maxConns.Store(2700) // Hard cap for 2700 concurrent requests
	return s
}

func (s *PerformanceScheduler) Next() *backend.Backend {
	// Check global connection limit first
	globalInFlight := int64(0)
	for _, b := range s.backends {
		globalInFlight += b.ActiveRequest()
	}
	if globalInFlight >= s.maxConns.Load() {
		// At capacity - return least loaded to drain queue fairly
		if best := s.leastLoadedAlive(); best != nil {
			return best
		}
		return s.leastLoadedAny()
	}

	var candidates []*backend.Backend
	for _, b := range s.backends {
		if b.IsAlive() && b.ActiveRequest() < s.threshold.Load() {
			candidates = append(candidates, b)
		}
	}

	if len(candidates) == 0 {
		if best := s.leastLoadedAlive(); best != nil {
			return best
		}
		return s.leastLoadedAny()
	}

	if len(candidates) == 1 {
		return candidates[0]
	}

	// Epsilon-greedy exploration (5%): serve a random alive backend.
	if rand.Intn(20) == 0 {
		return candidates[rand.Intn(len(candidates))]
	}

	i1 := rand.Intn(len(candidates))
	i2 := rand.Intn(len(candidates))
	for i2 == i1 {
		i2 = rand.Intn(len(candidates))
	}
	b1 := candidates[i1]
	b2 := candidates[i2]

	if b1.LoadScore() <= b2.LoadScore() {
		return b1
	}
	return b2
}

func (s *PerformanceScheduler) leastLoadedAlive() *backend.Backend {
	var best *backend.Backend
	for _, b := range s.backends {
		if !b.IsAlive() {
			continue
		}
		if best == nil || b.LoadScore() < best.LoadScore() {
			best = b
		}
	}
	return best
}

func (s *PerformanceScheduler) leastLoadedAny() *backend.Backend {
	var best *backend.Backend
	for _, b := range s.backends {
		if best == nil || b.LoadScore() < best.LoadScore() {
			best = b
		}
	}
	return best
}
