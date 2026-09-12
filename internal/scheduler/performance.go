package scheduler

import (
	"load-balancer/internal/backend"
	"math/rand"
	"sync/atomic"
)

const loadThreshold = 50

type PerformanceScheduler struct {
	backends  []*backend.Backend
	threshold atomic.Int64
}

func NewPerformanceScheduler(backends []*backend.Backend) *PerformanceScheduler {
	s := &PerformanceScheduler{
		backends: backends,
	}
	s.threshold.Store(loadThreshold)
	return s
}

func (s *PerformanceScheduler) Next() *backend.Backend {
	var candidates []*backend.Backend
	for _, b := range s.backends {
		if b.IsAlive() && b.ActiveRequest() < s.threshold.Load() {
			candidates = append(candidates, b)
		}
	}

	if len(candidates) == 0 {
		return s.leastLoadedAlive()
	}

	if len(candidates) == 1 {
		return candidates[0]
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
