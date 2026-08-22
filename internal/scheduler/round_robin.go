package scheduler

import (
	"load-balancer/internal/backend"
	"sync/atomic"
)

type RoundRobin struct {
	backends []*backend.Backend
	next     atomic.Uint64
}

func NewRrScheduler(backends []*backend.Backend) *RoundRobin {
	return &RoundRobin{
		backends: backends,
	}
}

func (rr *RoundRobin) Next() *backend.Backend {

	n := len(rr.backends)

	if n == 0 {
		return nil
	}

	start := rr.next.Add(1) - 1

	for i := 0; i < n; i++ {

		idx := int((uint64(i) + start) % uint64(n))

		b := rr.backends[idx]

		if b.IsAlive() {
			return b
		}
	}

	return nil
}
