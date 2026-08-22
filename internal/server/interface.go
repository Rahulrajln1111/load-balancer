package server

import "load-balancer/internal/backend"

type Scheduler interface {
	Next() *backend.Backend
}
