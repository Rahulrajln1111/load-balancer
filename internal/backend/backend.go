package backend

import (
	"net/http/httputil"
	"net/url"
	"sync/atomic"
)

type Backend struct {
	URL   *url.URL
	Proxy *httputil.ReverseProxy

	Alive    atomic.Bool
	InFlight atomic.Int64
	Requests atomic.Uint64
}

func New(target *url.URL, proxy *httputil.ReverseProxy) *Backend {
	b := &Backend{
		URL:   target,
		Proxy: proxy,
	}
	b.Alive.Store(true)

	return b
}

func (b *Backend) GetAlive() bool {
	return b.Alive.Load()
}

func (b *Backend) SetAlive(v bool) {
	b.Alive.Store(v)
}

func (b *Backend) IncInFlight() {
	b.InFlight.Add(1)
}

func (b *Backend) DecInFlight() {
	b.InFlight.Add(-1)
}

func (b *Backend) RecordRequests() {
	b.Requests.Add(1)
}

func (b *Backend) TotalRequests() uint64 {
	return b.Requests.Load()
}

func (b *Backend) ActiveRequest() int64 {
	return b.InFlight.Load()
}

func (b *Backend) IsAlive() bool {
	return b.Alive.Load()
}
