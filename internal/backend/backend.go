package backend

import (
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"time"
)

type Backend struct {
	URL           *url.URL
	Proxy         *httputil.ReverseProxy
	Failures      atomic.Int64
	Alive         atomic.Bool
	InFlight      atomic.Int64
	Requests      atomic.Uint64
	TotalRespTime atomic.Int64
	ResponseCount atomic.Uint64
}

func New(target *url.URL, proxy *httputil.ReverseProxy) *Backend {
	b := &Backend{
		URL:   target,
		Proxy: proxy,
	}
	b.Alive.Store(true)

	return b
}

func (b *Backend) RecordProbe(ok bool, failAfter int64) {
	if ok {
		b.Failures.Store(0)
		b.Alive.Store(true)
		return
	}
	if b.Failures.Add(1) >= failAfter {
		b.Alive.Store(false)
	}
}

func (b *Backend) IsAlive() bool        { return b.Alive.Load() }
func (b *Backend) SetAlive(v bool)       { b.Alive.Store(v) }
func (b *Backend) IncInFlight()          { b.InFlight.Add(1) }
func (b *Backend) DecInFlight()          { b.InFlight.Add(-1) }
func (b *Backend) RecordRequests()       { b.Requests.Add(1) }
func (b *Backend) TotalRequests() uint64 { return b.Requests.Load() }
func (b *Backend) ActiveRequest() int64  { return b.InFlight.Load() }

func (b *Backend) RecordResponseTime(d time.Duration) {
	b.TotalRespTime.Add(d.Milliseconds())
	b.ResponseCount.Add(1)
}

func (b *Backend) AvgResponseTime() int64 {
	count := b.ResponseCount.Load()
	if count == 0 {
		return 0
	}
	return b.TotalRespTime.Load() / int64(count)
}

func (b *Backend) LoadScore() int64 {
	return b.ActiveRequest() + b.AvgResponseTime()/100
}
