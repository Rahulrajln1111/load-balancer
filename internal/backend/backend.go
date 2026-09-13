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
	EwmaRespTime  atomic.Int64
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
	ms := d.Milliseconds()
	// Clamp absurd samples (host stalls, queueing spikes). They are noise for
	// scheduling purposes — a real failure is caught by health checks and the
	// proxy retry — and one unclamped spike wrecks the EWMA for minutes.
	const maxSampleMs = 5000
	if ms > maxSampleMs {
		ms = maxSampleMs
	}
	b.TotalRespTime.Add(ms)
	b.ResponseCount.Add(1)
	// EWMA (alpha = 0.2): recent samples dominate, so a backend that
	// recovers after a slow period rejoins rotation within ~10-20
	// requests instead of being penalized forever by history.
	for {
		old := b.EwmaRespTime.Load()
		var updated int64
		if old == 0 {
			updated = ms
		} else {
			updated = old + (ms-old)/5
		}
		if b.EwmaRespTime.CompareAndSwap(old, updated) {
			break
		}
	}
}

func (b *Backend) AvgResponseTime() int64 {
	return b.EwmaRespTime.Load()
}

func (b *Backend) LoadScore() int64 {
	// Weight: active requests matter more for concurrency control
	return b.ActiveRequest()*2 + b.AvgResponseTime()/200
}
