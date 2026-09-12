package health

import (
	"context"
	"io"
	"load-balancer/internal/backend"
	"net/http"
	"sync"
	"time"
)

const (
	probeTimeout       = 4 * time.Second
	failuresBeforeDown = 5
)

type Checker struct {
	backends []*backend.Backend
	client   *http.Client
	interval time.Duration
}

func New(b []*backend.Backend, interval time.Duration) *Checker {
	return &Checker{
		backends: b,
		client:   &http.Client{},
		interval: interval,
	}
}

func (c *Checker) probe(ctx context.Context, b *backend.Backend) {
	url := b.URL.String() + "/health"

	timoutctx, cancel := context.WithTimeout(ctx, probeTimeout)

	defer cancel()

	req, err := http.NewRequestWithContext(timoutctx, http.MethodGet, url, nil)

	if err != nil {
		b.RecordProbe(false, failuresBeforeDown)
		return
	}

	resp, err := c.client.Do(req)

	if err != nil {
		b.RecordProbe(false, failuresBeforeDown)
		return
	}

	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	b.RecordProbe(resp.StatusCode == http.StatusOK, failuresBeforeDown)
}

func (c *Checker) CheckAll(ctx context.Context) {
	var wg sync.WaitGroup

	for _, b := range c.backends {
		wg.Add(1)
		go func(b *backend.Backend) {
			defer wg.Done()
			c.probe(ctx, b)
		}(b)
	}
	wg.Wait()
}

func (c *Checker) Run(ctx context.Context) {

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.CheckAll(ctx)
		}
	}
}
