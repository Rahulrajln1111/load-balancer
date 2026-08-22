package health

import (
	"context"
	"load-balancer/internal/backend"
	"net/http"
	"sync"
	"time"
)

type Checker struct {
	backends []*backend.Backend
	client   *http.Client
	interval time.Duration
}

func New(b []*backend.Backend, interval time.Duration) *Checker {
	return &Checker{
		backends: b,
		client: &http.Client{
			Timeout: 500 * time.Millisecond,
		},
		interval: interval,
	}
}

func (c *Checker) probe(ctx context.Context, b *backend.Backend) {
	url := b.URL.String() + "/health"

	timoutctx, cancel := context.WithTimeout(ctx, 500*time.Microsecond)

	defer cancel()

	req, err := http.NewRequestWithContext(timoutctx, "GET", url, nil)

	if err != nil {
		b.SetAlive(false)
		return
	}


	resp,err:= c.client.Do(req)


	if err != nil {
		b.SetAlive(false)
		return
	}

	defer resp.Body.Close()

	b.SetAlive(resp.StatusCode == http.StatusOK)
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
