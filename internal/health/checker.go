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

func (c *Checker) probe(b *backend.Backend) {
	url := b.URL.String() + "/health"

	resp, err := c.client.Get(url)

	if err != nil {
		b.SetAlive(false)
		return
	}

	resp.Body.Close()

	b.SetAlive(resp.StatusCode == http.StatusOK)
}

func (c *Checker) CheckAll() {
	var wg sync.WaitGroup

	for _, b := range c.backends {
		wg.Add(1)
		go func(b *backend.Backend) {
			defer wg.Done()
			c.probe(b)
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
			c.CheckAll()
		}
	}
}
