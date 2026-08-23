package main

import (
	"context"
	"load-balancer/internal/backend"
	"load-balancer/internal/health"
	"load-balancer/internal/proxy"
	"load-balancer/internal/scheduler"
	"load-balancer/internal/server"
	"log"
	"net/http"
	"net/url"
	"time"
)

var backendURLs = []string{
	"http://10.1.75.51:3290",
	"http://10.1.75.51:3291",
	"http://10.1.75.51:3292",
}

func createBackends() []*backend.Backend {
	var backends []*backend.Backend
	for _, raw := range backendURLs {

		target, err := url.Parse(raw)

		if err != nil {
			log.Fatal("error parsing url")
		}

		p := proxy.New(target)
		b := backend.New(target, p)
		backends = append(backends, b)

	}

	return backends
}

func main() {
	backends := createBackends()

	rr := scheduler.NewRrScheduler(backends)

	srvc := server.New(rr, backends)

	mux := http.NewServeMux()

	srvc.Register(mux)

	checker := health.New(backends, time.Second*2)

	rootCtx, stopBackgWorker := context.WithCancel(context.Background())

	defer stopBackgWorker()
	go checker.Run(rootCtx)

	httpServer := &http.Server{
		Addr:    ":3000",
		Handler: mux,
	}

	log.Println("Load Balancer listening on :3000")

	if err := httpServer.ListenAndServe(); err != nil &&
		err != http.ErrServerClosed {

		log.Fatal(err)
	}

}
