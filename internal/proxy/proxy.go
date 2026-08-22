package proxy

import (
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"
)

var transport = &http.Transport{
	MaxIdleConns:        100,
	MaxIdleConnsPerHost: 20,
	IdleConnTimeout:     90 * time.Second,
}

func New(target *url.URL) *httputil.ReverseProxy {

	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
		},
		Transport: transport,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			http.Error(w, "backend unavailable", http.StatusBadGateway)
		},
	}
}


