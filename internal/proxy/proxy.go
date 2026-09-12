package proxy

import (
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"
)

type ctxKey struct{}
type ErrHolder struct {
	Err error
}

func CtxKey() ctxKey { return ctxKey{} }

// Optimized transport for managing up to 2700 concurrent requests
var transport = &http.Transport{
	MaxIdleConns:        3000,
	MaxIdleConnsPerHost: 800,
	MaxConnsPerHost:     1000,
	IdleConnTimeout:     90 * time.Second,
	ResponseHeaderTimeout: 15 * time.Second,
	ExpectContinueTimeout: 1 * time.Second,
	TLSHandshakeTimeout:  3 * time.Second,
	DisableKeepAlives:    false,
	ForceAttemptHTTP2:    true,
}

func New(target *url.URL) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			pr.SetXForwarded()
		},
		Transport: transport,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			if h, ok := r.Context().Value(ctxKey{}).(*ErrHolder); ok {
				h.Err = err
				return
			}
			http.Error(w, "backend unavailable", http.StatusBadGateway)
		},
	}
}
