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

// Transport sized for 2500+ concurrent clients across 3 backends.
// Backends ACK /message from a local journal (sub-ms), so per-backend
// connection limits are the only queue that can stall requests.
var transport = &http.Transport{
	MaxIdleConns:          4000,
	MaxIdleConnsPerHost:   1200,
	MaxConnsPerHost:       1600,
	IdleConnTimeout:       90 * time.Second,
	ResponseHeaderTimeout: 15 * time.Second,
	ExpectContinueTimeout: 1 * time.Second,
	TLSHandshakeTimeout:   3 * time.Second,
	DisableKeepAlives:     false,
	ForceAttemptHTTP2:     true,
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
