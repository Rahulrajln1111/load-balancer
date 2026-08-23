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

var transport = &http.Transport{
	MaxIdleConns:          1000,
	MaxIdleConnsPerHost:   200,
	IdleConnTimeout:       90 * time.Second,
	ResponseHeaderTimeout: 10 * time.Second,
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
