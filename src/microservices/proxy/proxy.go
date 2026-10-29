package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
)

// newHandler validates configuration and builds the router; draw returns a number in [0, n).
func newHandler(draw func(int) int) (http.Handler, error) {
	gradual, err := strconv.ParseBool(os.Getenv("GRADUAL_MIGRATION"))
	if err != nil {
		return nil, fmt.Errorf("GRADUAL_MIGRATION must be a boolean: %w", err)
	}
	percent, err := strconv.Atoi(os.Getenv("MOVIES_MIGRATION_PERCENT"))
	if err != nil || percent < 0 || percent > maxMigrationPercent {
		return nil, fmt.Errorf("MOVIES_MIGRATION_PERCENT must be an integer from 0 to 100")
	}
	upstreams := make(map[string]*httputil.ReverseProxy)
	for _, key := range []string{monolithURLKey, moviesServiceURLKey, eventsServiceURLKey} {
		target, err := url.Parse(os.Getenv(key))
		if err != nil || target.Hostname() == "" || (target.Scheme != "http" && target.Scheme != "https") || target.User != nil || target.RawQuery != "" || target.Fragment != "" {
			return nil, fmt.Errorf("%s must be an HTTP(S) URL without credentials, query or fragment", key)
		}
		proxy := &httputil.ReverseProxy{
			Rewrite: func(r *httputil.ProxyRequest) {
				r.SetURL(target)
				r.SetXForwarded()
			},
			ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
				log.Printf("upstream %s failed: %v", key, err)
				http.Error(w, "Bad Gateway", http.StatusBadGateway)
			},
		}
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.ResponseHeaderTimeout = upstreamHeaderTimeout
		proxy.Transport = transport
		upstreams[key] = proxy
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := monolithURLKey
		switch {
		case r.URL.Path == "/health":
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			fmt.Fprint(w, "Strangler Fig Proxy is healthy")
			return
		case r.URL.Path == "/api/movies/health":
			key = moviesServiceURLKey
		case r.URL.Path == "/api/movies" || strings.HasPrefix(r.URL.Path, "/api/movies/"):
			if !gradual || draw(maxMigrationPercent) < percent {
				key = moviesServiceURLKey
			}
		case strings.HasPrefix(r.URL.Path, "/api/events/"):
			key = eventsServiceURLKey
		}
		ctx, cancel := context.WithTimeout(r.Context(), upstreamRequestTimeout)
		defer cancel()
		upstreams[key].ServeHTTP(w, r.WithContext(ctx))
	}), nil
}
