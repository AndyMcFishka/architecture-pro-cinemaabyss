package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// setup creates local upstream servers and test configuration.
func setup(t *testing.T) {
	t.Helper()
	t.Setenv("GRADUAL_MIGRATION", "true")
	t.Setenv("MOVIES_MIGRATION_PERCENT", "50")
	for _, key := range []string{"MONOLITH_URL", "MOVIES_SERVICE_URL", "EVENTS_SERVICE_URL"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Upstream", key)
			w.Header().Set("X-Method", r.Method)
			w.Header().Set("X-URI", r.RequestURI)
			w.Header().Set("X-Received-Content-Type", r.Header.Get("Content-Type"))
			w.Header().Set("X-Received-Forwarded-For", r.Header.Get("X-Forwarded-For"))
			w.WriteHeader(http.StatusCreated)
			io.Copy(w, r.Body)
		}))
		t.Cleanup(server.Close)
		t.Setenv(key, server.URL)
	}
}

// TestRoutingAndForwarding checks migration boundaries and preserves HTTP requests and responses.
func TestRoutingAndForwarding(t *testing.T) {
	setup(t)
	for _, tc := range []struct {
		name, gradual, percent, path, want string
		draw                               int
	}{
		{"zero", "true", "0", "/api/movies?id=7", "MONOLITH_URL", 0},
		{"half movies", "true", "50", "/api/movies?id=7", "MOVIES_SERVICE_URL", 49},
		{"half monolith", "true", "50", "/api/movies?id=7", "MONOLITH_URL", 50},
		{"all", "true", "100", "/api/movies", "MOVIES_SERVICE_URL", 99},
		{"disabled", "false", "0", "/api/movies", "MOVIES_SERVICE_URL", 99},
		{"health bypass", "true", "0", "/api/movies/health", "MOVIES_SERVICE_URL", 0},
		{"users", "true", "100", "/api/users?id=2", "MONOLITH_URL", 0},
		{"payments", "true", "100", "/api/payments", "MONOLITH_URL", 0},
		{"subscriptions", "true", "100", "/api/subscriptions", "MONOLITH_URL", 0},
		{"events", "true", "0", "/api/events/movie", "EVENTS_SERVICE_URL", 0},
		{"events health", "true", "0", "/api/events/health", "EVENTS_SERVICE_URL", 0},
		{"prefix boundary", "true", "100", "/api/moviesXYZ", "MONOLITH_URL", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("GRADUAL_MIGRATION", tc.gradual)
			t.Setenv("MOVIES_MIGRATION_PERCENT", tc.percent)
			h, err := newHandler(func(n int) int {
				if n != 100 {
					t.Fatalf("draw bound = %d", n)
				}
				return tc.draw
			})
			if err != nil {
				t.Fatal(err)
			}
			for _, method := range []string{"GET", "POST"} {
				body := `{"title":"Test"}`
				req := httptest.NewRequest(method, tc.path, strings.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("X-Forwarded-For", "spoofed")
				w := httptest.NewRecorder()
				h.ServeHTTP(w, req)
				if w.Code != 201 || w.Header().Get("X-Upstream") != tc.want || w.Header().Get("X-Method") != method || w.Header().Get("X-URI") != tc.path || w.Body.String() != body || w.Header().Get("X-Received-Content-Type") != "application/json" {
					t.Fatalf("forwarding failed: %d %v %s", w.Code, w.Header(), w.Body.String())
				}
				if strings.Contains(w.Header().Get("X-Received-Forwarded-For"), "spoofed") {
					t.Fatal("forwarded untrusted client header")
				}
			}
		})
	}
}

// TestInvalidConfig checks that invalid settings prevent startup.
func TestInvalidConfig(t *testing.T) {
	setup(t)
	for _, tc := range []struct{ key, value string }{
		{"GRADUAL_MIGRATION", "invalid"}, {"MOVIES_MIGRATION_PERCENT", "-1"},
		{"MOVIES_MIGRATION_PERCENT", "101"}, {"MOVIES_MIGRATION_PERCENT", "1.5"},
		{"MONOLITH_URL", ""}, {"MOVIES_SERVICE_URL", "ftp://host"},
		{"EVENTS_SERVICE_URL", "http://user:password@host"}, {"MONOLITH_URL", "http://host?x=1"},
	} {
		t.Run(tc.key+tc.value, func(t *testing.T) {
			t.Setenv(tc.key, tc.value)
			if _, err := newHandler(func(int) int { return 0 }); err == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
}

// TestHealthAndUpstreamFailure checks health and a 502 response without retrying against the monolith.
func TestHealthAndUpstreamFailure(t *testing.T) {
	setup(t)
	h, err := newHandler(func(int) int { return 0 })
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/health", nil))
	if w.Code != 200 || w.Body.String() != "Strangler Fig Proxy is healthy" {
		t.Fatalf("health: %v", w)
	}
	// A closed local listener simulates an unavailable movies service.
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("unexpected call") }))
	dead.Close()
	t.Setenv("MOVIES_SERVICE_URL", dead.URL)
	t.Setenv("MOVIES_MIGRATION_PERCENT", "100")
	h, err = newHandler(func(int) int { return 0 })
	if err != nil {
		t.Fatal(err)
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("POST", "/api/movies", strings.NewReader(`{"title":"Test"}`)))
	if w.Code != 502 || w.Header().Get("X-Upstream") != "" {
		t.Fatalf("expected 502 without fallback, got %d", w.Code)
	}
}

// TestUpstreamBodyDeadline checks that a stalled body is canceled by the proxy itself.
func TestUpstreamBodyDeadline(t *testing.T) {
	canceled := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(canceled)
	}))
	defer upstream.Close()
	for _, key := range []string{"MONOLITH_URL", "MOVIES_SERVICE_URL", "EVENTS_SERVICE_URL"} {
		t.Setenv(key, upstream.URL)
	}
	t.Setenv("GRADUAL_MIGRATION", "true")
	t.Setenv("MOVIES_MIGRATION_PERCENT", "100")
	handler, err := newHandler(func(int) int { return 0 })
	if err != nil {
		t.Fatal(err)
	}
	proxy := httptest.NewServer(handler)
	defer proxy.Close()
	client := &http.Client{Timeout: 28 * time.Second}
	response, err := client.Get(proxy.URL + "/api/movies")
	if response != nil {
		io.Copy(io.Discard, response.Body)
		response.Body.Close()
	}
	// A client-side deadline must not be mistaken for a successful proxy timeout.
	if err != nil && strings.Contains(err.Error(), "Client.Timeout") {
		t.Fatal(err)
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("upstream context was not canceled")
	}
}
