package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
)

// TestCreateEvents checks all event types, payload preservation, and acknowledged Kafka metadata.
func TestCreateEvents(t *testing.T) {
	for _, tc := range []struct{ kind, body string }{
		{"movie", `{"movie_id":1,"title":"Test","action":"viewed","user_id":2,"extra":"preserved"}`},
		{"user", `{"user_id":2,"action":"logged_in","timestamp":"2026-10-29T12:00:00Z"}`},
		{"payment", `{"payment_id":3,"user_id":2,"amount":0,"status":"completed","timestamp":"2026-10-29T12:00:00Z"}`},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			var published Event
			calls := 0
			handler := newHandler(func(ctx context.Context, event Event) (int32, int64, error) {
				if _, ok := ctx.Deadline(); !ok {
					t.Error("publication has no deadline")
				}
				calls++
				published = event
				return 2, 42, nil
			})
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, httptest.NewRequest("POST", "/api/events/"+tc.kind, strings.NewReader(tc.body)))
			var result struct {
				Status    string
				Partition int32
				Offset    int64
				Event     Event
			}
			if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if w.Code != 201 || calls != 1 || result.Status != "success" || result.Partition != 2 || result.Offset != 42 || result.Event.ID != published.ID || len(published.ID) != 32 || published.Type != tc.kind || published.Timestamp.IsZero() || string(published.Payload) != tc.body {
				t.Fatalf("unexpected publication/response: calls=%d published=%+v response=%s", calls, published, w.Body.String())
			}
			if tc.kind != "movie" && published.Timestamp.Format("2006-01-02T15:04:05Z07:00") != "2026-10-29T12:00:00Z" {
				t.Fatal("timestamp not preserved")
			}
			second, err := newEvent(tc.kind, []byte(tc.body))
			if err != nil || second.ID == published.ID {
				t.Fatal("event ID not unique")
			}
		})
	}
}

// TestRejectInvalidEvents checks that invalid input never reaches Kafka.
func TestRejectInvalidEvents(t *testing.T) {
	handler := newHandler(func(context.Context, Event) (int32, int64, error) {
		t.Error("invalid event published")
		return 0, 0, nil
	})
	for _, tc := range []struct{ kind, body string }{
		{"movie", `null`}, {"movie", `[]`}, {"movie", `{}`}, {"movie", `{`},
		{"movie", `{"movie_id":1,"title":"T","action":"viewed"} {}`},
		{"movie", `{"movie_id":1.5,"title":"T","action":"viewed"}`},
		{"movie", `{"movie_id":null,"title":"T","action":"viewed"}`},
		{"movie", `{"movie_id":1,"title":" ","action":"viewed"}`},
		{"movie", `{"movie_id":1,"title":"T","action":"viewed","genres":123}`},
		{"user", `{"user_id":2,"action":"logged_in"}`},
		{"user", `{"user_id":2,"action":"logged_in","timestamp":"yesterday"}`},
		{"payment", `{"payment_id":3,"user_id":2,"status":"completed","timestamp":"2026-10-29T12:00:00Z"}`},
		{"payment", `{"payment_id":3,"user_id":2,"amount":"9.99","status":"completed","timestamp":"2026-10-29T12:00:00Z"}`},
	} {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest("POST", "/api/events/"+tc.kind, strings.NewReader(tc.body)))
		if w.Code != 400 || !strings.Contains(w.Body.String(), `"error"`) {
			t.Fatalf("invalid input accepted: %d %s", w.Code, w.Body.String())
		}
	}
}

// TestPublicationFailure checks that failures neither report success nor expose internal Kafka errors.
func TestPublicationFailure(t *testing.T) {
	h := newHandler(func(context.Context, Event) (int32, int64, error) { return 0, 0, errors.New("internal broker details") })
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("POST", "/api/events/movie", strings.NewReader(`{"movie_id":1,"title":"T","action":"viewed"}`)))
	if w.Code != 500 || strings.Contains(w.Body.String(), "internal broker details") {
		t.Fatalf("unexpected error response: %d %s", w.Code, w.Body.String())
	}
}

// TestRoutes checks health, unknown routes, and unsupported methods without publishing events.
func TestRoutes(t *testing.T) {
	h := newHandler(func(context.Context, Event) (int32, int64, error) {
		t.Error("unexpected publication")
		return 0, 0, nil
	})
	for _, tc := range []struct {
		method, path string
		code         int
	}{
		{"GET", "/api/events/health", 200}, {"GET", "/api/events/movie", 405},
		{"POST", "/api/events/unknown", 404},
	} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))
		if w.Code != tc.code {
			t.Fatalf("%s %s: %d", tc.method, tc.path, w.Code)
		}
	}
}

// TestOversizedEvents checks both HTTP body and Kafka record limits produce 413.
func TestOversizedEvents(t *testing.T) {
	calls := 0
	handler := newHandler(func(context.Context, Event) (int32, int64, error) {
		calls++
		return 0, 0, kerr.MessageTooLarge
	})
	for _, body := range []string{strings.Repeat(" ", (1<<20)+1), `{"movie_id":1,"title":"T","action":"viewed"}`} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest("POST", "/api/events/movie", strings.NewReader(body)))
		if response.Code != 413 {
			t.Fatalf("expected 413, got %d: %s", response.Code, response.Body.String())
		}
	}
	if calls != 1 {
		t.Fatalf("oversized body reached publisher: calls=%d", calls)
	}
}
