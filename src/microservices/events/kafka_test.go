package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/twmb/franz-go/pkg/kerr"
)

// TestPublishDeadline checks that publication stops waiting when the broker does not respond.
func TestPublishDeadline(t *testing.T) {
	client, err := kgo.NewClient(kgo.SeedBrokers("broker:9092"), kgo.Dialer(func(ctx context.Context, _, _ string) (net.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, _, err = publish(ctx, client, Event{ID: "test", Type: "movie", Payload: []byte(`{}`)})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline, got %v", err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("publication outlived deadline")
	}
}

// TestEncodedEventSize checks the exact limit and expansion caused by JSON escaping.
func TestEncodedEventSize(t *testing.T) {
	event := Event{ID: strings.Repeat("a", 32), Type: "movie", Payload: []byte(`{"extra":""}`)}
	encoded, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	padding := maxEventBytes - len(encoded) - len(event.ID)
	for _, tc := range []struct {
		name, value string
		tooLarge    bool
	}{
		{"at limit", strings.Repeat("x", padding), false},
		{"one byte over", strings.Repeat("x", padding+1), true},
		{"escaped JSON", strings.Repeat("<", 200000), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			event.Payload = []byte(`{"extra":"` + tc.value + `"}`)
			client, err := kgo.NewClient(kgo.SeedBrokers("unused:9092"), kgo.Dialer(func(ctx context.Context, _, _ string) (net.Conn, error) { <-ctx.Done(); return nil, ctx.Err() }))
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, _, err = publish(ctx, client, event)
			if tc.tooLarge {
				if !errors.Is(err, kerr.MessageTooLarge) {
					t.Fatalf("expected size rejection, got %v", err)
				}
			} else if !errors.Is(err, context.Canceled) {
				t.Fatalf("valid size did not reach producer: %v", err)
			}
		})
	}
}
