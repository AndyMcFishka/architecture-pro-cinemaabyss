package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

// TestShutdownDrainsRequest checks that cancellation preserves an in-flight response.
func TestShutdownDrainsRequest(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started, release, shuttingDown := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(release)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		io.WriteString(w, "completed")
	})}
	server.RegisterOnShutdown(func() { close(shuttingDown) })
	done := make(chan error, 1)
	go func() { done <- serve(ctx, server, listener) }()
	response := make(chan string, 1)
	go func() {
		client := &http.Client{Timeout: 3 * time.Second}
		r, err := client.Get("http://" + listener.Addr().String())
		if err != nil {
			response <- err.Error()
			return
		}
		defer r.Body.Close()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			response <- err.Error()
			return
		}
		response <- string(body)
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("request did not start")
	}
	cancel()
	select {
	case <-shuttingDown:
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown did not start")
	}
	select {
	case err := <-done:
		t.Fatalf("shutdown did not wait: %v", err)
	default:
	}
	release <- struct{}{}
	if got := <-response; got != "completed" {
		t.Fatalf("response lost: %s", got)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// TestServeFailure checks that a listener error returns without waiting for a signal.
func TestServeFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener.Close()
	if err := serve(context.Background(), &http.Server{}, listener); err == nil {
		t.Fatal("expected listener error")
	}
}
