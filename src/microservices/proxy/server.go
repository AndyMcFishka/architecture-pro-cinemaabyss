package main

import (
	"context"
	"errors"
	"net"
	"net/http"
)

// serve drains active requests for up to five seconds after cancellation.
func serve(ctx context.Context, server *http.Server, listener net.Listener) error {
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	err := server.Shutdown(shutdown)
	if err != nil {
		server.Close()
	}
	serveErr := <-done
	if err != nil {
		return err
	}
	if !errors.Is(serveErr, http.ErrServerClosed) {
		return serveErr
	}
	return nil
}
