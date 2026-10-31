// Package main runs the CinemaAbyss HTTP gateway for gradual migration.
package main

import (
	"context"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

// main handles termination signals and reports startup or shutdown failures.
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

// run configures the proxy and waits for in-flight requests during shutdown.
func run(ctx context.Context) error {
	handler, err := newHandler(rand.IntN)
	if err != nil {
		return err
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}
	server := &http.Server{
		Addr: ":" + port, Handler: handler,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout, WriteTimeout: writeTimeout,
		IdleTimeout: idleTimeout,
	}
	log.Printf("Starting proxy on port %s", port)
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return err
	}
	return serve(ctx, server, listener)
}
