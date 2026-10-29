// Package main runs a standalone API MVP that publishes and consumes Kafka events.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/twmb/franz-go/pkg/kgo"
)

// main reports fatal errors after releasing service resources.
func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

// run connects HTTP, producer, and consumer lifecycles and handles SIGINT/SIGTERM.
func run() error {
	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	for i, broker := range brokers {
		broker = strings.TrimSpace(broker)
		host, port, err := net.SplitHostPort(broker)
		number, numberErr := strconv.Atoi(port)
		if err != nil || host == "" || numberErr != nil || number < 1 || number > 65535 {
			return fmt.Errorf("invalid KAFKA_BROKERS address: %q", broker)
		}
		brokers[i] = broker
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(eventsConsumerGroup),
		kgo.ConsumeTopics(movieEventsTopic, userEventsTopic, paymentEventsTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(),
		kgo.RecordDeliveryTimeout(publishTimeout),
	)
	if err != nil {
		return err
	}
	defer client.CloseAllowingRebalance()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan struct{})
	go func() { defer close(done); consume(ctx, client) }()
	defer func() { stop(); <-done }()
	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}
	server := &http.Server{
		Addr:              ":" + port,
		Handler:           newHandler(func(ctx context.Context, event Event) (int32, int64, error) { return publish(ctx, client, event) }),
		ReadHeaderTimeout: readHeaderTimeout, ReadTimeout: readTimeout,
		WriteTimeout: writeTimeout, IdleTimeout: idleTimeout,
	}
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			log.Printf("HTTP shutdown: %v", err)
			server.Close()
		}
	}()
	log.Printf("Starting events on port %s", port)
	err = server.ListenAndServe()
	stop()
	<-stopped
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
