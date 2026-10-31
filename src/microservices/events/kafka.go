package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// publish sends an event to its topic and returns metadata acknowledged by Kafka.
func publish(ctx context.Context, client *kgo.Client, event Event) (int32, int64, error) {
	data, err := json.Marshal(event)
	if err != nil {
		return 0, 0, err
	}
	if len(data)+len(event.ID) > maxEventBytes {
		return 0, 0, kerr.MessageTooLarge
	}
	record := &kgo.Record{Topic: event.Type + eventTopicSuffix, Key: []byte(event.ID), Value: data}
	// Kafka may continue resolving the send result after context cancellation.
	result := make(chan kgo.ProduceResult, 1)
	client.TryProduce(ctx, record, func(record *kgo.Record, err error) {
		result <- kgo.ProduceResult{Record: record, Err: err}
	})
	select {
	case response := <-result:
		return response.Record.Partition, response.Record.Offset, response.Err
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	}
}

// consume logs processed messages before committing offsets; duplicate processing is possible.
func consume(ctx context.Context, client *kgo.Client) {
	for ctx.Err() == nil {
		fetches := client.PollRecords(ctx, consumerBatchSize)
		if ctx.Err() != nil {
			return
		}
		for _, err := range fetches.Errors() {
			log.Printf("consume topic=%s partition=%d: %v", err.Topic, err.Partition, err.Err)
		}
		records := fetches.Records()
		for _, record := range records {
			// shortcut: MVP processing only writes logs, add idempotency before introducing business side effects.
			log.Printf("processed topic=%s partition=%d offset=%d event=%s", record.Topic, record.Partition, record.Offset, record.Value)
		}
		if len(records) > 0 {
			if err := client.CommitRecords(ctx, records...); err != nil {
				log.Printf("commit offsets: %v", err)
			}
		}
		client.AllowRebalance()
	}
}
