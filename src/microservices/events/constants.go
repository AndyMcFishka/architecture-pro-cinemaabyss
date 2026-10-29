package main

import "time"

// Event types and topic names are shared by the API, producer, and consumer.
const (
	movieEventType      = "movie"
	userEventType       = "user"
	paymentEventType    = "payment"
	eventTopicSuffix    = "-events"
	movieEventsTopic    = movieEventType + eventTopicSuffix
	userEventsTopic     = userEventType + eventTopicSuffix
	paymentEventsTopic  = paymentEventType + eventTopicSuffix
	eventsConsumerGroup = "cinemaabyss-events"
)

// Event limits keep HTTP input and encoded Kafka records bounded.
const (
	maxRequestBodyBytes      = 1 << 20
	kafkaBatchMaxBytes       = 1000012
	kafkaRecordOverheadBytes = 1024 // Reserve space for record and batch metadata.
	maxEventBytes            = kafkaBatchMaxBytes - kafkaRecordOverheadBytes
	consumerBatchSize        = 100
)

// Service timeouts bound HTTP work, publication, and shutdown independently.
const (
	publishTimeout    = 10 * time.Second
	readHeaderTimeout = 5 * time.Second
	readTimeout       = 15 * time.Second
	writeTimeout      = 15 * time.Second
	idleTimeout       = 60 * time.Second
	shutdownTimeout   = 15 * time.Second
)
