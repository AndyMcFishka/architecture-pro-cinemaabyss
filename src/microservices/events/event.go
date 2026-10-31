package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Event wraps a Kafka message with a unique ID and the original API payload.
type Event struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

// eventPayload validates field types; pointers distinguish missing values from zero values.
type eventPayload struct {
	MovieID     *int64     `json:"movie_id"`
	UserID      *int64     `json:"user_id"`
	PaymentID   *int64     `json:"payment_id"`
	Title       *string    `json:"title"`
	Action      *string    `json:"action"`
	Amount      *float64   `json:"amount"`
	Status      *string    `json:"status"`
	Timestamp   *time.Time `json:"timestamp"`
	Username    *string    `json:"username"`
	Email       *string    `json:"email"`
	MethodType  *string    `json:"method_type"`
	Rating      *float64   `json:"rating"`
	Genres      []string   `json:"genres"`
	Description *string    `json:"description"`
}

// newEvent validates the payload for its event type and preserves all input fields.
func newEvent(kind string, data []byte) (Event, error) {
	var p eventPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return Event{}, fmt.Errorf("invalid event JSON: %w", err)
	}
	valid := false
	switch kind {
	case movieEventType:
		valid = p.MovieID != nil && nonempty(p.Title) && nonempty(p.Action)
	case userEventType:
		valid = p.UserID != nil && nonempty(p.Action) && p.Timestamp != nil
	case paymentEventType:
		valid = p.PaymentID != nil && p.UserID != nil && p.Amount != nil && nonempty(p.Status) && p.Timestamp != nil
	}
	if !valid {
		return Event{}, fmt.Errorf("missing required fields for %s event", kind)
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return Event{}, fmt.Errorf("generate event ID: %w", err)
	}
	timestamp := time.Now().UTC()
	if p.Timestamp != nil {
		timestamp = *p.Timestamp
	}
	return Event{ID: hex.EncodeToString(id[:]), Type: kind, Timestamp: timestamp, Payload: json.RawMessage(data)}, nil
}

// nonempty checks that a required string is present and not blank.
func nonempty(value *string) bool { return value != nil && strings.TrimSpace(*value) != "" }
