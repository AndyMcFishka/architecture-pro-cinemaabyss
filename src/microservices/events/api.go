package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"

	"github.com/twmb/franz-go/pkg/kerr"
)

// publishEvent returns the partition and offset acknowledged by Kafka, or a publication error.
type publishEvent func(context.Context, Event) (int32, int64, error)

// newHandler builds the event API; 201 confirms publication, not consumer processing.
func newHandler(publish publishEvent) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/events/health", func(w http.ResponseWriter, r *http.Request) {
		// Health indicates HTTP process availability; actual publication verifies Kafka connectivity.
		writeJSON(w, http.StatusOK, map[string]bool{"status": true})
	})
	for _, kind := range []string{movieEventType, userEventType, paymentEventType} {
		mux.HandleFunc("POST /api/events/"+kind, func(w http.ResponseWriter, r *http.Request) {
			data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBodyBytes))
			if err != nil {
				var tooLarge *http.MaxBytesError
				if errors.As(err, &tooLarge) {
					writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "event body exceeds 1 MiB"})
				} else {
					writeJSON(w, 400, map[string]string{"error": "cannot read event body"})
				}
				return
			}
			event, err := newEvent(kind, data)
			if err != nil {
				writeJSON(w, 400, map[string]string{"error": err.Error()})
				return
			}
			ctx, cancel := context.WithTimeout(r.Context(), publishTimeout)
			defer cancel()
			partition, offset, err := publish(ctx, event)
			if err != nil {
				if errors.Is(err, kerr.MessageTooLarge) {
					writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "encoded event exceeds Kafka message size limit"})
					return
				}
				log.Printf("publish failed event_id=%s type=%s: %v", event.ID, event.Type, err)
				writeJSON(w, 500, map[string]string{"error": "Kafka publication failed"})
				return
			}
			writeJSON(w, 201, struct {
				Status    string `json:"status"`
				Partition int32  `json:"partition"`
				Offset    int64  `json:"offset"`
				Event     Event  `json:"event"`
			}{"success", partition, offset, event})
		})
	}
	return mux
}

// writeJSON sends an HTTP status and a JSON response in the shared API format.
func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("write response: %v", err)
	}
}
