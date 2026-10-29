# Build the standalone API and Kafka producer/consumer.
FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /events-service .

# Run as a non-root user with CA certificates for TLS connections.
FROM alpine:latest
RUN apk --no-cache add ca-certificates
COPY --from=builder /events-service /events-service
USER 65534:65534
EXPOSE 8082
CMD ["/events-service"]
