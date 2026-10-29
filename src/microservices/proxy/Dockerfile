# Build the HTTP gateway using the Go standard library.
FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY . .
RUN CGO_ENABLED=0 go build -o /proxy-service .

# Run as a non-root user with CA certificates for HTTPS upstreams.
FROM alpine:latest
RUN apk --no-cache add ca-certificates
COPY --from=builder /proxy-service /proxy-service
USER 65534:65534
EXPOSE 8000
CMD ["/proxy-service"]
