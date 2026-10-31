package main

import "time"

// Upstream keys identify both environment settings and configured proxies.
const (
	monolithURLKey      = "MONOLITH_URL"
	moviesServiceURLKey = "MOVIES_SERVICE_URL"
	eventsServiceURLKey = "EVENTS_SERVICE_URL"
	maxMigrationPercent = 100
)

// The upstream deadline expires before the HTTP socket write deadline.
const (
	upstreamHeaderTimeout  = 20 * time.Second
	upstreamRequestTimeout = 25 * time.Second
	readHeaderTimeout      = 5 * time.Second
	readTimeout            = 30 * time.Second
	writeTimeout           = 30 * time.Second
	idleTimeout            = 60 * time.Second
	shutdownTimeout        = 5 * time.Second
)
