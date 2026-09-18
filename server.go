package main

// The HTTP server itself, as distinct from the metrics logic it serves.
//
// Everything here exists because of the same observation: net/http will
// happily let a client cost the server unbounded time and unbounded
// goroutines, and the defaults do nothing about it. A metrics ingest
// endpoint is exactly the kind of service that meets both a badly-behaved
// client and a genuine firehose, so the failure modes are not hypothetical.

import (
	"net/http"
	"time"
)

const (
	// readHeaderTimeout is the slow-loris defence, and the single most
	// important field here. Without it a client can open a connection,
	// dribble one header byte a minute, and hold a goroutine and a socket
	// for as long as it likes; a few thousand such connections cost
	// nothing to make and take the server down. Headers from anything
	// legitimate arrive in one packet, so this can be tight.
	readHeaderTimeout = 5 * time.Second

	// readTimeout bounds the whole request, headers and body together.
	//
	// It works with the body caps rather than replacing them:
	// MaxBytesReader bounds how *much* a client may send, this bounds how
	// *long* it may take, and only the two together bound the rate. Either
	// alone leaves a hole - unbounded bytes, or a 1 MiB batch delivered one
	// byte per second. It is much looser than the header timeout because a
	// full batch on a slow link is a legitimate request that needs room.
	readTimeout = 30 * time.Second

	// writeTimeout starts when the request headers are read, not when the
	// response begins, so it covers handler time as well as the write
	// itself. The slowest thing here is /stats over a large store, at well
	// under a millisecond, so this is a backstop against a stuck handler
	// rather than a budget anything approaches.
	writeTimeout = 30 * time.Second

	// idleTimeout reaps keep-alive connections that have gone quiet. It is
	// what makes the read timeouts above safe to set: without it, Go
	// applies ReadTimeout to the idle wait between keep-alive requests, so
	// a well-behaved client reusing a connection would be disconnected for
	// the crime of not having anything to say yet. loadgen holds one
	// connection per worker and uses it continuously, so in practice this
	// only closes genuinely abandoned sockets.
	idleTimeout = 60 * time.Second

	// maxHeaderBytes caps the headers, which MaxBytesReader does not cover
	// - that only ever sees the body.
	maxHeaderBytes = 1 << 16 // 64 KiB
)

// newServer builds the HTTP server with every timeout set.
//
// The zero value of http.Server has no timeouts at all: no limit on how
// long a client may take to send its headers, its body, or to read the
// response. That is the default this project shipped with until now, and
// it is the standard Go production bug - the code works perfectly against
// well-behaved clients and falls over against one that isn't.
func newServer(h http.Handler) *http.Server {
	return &http.Server{
		Handler:           h,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
		MaxHeaderBytes:    maxHeaderBytes,
	}
}
