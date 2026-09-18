package main

// The HTTP server itself, as distinct from the metrics logic it serves.
//
// Everything here exists because of the same observation: net/http will
// happily let a client cost the server unbounded time and unbounded
// goroutines, and the defaults do nothing about it. A metrics ingest
// endpoint is exactly the kind of service that meets both a badly-behaved
// client and a genuine firehose, so the failure modes are not hypothetical.

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
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

// maxInFlight caps how many requests may be in a handler at once.
//
// net/http runs every connection on its own goroutine and imposes no limit,
// so under a genuine firehose the server's concurrency is whatever clients
// decide it is. The number itself is chosen to be far above real load rather
// than tuned: a reporter fleet holds one keep-alive connection per agent and
// sends one request at a time, so 256 is comfortable for a fleet several
// times larger than anything this project simulates, while still bounding
// the worst case - 256 batch bodies being decoded at once, rather than
// however many sockets an attacker can open.
const maxInFlight = 256

// inFlightLimit is maxInFlight, unless METRICFLOW_MAX_INFLIGHT overrides it.
//
// Why this is tunable when shardCount (§24) deliberately is not: shardCount is
// an algorithmic choice whose right value follows from the code, and exposing
// it would invite tuning nobody has data for. A concurrency limit is an
// operational choice - it depends on the machine, the deployment, and how
// expensive a request is there, none of which this code can know. It is also
// what lets CI prove the shedding path on every push, by starting a server
// small enough to saturate on purpose rather than needing a machine that can
// actually overwhelm 256 concurrent handlers.
//
// A bad value fails startup rather than falling back to the default, for the
// same reason a bad alert rule does (§18): a misconfigured limit that quietly
// ignores you is found at 3am, and a refusal to start is found immediately.
func inFlightLimit() (int, error) {
	v, ok := os.LookupEnv("METRICFLOW_MAX_INFLIGHT")
	if !ok {
		return maxInFlight, nil
	}

	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("METRICFLOW_MAX_INFLIGHT=%q is not an integer", v)
	}
	if n < 1 {
		return 0, fmt.Errorf("METRICFLOW_MAX_INFLIGHT=%d must be at least 1; "+
			"zero would shed every request", n)
	}
	return n, nil
}

// limiter bounds concurrent in-flight requests and refuses the excess.
//
// The decision it encodes is shed, don't queue. Under sustained overload a
// queue does not reduce the work, it only hides it: latency grows without
// bound, every client waits longer for a response it will eventually time
// out on, and the server spends its capacity on requests nobody is still
// listening for. Shedding keeps the requests it does serve fast and hands
// the rest an immediate, honest answer they can act on - which for a
// metrics reporter means "retry, or drop this interval", both far better
// than hanging.
//
// The slots channel is a counting semaphore. The select below never blocks:
// either a slot is free right now, or the request is refused.
type limiter struct {
	slots chan struct{}
	shed  atomic.Int64
}

func newLimiter(n int) *limiter {
	return &limiter{slots: make(chan struct{}, n)}
}

// limit wraps h so at most n requests are inside it at once.
func (l *limiter) limit(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case l.slots <- struct{}{}:
			// Deferred, so the slot comes back even if the handler panics.
			// A leaked slot is permanent: the server would shed a little
			// more after every panic until it shed everything.
			defer func() { <-l.slots }()
			h.ServeHTTP(w, r)
		default:
			l.shed.Add(1)
			// Retry-After is the part that makes this cooperative rather
			// than just a refusal - it tells a client to back off instead
			// of immediately trying again and deepening the overload.
			w.Header().Set("Retry-After", "1")
			http.Error(w, "server at capacity", http.StatusServiceUnavailable)
		}
	})
}

// shedded is how many requests have been refused for capacity. Exported to
// tests rather than to clients: it is a property of the server, not of the
// metrics, and putting it in /stats would mix the two.
func (l *limiter) shedded() int64 { return l.shed.Load() }
