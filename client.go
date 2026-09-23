package main

// Client identity, and the fairness it buys.
//
// Three sections have now ended with the same admission. §26's shed slots are
// global, so one noisy client can consume all 256 and shed everyone else.
// §27's cardinality budget is shared, so one client's names can fill a shard
// another client's legitimate metric then cannot enter. Both said the fix
// needs a notion of client identity, which the server did not have. This is
// that notion.
//
// What it is for, and what it is not for.
//
// The demo scenario is a fleet of services under one operator, not a set of
// mutually distrusting tenants. So the failure worth preventing is an
// *accident*: one buggy service emitting unbounded names, or one runaway
// reporter opening hundreds of connections, degrading observability of every
// other service at exactly the moment somebody needs it. That calls for
// resource fairness. It does not call for data isolation - the metric
// namespace stays shared and global on purpose, because a fleet's metrics are
// meant to be queried together.
//
// The identity is self-asserted, which is the same contract Cortex and Mimir
// give X-Scope-OrgID: a tenancy boundary, not an authentication boundary. It
// isolates clients that are honest about who they are, which is every client
// that is merely broken. A deployment facing untrusted callers must set this
// header at a trusted proxy and strip whatever the caller sent.
//
// Saying that plainly matters more than it might seem, because the limits
// below would otherwise look like a defence they are not. An attacker who
// rotates the header simply gets a fresh budget each time - so the tracking
// table is bounded, and past that bound everyone shares one bucket. Under
// that attack the server degrades to precisely the global limits it had
// before this file existed, which is the honest ceiling of what an
// unauthenticated identity can promise.

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// clientHeader is how a caller says who it is.
	clientHeader = "X-Client-ID"

	// defaultClient is the bucket for callers that do not say. They share
	// it, which is the pre-existing behaviour and a fair default: a client
	// that wants its own budget can have one by asking.
	defaultClient = "anonymous"

	// overflowClient is where callers land once the tracking table is full.
	// Separate from defaultClient on purpose - a client rotating IDs should
	// not be able to degrade the honest callers who simply sent no header.
	overflowClient = "overflow"

	// maxClientIDLen bounds one identifier, for the same reason
	// maxMetricNameLen bounds one metric name (§27): a cap on how many
	// exist says nothing about how large each one is.
	maxClientIDLen = 64

	// maxClients bounds how many distinct identities are tracked.
	//
	// This is §27's problem again one level up, and it has to be, because
	// the identifier is supplied by the caller: an unbounded set of client
	// IDs is an unbounded set of counters. Past this, callers share the
	// overflow bucket and the server behaves exactly as it did before
	// per-client limits existed.
	maxClients = 256

	// perClientInFlight caps concurrent requests from one client.
	//
	// It sits well below maxInFlight (§26) rather than dividing it: the
	// point is not to partition capacity, which would waste it whenever
	// fewer clients are active, but to stop any one client from taking
	// enough of the shared pool to starve the rest. At 32 of 256, eight
	// clients would have to misbehave at once before anyone else is shed.
	perClientInFlight = 32

	// maxNewNamesPerEpoch caps how fast one client may introduce metric
	// names, measured over nameEpoch.
	//
	// A *rate* rather than a stock, which is the design decision here. A
	// per-client stock limit would need to know which client created each
	// name, so the sweeper could give the budget back - an ownership field
	// on every metric, and a store restructure to carry it. A rate needs
	// one counter per client and nothing on the metric at all.
	//
	// It composes with §27 rather than replacing it: the global cap still
	// bounds absolute memory, and this bounds how fast any one client can
	// consume it. A service with a stable set of names creates nothing
	// after startup and never touches this. A service emitting a name per
	// request is throttled to 64 per 10s, so it would need over an hour of
	// sustained flooding to reach the global ceiling on its own - by which
	// time the sweeper has long since reclaimed its earlier names.
	maxNewNamesPerEpoch = 64
	nameEpoch           = bucketWidth
)

// parseClientID returns the caller's identity, or an error if the header is
// present but unusable.
//
// Rejecting rather than falling back to the default is the same call §11,
// §25 and §28 made: a client that sent an identifier and was silently put in
// the shared bucket believes it has a budget it does not have, and will be
// baffled when someone else's traffic gets it throttled.
func parseClientID(r *http.Request) (string, error) {
	raw := strings.TrimSpace(r.Header.Get(clientHeader))
	if raw == "" {
		return defaultClient, nil
	}
	if len(raw) > maxClientIDLen {
		return "", fmt.Errorf("%s is %d bytes, over the %d-byte limit",
			clientHeader, len(raw), maxClientIDLen)
	}
	for _, c := range raw {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '.', c == '-', c == '_', c == ':':
		default:
			return "", fmt.Errorf("%s contains %q; allowed are letters, digits and .-_:",
				clientHeader, c)
		}
	}
	return raw, nil
}

// client is one caller's share of the limits.
type client struct {
	inFlight atomic.Int64

	// Guarded by mu rather than atomics because the counter and the epoch
	// it belongs to have to change together. The cost is irrelevant: this
	// is only touched when a metric name is created, which is rare by
	// definition - an established client creates none at all.
	mu      sync.Mutex
	epoch   int64
	created int64
}

// acquire takes one of this client's in-flight slots, reporting whether it
// got one.
//
// The optimistic add-then-back-out never admits past the cap: two callers
// racing get distinct values, and any caller whose value exceeds the limit
// undoes its own increment.
func (c *client) acquire() bool {
	if c.inFlight.Add(1) > perClientInFlight {
		c.inFlight.Add(-1)
		return false
	}
	return true
}

func (c *client) release() { c.inFlight.Add(-1) }

// allowNewName reports whether this client may introduce another metric name
// now, charging it if so.
//
// A nil client is unmetered, which is what internal callers and tests get:
// they are subject to the global cardinality cap (§27) and nothing else.
func (c *client) allowNewName(now time.Time) bool {
	if c == nil {
		return true
	}

	epoch := now.Truncate(nameEpoch).Unix()

	c.mu.Lock()
	defer c.mu.Unlock()

	// The window is derived from the clock rather than reset by a ticker,
	// so there is no third background loop to run, supervise and shut down
	// - and a client that goes quiet needs no cleanup to stop counting.
	if c.epoch != epoch {
		c.epoch, c.created = epoch, 0
	}
	if c.created >= maxNewNamesPerEpoch {
		return false
	}
	c.created++
	return true
}

// clients is the bounded table of per-client state.
type clients struct {
	mu   sync.Mutex
	byID map[string]*client
}

func newClients() *clients {
	return &clients{byID: make(map[string]*client, maxClients)}
}

// get returns the state for an identity, creating it if there is room and
// falling back to the shared overflow bucket if there is not.
//
// One mutex over the whole table, deliberately. It is taken once per
// request, where §24 measured the contention that matters as one per *event*
// - and a batch of 10000 events takes it once. Sharding this would be
// optimising the wrong thing.
func (cs *clients) get(id string) *client {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if c, ok := cs.byID[id]; ok {
		return c
	}
	if len(cs.byID) >= maxClients {
		id = overflowClient
		if c, ok := cs.byID[id]; ok {
			return c
		}
	}

	c := &client{}
	cs.byID[id] = c
	return c
}

// tracked is how many identities the table holds. For tests and for the
// operator-facing question "am I at the cap".
func (cs *clients) tracked() int {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return len(cs.byID)
}

// clientKey is the context key for the caller's state. An unexported empty
// struct, so nothing outside this file can collide with it.
type clientKey struct{}

type clientInfo struct {
	c  *client
	id string
}

func withClient(ctx context.Context, c *client, id string) context.Context {
	return context.WithValue(ctx, clientKey{}, clientInfo{c: c, id: id})
}

// clientFrom returns the caller's state and identity.
//
// A request that did not pass through identify - an internal call, or a test
// driving a handler directly - yields a nil client, which every limit here
// treats as unmetered. That is deliberate: the global limits still apply, so
// the fallback is the behaviour the server had before this file, not an
// escape from all bounds.
func clientFrom(ctx context.Context) (*client, string) {
	info, _ := ctx.Value(clientKey{}).(clientInfo)
	if info.c == nil {
		return nil, defaultClient
	}
	return info.c, info.id
}

// identify is middleware that resolves the caller's identity once and hands
// it to everything downstream through the request context.
//
// Resolved once per request rather than per handler, because parsing and the
// table lookup should not happen twice for one request - and because the
// limiter and the ingest path must agree on who the caller is, which they
// cannot do if each works it out separately.
func (cs *clients) identify(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, err := parseClientID(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		h.ServeHTTP(w, r.WithContext(withClient(r.Context(), cs.get(id), id)))
	})
}
