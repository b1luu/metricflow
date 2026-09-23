package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// --- parsing the header ---

func requestAs(id string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/stats", nil)
	if id != "" {
		r.Header.Set(clientHeader, id)
	}
	return r
}

func TestParseClientID(t *testing.T) {
	cases := []struct {
		name    string
		header  string
		want    string
		wantErr string
	}{
		{name: "absent falls back to the shared bucket", want: defaultClient},
		{name: "whitespace only is absent", header: "   ", want: defaultClient},
		{name: "plain", header: "api-gateway", want: "api-gateway"},
		{name: "trimmed", header: "  api-gateway  ", want: "api-gateway"},
		{name: "the full allowed charset", header: "svc.api-01_west:v2", want: "svc.api-01_west:v2"},
		{name: "exactly at the length limit", header: strings.Repeat("a", maxClientIDLen), want: strings.Repeat("a", maxClientIDLen)},

		{name: "one byte over", header: strings.Repeat("a", maxClientIDLen+1), wantErr: "over the"},
		{name: "a space inside", header: "api gateway", wantErr: "contains"},
		{name: "a slash", header: "api/gateway", wantErr: "contains"},
		{name: "a newline", header: "api\ngateway", wantErr: "contains"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := parseClientID(requestAs(c.header))
			if c.wantErr != "" {
				if err == nil {
					t.Fatalf("accepted %q as %q, want an error", c.header, got)
				}
				if !strings.Contains(err.Error(), c.wantErr) {
					t.Errorf("error = %q, want it to contain %q", err, c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("%q: %v", c.header, err)
			}
			if got != c.want {
				t.Errorf("got %q, want %q", got, c.want)
			}
		})
	}
}

// A malformed identifier is rejected rather than quietly demoted to the
// shared bucket. A client that sent an ID and was silently pooled believes it
// has a budget it does not have, and will be baffled when someone else's
// traffic throttles it.
func TestIdentifyRejectsAMalformedID(t *testing.T) {
	cs := newClients()
	reached := false
	h := cs.identify(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached = true }))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, requestAs("bad id"))

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
	if reached {
		t.Error("the handler ran despite a rejected identity")
	}
	if got := cs.tracked(); got != 0 {
		t.Errorf("a rejected identity created %d table entries, want 0", got)
	}
}

// --- the table ---

// The same identity must resolve to the same state, or per-client counters
// count nothing.
func TestClientsTableIsStablePerIdentity(t *testing.T) {
	cs := newClients()

	first := cs.get("api")
	for i := 0; i < 100; i++ {
		if got := cs.get("api"); got != first {
			t.Fatal("the same identity resolved to two different clients")
		}
	}
	if cs.get("other") == first {
		t.Error("two identities share one client")
	}
	if got := cs.tracked(); got != 2 {
		t.Errorf("tracked = %d, want 2", got)
	}
}

// This is §27's problem one level up, and it has to be handled: the
// identifier comes from the caller, so an unbounded set of IDs is an
// unbounded set of counters. Past the cap everyone shares one bucket, which
// degrades to exactly the behaviour the server had before per-client limits.
func TestClientsTableIsBoundedAndOverflowsToOneBucket(t *testing.T) {
	cs := newClients()

	for i := 0; i < maxClients*4; i++ {
		cs.get(fmt.Sprintf("rotating-%06d", i))
	}

	if got := cs.tracked(); got > maxClients+1 {
		t.Errorf("tracked = %d after %d distinct IDs; the table is unbounded",
			got, maxClients*4)
	}

	// And everyone past the cap really does share, rather than each getting
	// a fresh budget - which is the whole point of bounding it.
	a := cs.get("late-arrival-a")
	b := cs.get("late-arrival-b")
	if a != b {
		t.Error("two clients past the cap got separate state; the cap buys nothing")
	}
}

// A rotating attacker must not be able to degrade the honest callers who
// simply sent no header. That is why overflow is its own bucket rather than
// the default one.
func TestOverflowDoesNotPoisonTheAnonymousBucket(t *testing.T) {
	cs := newClients()

	anon := cs.get(defaultClient)
	for i := 0; i < maxClients*2; i++ {
		cs.get(fmt.Sprintf("rotating-%06d", i))
	}

	if cs.get(defaultClient) != anon {
		t.Error("the anonymous bucket was displaced by rotating IDs")
	}
	if cs.get("someone-new") == anon {
		t.Error("overflowing clients landed in the anonymous bucket")
	}
}

// The table is touched once per request from every connection at once.
func TestClientsTableUnderConcurrentLookups(t *testing.T) {
	const (
		goroutines = 32
		lookups    = 500
	)

	cs := newClients()
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < lookups; i++ {
				// A mix of a few shared identities and many distinct ones,
				// so the table both contends and fills.
				if i%2 == 0 {
					cs.get(fmt.Sprintf("shared-%d", i%4))
				} else {
					cs.get(fmt.Sprintf("distinct-%d-%d", g, i))
				}
			}
		}(g)
	}
	wg.Wait()

	if got := cs.tracked(); got > maxClients+1 {
		t.Errorf("tracked = %d, past the cap of %d", got, maxClients)
	}
}

// --- plumbing ---

// The limiter and the ingest path have to agree on who the caller is, which
// they cannot do if each works it out separately - so it is resolved once and
// carried on the request.
func TestIdentifyPutsTheClientOnTheRequest(t *testing.T) {
	cs := newClients()

	var gotID string
	var gotClient *client
	h := cs.identify(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		gotClient, gotID = clientFrom(r.Context())
	}))

	h.ServeHTTP(httptest.NewRecorder(), requestAs("api-gateway"))

	if gotID != "api-gateway" {
		t.Errorf("id = %q, want %q", gotID, "api-gateway")
	}
	if gotClient == nil {
		t.Fatal("no client on the request context")
	}
	if gotClient != cs.get("api-gateway") {
		t.Error("the handler got a different client than the table holds")
	}
}

// A request that never passed through identify - an internal call, or a test
// driving a handler directly - is unmetered rather than broken. The global
// limits still apply, so the fallback is the old behaviour, not an escape.
func TestClientFromAnUnidentifiedRequestIsNil(t *testing.T) {
	c, id := clientFrom(httptest.NewRequest(http.MethodGet, "/stats", nil).Context())
	if c != nil {
		t.Error("an unidentified request produced a client")
	}
	if id != defaultClient {
		t.Errorf("id = %q, want %q", id, defaultClient)
	}
	// And every limit must tolerate that nil.
	if !c.allowNewName(time.Now()) {
		t.Error("a nil client was refused a new name; internal callers would break")
	}
}

// /health is outside identify as well as outside the limiter. A malformed
// header from a sidecar must not make health checks fail, or an orchestrator
// kills a server that is serving everything else perfectly well (§26).
func TestHealthIgnoresAMalformedClientID(t *testing.T) {
	s := newStore()
	a, err := newAlerter(time.Now(), s, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := routes(s, a, newLimiter(maxInFlight), newClients())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set(clientHeader, "not a valid id")
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("/health with a malformed %s: %d, want 200", clientHeader, rec.Code)
	}

	// But the routes that are identified do reject it, or the exemption
	// above would be proving nothing about identify being wired in at all.
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/stats", nil)
	req.Header.Set(clientHeader, "not a valid id")
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("/stats with a malformed %s: %d, want 400", clientHeader, rec.Code)
	}
}
