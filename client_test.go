package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
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

// --- per-client concurrency ---

// blockingRoutes returns a handler stack whose requests park until released,
// so a test can hold a precise number of them in flight.
func blockingRoutes(t *testing.T, global int) (h http.Handler, l *limiter,
	entered chan string, release chan struct{}) {
	t.Helper()

	entered = make(chan string, 1024)
	release = make(chan struct{})
	l = newLimiter(global)
	cs := newClients()

	h = cs.identify(l.limit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, id := clientFrom(r.Context())
		entered <- id
		<-release
		w.WriteHeader(http.StatusOK)
	})))
	return h, l, entered, release
}

// The gap §26 left open, now closed: one client at its own ceiling must not
// cost anyone else anything. This is the test the whole slice exists for.
func TestANoisyClientCannotStarveAQuietOne(t *testing.T) {
	h, l, entered, release := blockingRoutes(t, maxInFlight)

	call := func(id string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/stats", nil)
		req.Header.Set(clientHeader, id)
		h.ServeHTTP(rec, req)
		return rec
	}

	// Fill the noisy client's whole share and hold it there.
	var wg sync.WaitGroup
	for i := 0; i < perClientInFlight; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if rec := call("noisy"); rec.Code != http.StatusOK {
				t.Errorf("a request within the noisy client's share got %d, want 200", rec.Code)
			}
		}()
	}
	for i := 0; i < perClientInFlight; i++ {
		<-entered
	}

	// Its next request is refused - and refused as 429, not 503: the server
	// is nowhere near capacity, this client is over its share.
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- call("noisy") }()
	select {
	case rec := <-done:
		if rec.Code != http.StatusTooManyRequests {
			t.Errorf("the noisy client past its share got %d, want 429", rec.Code)
		}
		if rec.Header().Get("Retry-After") == "" {
			t.Error("no Retry-After on a throttled request")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the request past the per-client share blocked instead of being refused")
	}

	// And the quiet client sails straight through, which is the point.
	quiet := make(chan *httptest.ResponseRecorder, 1)
	go func() { quiet <- call("quiet") }()
	select {
	case <-entered: // it reached the handler
	case rec := <-quiet:
		t.Fatalf("the quiet client was refused with %d while another client misbehaved", rec.Code)
	case <-time.After(2 * time.Second):
		t.Fatal("the quiet client never reached the handler")
	}

	if l.shedded() != 0 {
		t.Errorf("shedded = %d; nothing should have hit the global limit", l.shedded())
	}
	if l.throttledCount() != 1 {
		t.Errorf("throttled = %d, want 1", l.throttledCount())
	}

	close(release)
	wg.Wait()
	<-quiet
}

// A client's share comes back when its requests finish, or the limit is a
// one-way door that throttles harder over time.
func TestAClientsShareIsReturned(t *testing.T) {
	h, _, entered, release := blockingRoutes(t, maxInFlight)

	call := func() *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/stats", nil)
		req.Header.Set(clientHeader, "api")
		h.ServeHTTP(rec, req)
		return rec
	}

	var wg sync.WaitGroup
	for i := 0; i < perClientInFlight; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); call() }()
	}
	for i := 0; i < perClientInFlight; i++ {
		<-entered
	}
	close(release)
	wg.Wait()

	// Every slot is back, so a fresh request is admitted rather than 429'd.
	release = make(chan struct{})
	close(release)
	if rec := call(); rec.Code != http.StatusOK {
		t.Errorf("after the client's requests finished it got %d, want 200", rec.Code)
	}
}

// A leaked per-client slot is permanent, exactly like a leaked shared one:
// the client would be throttled a little harder after every panic until it
// was throttled always.
func TestAClientsSlotSurvivesAPanickingHandler(t *testing.T) {
	cs := newClients()
	l := newLimiter(maxInFlight)
	h := cs.identify(l.limit(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	})))

	call := func() (rec *httptest.ResponseRecorder, panicked bool) {
		rec = httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/stats", nil)
		req.Header.Set(clientHeader, "api")
		defer func() { panicked = recover() != nil }()
		h.ServeHTTP(rec, req)
		return rec, false
	}

	// More panics than the client's whole share, so a leak would exhaust it.
	for i := 0; i <= perClientInFlight; i++ {
		if _, panicked := call(); !panicked {
			t.Fatalf("call %d did not panic; the test proves nothing", i)
		}
	}
	if got := cs.get("api").inFlight.Load(); got != 0 {
		t.Errorf("the client holds %d slots after panicking handlers, want 0", got)
	}
}

// Enough distinct clients still exhaust the shared pool, and that must read
// as a 503 rather than a 429: the server really is at capacity and no single
// client is at fault.
func TestEnoughClientsStillHitTheGlobalLimit(t *testing.T) {
	const global = 4

	h, l, entered, release := blockingRoutes(t, global)

	call := func(id string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/stats", nil)
		req.Header.Set(clientHeader, id)
		h.ServeHTTP(rec, req)
		return rec
	}

	// One request each from `global` different clients, so nobody is near
	// their own share but the pool is full.
	var wg sync.WaitGroup
	for i := 0; i < global; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			call(fmt.Sprintf("client-%d", i))
		}(i)
	}
	for i := 0; i < global; i++ {
		<-entered
	}

	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- call("one-more") }()
	select {
	case rec := <-done:
		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("status = %d, want 503 - the server is at capacity, not this client", rec.Code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the request blocked instead of being shed")
	}

	if l.throttledCount() != 0 {
		t.Errorf("throttled = %d; no client was over its own share", l.throttledCount())
	}
	if l.shedded() != 1 {
		t.Errorf("shedded = %d, want 1", l.shedded())
	}

	close(release)
	wg.Wait()
}

// --- per-client name budget ---

// The gap §27 left open: its cap is global, so one client's names could fill
// a shard a different client's legitimate metric then could not enter. A rate
// per client is what stops that, without needing an owner recorded on every
// metric so the sweeper could give the budget back.
func TestAClientIsThrottledAfterItsNameAllowance(t *testing.T) {
	s := newStore()
	c := &client{}
	now := time.Now()
	ts := now.UnixMilli()

	for i := 0; i < maxNewNamesPerEpoch; i++ {
		ev := Event{Name: fmt.Sprintf("flood.%04d", i), Value: 1, TS: ts}
		if err := s.recordFor(now, c, ev); err != nil {
			t.Fatalf("name %d of its allowance was refused: %v", i+1, err)
		}
	}

	ev := Event{Name: "flood.one-too-many", Value: 1, TS: ts}
	if err := s.recordFor(now, c, ev); !errors.Is(err, errClientNames) {
		t.Fatalf("the name past the allowance returned %v, want errClientNames", err)
	}
	if got := metricCount(s); got != maxNewNamesPerEpoch {
		t.Errorf("store holds %d metrics, want exactly the allowance %d", got, maxNewNamesPerEpoch)
	}
}

// The limit is on *new* names. A client that has run out of allowance must
// still be able to write to metrics it already has, or a brief flood would
// silence a service's real metrics too.
func TestAThrottledClientStillWritesToItsExistingMetrics(t *testing.T) {
	s := newStore()
	c := &client{}
	now := time.Now()
	ts := now.UnixMilli()

	for i := 0; i < maxNewNamesPerEpoch; i++ {
		if err := s.recordFor(now, c, Event{Name: fmt.Sprintf("svc.%04d", i), Value: 1, TS: ts}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.recordFor(now, c, Event{Name: "svc.new", Value: 1, TS: ts}); err == nil {
		t.Fatal("setup: the client is not actually out of allowance")
	}

	for i := 0; i < maxNewNamesPerEpoch; i++ {
		name := fmt.Sprintf("svc.%04d", i)
		if err := s.recordFor(now, c, Event{Name: name, Value: 3, TS: ts}); err != nil {
			t.Fatalf("%s: an existing metric was refused: %v", name, err)
		}
	}
	got, ok := mergeAll(s, "svc.0000")
	if !ok || got.Count != 2 || got.Sum != 4 {
		t.Errorf("svc.0000 = %+v (ok=%v), want Count=2 Sum=4", got, ok)
	}
}

// The allowance is a rate, so it comes back. A stock limit would need the
// sweeper to know who created each name; this just needs the clock to move.
func TestTheNameAllowanceRefillsNextEpoch(t *testing.T) {
	s := newStore()
	c := &client{}
	now := time.Now().Truncate(nameEpoch)

	for i := 0; i < maxNewNamesPerEpoch; i++ {
		ev := Event{Name: fmt.Sprintf("a.%04d", i), Value: 1, TS: now.UnixMilli()}
		if err := s.recordFor(now, c, ev); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.recordFor(now, c, Event{Name: "a.extra", Value: 1, TS: now.UnixMilli()}); err == nil {
		t.Fatal("setup: the allowance was not exhausted")
	}

	later := now.Add(nameEpoch)
	if err := s.recordFor(later, c, Event{Name: "a.extra", Value: 1, TS: later.UnixMilli()}); err != nil {
		t.Errorf("the allowance did not refill in the next epoch: %v", err)
	}
}

// One client's flood must not spend another client's allowance - that is the
// entire point, and it is what §27 could not offer.
func TestOneClientsFloodDoesNotSpendAnothersAllowance(t *testing.T) {
	s := newStore()
	noisy, quiet := &client{}, &client{}
	now := time.Now()
	ts := now.UnixMilli()

	for i := 0; i < maxNewNamesPerEpoch*4; i++ {
		_ = s.recordFor(now, noisy, Event{Name: fmt.Sprintf("noisy.%05d", i), Value: 1, TS: ts})
	}

	// The quiet client introduces its whole allowance, untouched.
	for i := 0; i < maxNewNamesPerEpoch; i++ {
		ev := Event{Name: fmt.Sprintf("quiet.%04d", i), Value: 1, TS: ts}
		if err := s.recordFor(now, quiet, ev); err != nil {
			t.Fatalf("the quiet client's name %d was refused while another client flooded: %v", i+1, err)
		}
	}
}

// A malformed event must consume neither budget. Otherwise a client could
// spend its own allowance - and the store's - on names that were never going
// to be valid, which is the cheapest denial of service available.
func TestInvalidEventsSpendNoClientAllowance(t *testing.T) {
	s := newStore()
	c := &client{}
	now := time.Now()

	for i := 0; i < maxNewNamesPerEpoch*2; i++ {
		ev := Event{Name: fmt.Sprintf("never.%04d", i), Value: 1,
			TS: now.Add(-window - time.Hour).UnixMilli()}
		if err := s.admitFor(now, c, ev); err == nil {
			t.Fatalf("event %d was admitted despite an expired ts", i)
		}
	}

	// The whole allowance is still there.
	for i := 0; i < maxNewNamesPerEpoch; i++ {
		ev := Event{Name: fmt.Sprintf("valid.%04d", i), Value: 1, TS: now.UnixMilli()}
		if err := s.admitFor(now, c, ev); err != nil {
			t.Fatalf("valid name %d was refused: %v", i+1, err)
		}
	}
}

// Over HTTP it reads as 429 with Retry-After, like the cardinality cap - and
// in a batch it is a per-event rejection marked retryable, because unlike a
// contract violation it will succeed on its own once the epoch rolls.
func TestTheNameAllowanceOverHTTP(t *testing.T) {
	s := newStore()
	a, err := newAlerter(time.Now(), s, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := routes(s, a, newLimiter(maxInFlight), newClients())

	post := func(path, body string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		req.Header.Set(clientHeader, "flooder")
		h.ServeHTTP(rec, req)
		return rec
	}

	now := time.Now()
	for i := 0; i < maxNewNamesPerEpoch; i++ {
		body := fmt.Sprintf(`{"name":"http.%04d","value":1,"ts":%d}`, i, now.UnixMilli())
		if rec := post("/ingest", body); rec.Code != http.StatusOK {
			t.Fatalf("name %d: status %d, want 200 (%s)", i, rec.Code, rec.Body.String())
		}
	}

	rec := post("/ingest", fmt.Sprintf(`{"name":"http.extra","value":1,"ts":%d}`, now.UnixMilli()))
	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("no Retry-After on a throttled name")
	}

	// And in a batch, marked retryable rather than looking like a contract
	// violation the client should give up on.
	batch := fmt.Sprintf(`{"name":"http.batch","value":1,"ts":%d}`, now.UnixMilli()) + "\n"
	rec = post("/ingest/batch", batch)

	var resp BatchResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("batch reply is not JSON: %v (%s)", err, rec.Body.String())
	}
	if resp.Rejected != 1 || len(resp.Errors) != 1 {
		t.Fatalf("accepted/rejected = %d/%d with %d errors, want 0/1 and 1",
			resp.Accepted, resp.Rejected, len(resp.Errors))
	}
	if !resp.Errors[0].Retryable {
		t.Errorf("a name-allowance rejection is not marked retryable: %+v", resp.Errors[0])
	}
}

// Requests that never passed through identify are unmetered by the client
// budget - they are subject to the global cap and nothing else, which is the
// behaviour every existing test and internal caller relies on.
func TestAnUnidentifiedCallerHasNoNameAllowance(t *testing.T) {
	s := newStore()
	now := time.Now()

	for i := 0; i < maxNewNamesPerEpoch*3; i++ {
		ev := Event{Name: fmt.Sprintf("internal.%05d", i), Value: 1, TS: now.UnixMilli()}
		if err := s.record(now, ev); err != nil {
			t.Fatalf("internal caller was throttled at name %d: %v", i+1, err)
		}
	}
}

// --- per-client limits under concurrency ---

// The allowance is a check followed by an increment, which is the same
// check-then-act race §27's cap had: without one lock over both, N
// goroutines could each see room for one more and each take it. The clock is
// fixed so a real epoch boundary cannot roll mid-test and make the expected
// count ambiguous.
func TestTheNameAllowanceIsExactUnderConcurrentCreation(t *testing.T) {
	const writers = 32

	s := newStore()
	c := &client{}
	now := time.Now().Truncate(nameEpoch)
	ts := now.UnixMilli()

	var (
		created   atomic.Int64
		throttled atomic.Int64
		wg        sync.WaitGroup
		failures  = make(chan string, 16)
	)

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			// Far more attempts than the allowance, interleaved across
			// writers so they contend for the last few slots.
			for i := w; i < maxNewNamesPerEpoch*8; i += writers {
				err := s.recordFor(now, c, Event{
					Name: fmt.Sprintf("race.%05d", i), Value: 1, TS: ts})
				switch {
				case err == nil:
					created.Add(1)
				case errors.Is(err, errClientNames):
					throttled.Add(1)
				default:
					select {
					case failures <- fmt.Sprintf("unexpected error: %v", err):
					default:
					}
				}
			}
		}(w)
	}
	wg.Wait()
	close(failures)

	for msg := range failures {
		t.Error(msg)
	}
	if got := created.Load(); got != maxNewNamesPerEpoch {
		t.Errorf("created %d names, want exactly the allowance %d - the check and "+
			"the increment are not atomic", got, maxNewNamesPerEpoch)
	}
	if got := metricCount(s); int64(got) != created.Load() {
		t.Errorf("recordFor reported %d creations but the store holds %d metrics",
			created.Load(), got)
	}
	if throttled.Load() == 0 {
		t.Fatal("nothing was throttled; the test never reached the allowance")
	}
}

// Each client's allowance is its own, under contention as much as in
// isolation - the property the whole slice exists for, with the race added.
func TestEveryClientGetsItsOwnAllowanceConcurrently(t *testing.T) {
	const (
		clientCount = 8
		writers     = 4
	)

	s := newStore()
	now := time.Now().Truncate(nameEpoch)
	ts := now.UnixMilli()

	cs := make([]*client, clientCount)
	created := make([]atomic.Int64, clientCount)
	for i := range cs {
		cs[i] = &client{}
	}

	var wg sync.WaitGroup
	for ci := range cs {
		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func(ci, w int) {
				defer wg.Done()
				for i := w; i < maxNewNamesPerEpoch*4; i += writers {
					ev := Event{Name: fmt.Sprintf("c%02d.name.%05d", ci, i), Value: 1, TS: ts}
					if err := s.recordFor(now, cs[ci], ev); err == nil {
						created[ci].Add(1)
					}
				}
			}(ci, w)
		}
	}
	wg.Wait()

	for i := range cs {
		if got := created[i].Load(); got != maxNewNamesPerEpoch {
			t.Errorf("client %d created %d names, want its full allowance %d",
				i, got, maxNewNamesPerEpoch)
		}
	}
}

// The cap refuses, deterministically. Asserted from one goroutine because a
// concurrent version of this claim is machine-dependent: on a two-core runner
// 128 goroutines that acquire and release immediately never hold more than
// two slots at once, so nothing is refused and the test passes while proving
// nothing. That is exactly how the first version of this failed in CI.
func TestAClientIsRefusedPastItsCap(t *testing.T) {
	c := &client{}

	for i := 0; i < perClientInFlight; i++ {
		if !c.acquire() {
			t.Fatalf("slot %d of %d was refused", i+1, perClientInFlight)
		}
	}
	if c.acquire() {
		t.Fatalf("a %dth slot was admitted past a cap of %d",
			perClientInFlight+1, perClientInFlight)
	}

	// And it comes back: releasing one admits exactly one more.
	c.release()
	if !c.acquire() {
		t.Error("a released slot was not reusable")
	}
	if c.acquire() {
		t.Error("releasing one slot admitted two")
	}
}

// And the counter holds up under contention. This asserts only what a race
// can prove regardless of how much parallelism the machine actually offers:
// the cap is never exceeded, and every slot comes back. Whether anything is
// refused depends on scheduling, so it is not asserted here - the test above
// owns that claim.
func TestPerClientInFlightNeverExceedsItsCap(t *testing.T) {
	const goroutines = 64

	c := &client{}
	var (
		peak atomic.Int64
		held atomic.Int64
		wg   sync.WaitGroup
	)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				if !c.acquire() {
					continue
				}
				n := held.Add(1)
				for {
					old := peak.Load()
					if n <= old || peak.CompareAndSwap(old, n) {
						break
					}
				}
				// Hold long enough to overlap with somebody, without
				// depending on it.
				runtime.Gosched()

				held.Add(-1)
				c.release()
			}
		}()
	}
	wg.Wait()

	if got := peak.Load(); got > perClientInFlight {
		t.Errorf("peak concurrent holders = %d, past the cap of %d", got, perClientInFlight)
	}
	if got := c.inFlight.Load(); got != 0 {
		t.Errorf("client holds %d slots after everything finished, want 0", got)
	}
}

// The standing invariant, with the per-client limits firing throughout: the
// events the server says it took are exactly the events it holds.
func TestExactnessHoldsWhileClientsAreThrottled(t *testing.T) {
	const (
		clientCount = 4
		writers     = 8
		perWriter   = 300
	)

	s := newStore()
	now := time.Now().Truncate(nameEpoch)
	ts := now.UnixMilli()

	cs := make([]*client, clientCount)
	for i := range cs {
		cs[i] = &client{}
	}

	var (
		accepted atomic.Int64
		wg       sync.WaitGroup
	)
	for ci := range cs {
		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func(ci, w int) {
				defer wg.Done()
				for i := 0; i < perWriter; i++ {
					// Half the traffic re-uses an early name, half invents
					// one, so the allowance fires partway through.
					var name string
					if i%2 == 0 {
						name = fmt.Sprintf("c%02d.stable.%02d", ci, i%16)
					} else {
						name = fmt.Sprintf("c%02d.fresh.%05d", ci, w*perWriter+i)
					}
					if err := s.recordFor(now, cs[ci], Event{Name: name, Value: 1, TS: ts}); err == nil {
						accepted.Add(1)
					}
				}
			}(ci, w)
		}
	}
	wg.Wait()

	recorded := 0
	for _, series := range storeDump(s) {
		agg, ok := mergeBuckets(series, 0)
		if !ok {
			continue
		}
		recorded += agg.Count
	}
	if int64(recorded) != accepted.Load() {
		t.Errorf("recordFor accepted %d events, store holds %d", accepted.Load(), recorded)
	}
}

// --- the query budget ---

// statsAs runs one /stats as a given client and returns the response.
func statsAs(t *testing.T, s *Store, c *client, query string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/stats"+query, nil)
	req = req.WithContext(withClient(req.Context(), c, "tester"))
	rec := httptest.NewRecorder()
	s.handleStats(rec, req)
	return rec
}

// The claim §29 deferred: a client is charged for the work its queries
// caused, not for the number of requests it made. Same request count, two
// very different bills.
func TestQueryCostFollowsWorkNotRequests(t *testing.T) {
	s := storeWithMetrics(2000)

	wide := &client{}
	statsAs(t, s, wide, "?limit=1000")

	narrow := &client{}
	statsAs(t, s, narrow, "?prefix=svc.metric.42&limit=1")

	wide.mu.Lock()
	wideSpent := wide.querySpent
	wide.mu.Unlock()

	narrow.mu.Lock()
	narrowSpent := narrow.querySpent
	narrow.mu.Unlock()

	if narrowSpent >= wideSpent {
		t.Errorf("one narrow query cost %d and one wide query cost %d; "+
			"the budget is counting requests, not work", narrowSpent, wideSpent)
	}
	// The wide query computed 1000 metrics, so the bill has to be of that
	// order rather than a token charge.
	if wideSpent < 1000 {
		t.Errorf("a 1000-metric query cost %d metric-equivalents, want at least 1000", wideSpent)
	}
}

// The hole that charging only for returned metrics would leave: a prefix
// matching nothing still walks the whole store, and still has to be paid
// for.
func TestAScanThatMatchesNothingStillCosts(t *testing.T) {
	s := storeWithMetrics(2000)

	c := &client{}
	statsAs(t, s, c, "?prefix=nothing.matches.this")

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.querySpent <= 0 {
		t.Errorf("a full-store scan returning nothing cost %d; "+
			"an empty result is not free work", c.querySpent)
	}
}

// Spend the budget and the next query is refused, with the status §29 uses
// for a client over its own share rather than §26's server-at-capacity.
func TestAClientOverItsQueryBudgetIsRefused(t *testing.T) {
	s := storeWithMetrics(2000)
	c := &client{}

	var refused *httptest.ResponseRecorder
	for i := 0; i < 200; i++ {
		rec := statsAs(t, s, c, "?limit=1000")
		if rec.Code == http.StatusTooManyRequests {
			refused = rec
			break
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("unexpected status %d: %s", rec.Code, rec.Body.String())
		}
	}
	if refused == nil {
		t.Fatal("a client querying in a loop was never refused")
	}
	if got := refused.Header().Get("Retry-After"); got == "" {
		t.Error("a refusal with no Retry-After tells the client nothing about when to come back")
	}
	if body := refused.Body.String(); !strings.Contains(body, "query budget") {
		t.Errorf("refusal body = %q, want it to name the budget", body)
	}
}

// And the budget refills, or a client that once went over is silenced
// forever.
func TestTheQueryBudgetRefillsNextEpoch(t *testing.T) {
	c := &client{}
	now := time.Now().Truncate(queryEpoch)

	c.chargeQuery(now, maxQueryCostPerEpoch*2)
	if c.allowQuery(now) {
		t.Fatal("a client well over its budget was still allowed")
	}
	if !c.allowQuery(now.Add(queryEpoch)) {
		t.Error("the budget did not refill in the next epoch")
	}
}

// One client's spending must not touch another's, which is the entire
// point of a per-client budget rather than a global one.
func TestOneClientsQueriesDoNotSpendAnothers(t *testing.T) {
	s := storeWithMetrics(2000)
	greedy, quiet := &client{}, &client{}

	for i := 0; i < 200; i++ {
		if statsAs(t, s, greedy, "?limit=1000").Code == http.StatusTooManyRequests {
			break
		}
	}
	if greedy.allowQuery(time.Now()) {
		t.Fatal("setup: the greedy client never exhausted its budget")
	}

	if rec := statsAs(t, s, quiet, "?limit=1000"); rec.Code != http.StatusOK {
		t.Errorf("a quiet client got %d while another was over budget", rec.Code)
	}
}

// Internal callers have no client and must stay unmetered - the alerter and
// the self-reporter read the store constantly and answer to §28 alone.
func TestANilClientIsUnmetered(t *testing.T) {
	var c *client

	c.chargeQuery(time.Now(), maxQueryCostPerEpoch*100)
	if !c.allowQuery(time.Now()) {
		t.Error("a nil client was refused; internal readers must not be budgeted")
	}
}

// Overshoot is bounded to one query, because the charge lands after the
// work. Worth pinning: it is the deliberate cost of pricing accurately
// instead of guessing beforehand.
func TestOvershootIsBoundedToOneQuery(t *testing.T) {
	c := &client{}
	now := time.Now().Truncate(queryEpoch)

	// One unit short of the ceiling: the next query is allowed however
	// expensive it turns out to be.
	c.chargeQuery(now, maxQueryCostPerEpoch-1)
	if !c.allowQuery(now) {
		t.Fatal("a client just under its ceiling was refused")
	}
	c.chargeQuery(now, maxStatsLimit)

	// But only that one.
	if c.allowQuery(now) {
		t.Error("a client past its ceiling was allowed a second query")
	}
}
