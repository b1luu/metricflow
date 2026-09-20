package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The regression this guards against is a one-line revert: someone writes
// &http.Server{Handler: h} again and every timeout silently becomes
// infinite. Nothing fails, no test breaks, and the server is defenceless -
// which is exactly why it needs asserting rather than trusting.
func TestNewServerSetsEveryTimeout(t *testing.T) {
	srv := newServer(http.NewServeMux())

	fields := []struct {
		name string
		got  time.Duration
	}{
		{"ReadHeaderTimeout", srv.ReadHeaderTimeout},
		{"ReadTimeout", srv.ReadTimeout},
		{"WriteTimeout", srv.WriteTimeout},
		{"IdleTimeout", srv.IdleTimeout},
	}
	for _, f := range fields {
		if f.got <= 0 {
			t.Errorf("%s = %v; the zero value means no limit at all", f.name, f.got)
		}
	}

	if srv.MaxHeaderBytes <= 0 {
		t.Errorf("MaxHeaderBytes = %d, want a positive cap", srv.MaxHeaderBytes)
	}
	if srv.Handler == nil {
		t.Error("Handler is nil")
	}

	// Headers must be tighter than the whole request: they are small and
	// arrive at once, while a full batch body legitimately needs room.
	if srv.ReadHeaderTimeout >= srv.ReadTimeout {
		t.Errorf("ReadHeaderTimeout (%v) >= ReadTimeout (%v); headers should be the tighter bound",
			srv.ReadHeaderTimeout, srv.ReadTimeout)
	}
	// IdleTimeout is what makes ReadTimeout safe: without it Go applies
	// ReadTimeout to the idle wait between keep-alive requests, and a
	// well-behaved client reusing a connection gets disconnected for having
	// nothing to say yet.
	if srv.IdleTimeout < srv.ReadTimeout {
		t.Errorf("IdleTimeout (%v) < ReadTimeout (%v); keep-alive connections would be reaped too eagerly",
			srv.IdleTimeout, srv.ReadTimeout)
	}
}

// The values above are policy; this is the mechanism. A client that opens a
// connection and never finishes its headers - the slow-loris shape - must be
// disconnected by the server rather than held forever.
//
// The timeout is overridden to something short because the test is about
// whether net/http enforces the field at all, not about whether 5 seconds is
// the right number. Waiting out the real value would make this a five-second
// test that asserts the same thing.
func TestServerDisconnectsAClientThatNeverFinishesItsHeaders(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	srv := newServer(http.NewServeMux())
	srv.ReadHeaderTimeout = 100 * time.Millisecond
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// A request line and one header, then silence - never the blank line
	// that ends the header block. A server with no ReadHeaderTimeout waits
	// here indefinitely, holding a goroutine and a socket.
	if _, err := fmt.Fprint(conn, "GET /health HTTP/1.1\r\nHost: x\r\n"); err != nil {
		t.Fatal(err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(conn)

	// The server may close cleanly (EOF) or reset the connection; either is
	// the server hanging up. What must not happen is our own deadline
	// firing, which would mean it was still waiting.
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("the server was still holding the connection after the read-header timeout")
	}
}

// --- load shedding ---

// The three claims in one test, because they only mean anything together:
// requests up to capacity are admitted, the next is refused, and it is
// refused *immediately* rather than waiting for a slot. The last one is the
// actual design decision - a limiter that queued would pass the first two.
func TestLimiterAdmitsToCapacityThenShedsWithoutWaiting(t *testing.T) {
	const capacity = 4

	entered := make(chan struct{}, capacity)
	release := make(chan struct{})

	l := newLimiter(capacity)
	h := l.limit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-release
		w.WriteHeader(http.StatusOK)
	}))

	get := func() *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stats", nil))
		return rec
	}

	var wg sync.WaitGroup
	for i := 0; i < capacity; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if rec := get(); rec.Code != http.StatusOK {
				t.Errorf("a request within capacity got %d, want 200", rec.Code)
			}
		}()
	}
	for i := 0; i < capacity; i++ {
		<-entered // every slot is genuinely occupied before we push further
	}

	// The handlers above are still blocked, so a limiter that queued would
	// never return from this. Bounded rather than left to hang, so the
	// failure reads as a failure instead of a ten-minute test timeout.
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- get() }()

	select {
	case rec := <-done:
		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("request past capacity got %d, want 503", rec.Code)
		}
		if rec.Header().Get("Retry-After") == "" {
			t.Error("no Retry-After on a shed request; the client has nothing to back off on")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the request past capacity blocked; the limiter is queueing, not shedding")
	}

	if got := l.shedded(); got != 1 {
		t.Errorf("shedded = %d, want 1", got)
	}

	// And the capacity comes back once the handlers finish.
	close(release)
	wg.Wait()

	release = make(chan struct{})
	close(release)
	if rec := get(); rec.Code != http.StatusOK {
		t.Errorf("after the in-flight requests finished, got %d, want 200", rec.Code)
	}
}

// A leaked slot is permanent: the server would shed a little more after
// every panic until it shed everything. That is a slow, silent failure of
// exactly the kind a defer exists to prevent, so it gets a test.
func TestLimiterReleasesItsSlotWhenAHandlerPanics(t *testing.T) {
	l := newLimiter(1)
	h := l.limit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	}))

	call := func() (rec *httptest.ResponseRecorder, panicked bool) {
		rec = httptest.NewRecorder()
		defer func() { panicked = recover() != nil }()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stats", nil))
		return rec, false
	}

	if _, panicked := call(); !panicked {
		t.Fatal("the handler was supposed to panic; this test proves nothing otherwise")
	}

	// Admitted means it reaches the handler and panics again. Shed means
	// the only slot never came back.
	rec, panicked := call()
	if !panicked {
		t.Errorf("second request was not admitted (status %d) - the panic leaked the slot", rec.Code)
	}
}

// /health must never be shed. A health check that fails under load makes an
// overloaded server look like a dead one, so whatever is watching kills or
// depools it - turning a server that was still serving most of its traffic
// into one serving none, and moving its load onto equally-loaded neighbours.
//
// A limiter of capacity zero sheds everything, which makes the exemption the
// only thing that can produce a 200 here.
func TestHealthIsNeverShed(t *testing.T) {
	s := newStore()
	a, err := newAlerter(time.Now(), s, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := routes(s, a, newLimiter(0))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("/health under a fully shedding limiter: %d, want 200", rec.Code)
	}

	// Everything else must be shed, or the test above proves nothing about
	// the exemption - it would just mean the limiter is not wired in.
	shed := []struct {
		method, path string
	}{
		{http.MethodPost, "/ingest"},
		{http.MethodPost, "/ingest/batch"},
		{http.MethodGet, "/stats"},
		{http.MethodGet, "/alerts"},
	}
	for _, c := range shed {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(c.method, c.path, strings.NewReader("")))
		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("%s %s: %d, want 503 - the limiter is not covering this route",
				c.method, c.path, rec.Code)
		}
	}
}

// Shedding has to be cheap, or it is not a defence. If refusing a request
// cost anything like serving one, an overloaded server would still be
// overloaded, just with worse output.
func BenchmarkShedRequest(b *testing.B) {
	l := newLimiter(0) // sheds everything
	h := l.limit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.Fatal("a shedding limiter reached the handler")
	}))

	w := &discardWriter{}
	req := httptest.NewRequest(http.MethodGet, "/stats", nil)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.ServeHTTP(w, req)
	}
}

// The limit is the one operational knob this server exposes, so its parsing
// is held to the same standard as an alert rule: a value the operator got
// wrong stops the process rather than being quietly replaced by the default.
func TestInFlightLimitFromTheEnvironment(t *testing.T) {
	cases := []struct {
		name    string
		set     bool
		value   string
		want    int
		wantErr bool
	}{
		{name: "unset falls back to the constant", want: maxInFlight},
		{name: "a plain number", set: true, value: "4", want: 4},
		{name: "one is allowed", set: true, value: "1", want: 1},
		{name: "zero would shed everything", set: true, value: "0", wantErr: true},
		{name: "negative", set: true, value: "-1", wantErr: true},
		{name: "not a number", set: true, value: "lots", wantErr: true},
		{name: "empty but set", set: true, value: "", wantErr: true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.set {
				t.Setenv("METRICFLOW_MAX_INFLIGHT", c.value)
			}

			got, err := inFlightLimit()
			if c.wantErr {
				if err == nil {
					t.Fatalf("value %q was accepted as %d, want an error", c.value, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("value %q: %v", c.value, err)
			}
			if got != c.want {
				t.Errorf("value %q gave %d, want %d", c.value, got, c.want)
			}
		})
	}
}

// --- supervised background loops ---

// The failure this prevents: an unrecovered panic in a bare goroutine takes
// the process down, and §2 keeps every aggregate in memory, so that is total
// data loss. A handler panic costs one connection; a background panic used
// to cost the last minute of every metric in the store.
func TestSuperviseRestartsAfterAPanic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var starts atomic.Int64
	done := make(chan struct{})

	out := captureLog(t, func() {
		go func() {
			defer close(done)
			supervise(ctx, "flaky", time.Millisecond, func(context.Context) {
				// Panic the first two times, then block until cancelled.
				if starts.Add(1) <= 2 {
					panic("boom")
				}
				<-ctx.Done()
			})
		}()

		deadline := time.After(10 * time.Second)
		for starts.Load() < 3 {
			select {
			case <-deadline:
				t.Error("the loop was not restarted after panicking")
				return
			default:
				time.Sleep(5 * time.Millisecond)
			}
		}
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("supervise did not return after cancellation")
		}
	})

	if got := starts.Load(); got < 3 {
		t.Errorf("the loop ran %d times, want at least 3 (two panics then a clean run)", got)
	}
	// A recovered panic that is not loud is worse than a crash: it hides a
	// bug behind a server that looks healthy.
	if !strings.Contains(out, "PANIC in flaky") {
		t.Errorf("the panic was not logged with its loop's name:\n%s", out)
	}
	if !strings.Contains(out, "boom") {
		t.Errorf("the panic value was not logged:\n%s", out)
	}
	// The stack is the only thing that makes a recovered panic diagnosable.
	if !strings.Contains(out, "supervise") && !strings.Contains(out, "goroutine") {
		t.Errorf("no stack trace in the panic log:\n%s", out)
	}
}

// A loop that returns on its own has finished, and supervise must not spin
// restarting it.
func TestSuperviseStopsWhenTheLoopReturns(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var runs atomic.Int64
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		supervise(ctx, "tidy", time.Millisecond, func(ctx context.Context) {
			runs.Add(1)
			close(started)
			<-ctx.Done() // the shape every real loop has
		})
	}()

	// Cancelling before the goroutine is scheduled would leave the loop
	// never entered, and the test would pass or fail on timing alone.
	<-started
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("supervise did not return when its loop did")
	}
	if got := runs.Load(); got != 1 {
		t.Errorf("the loop ran %d times, want 1 - a clean return must not be restarted", got)
	}
}

// testBackoff is long enough that a supervise which slept through it
// instead of selecting on ctx would be obvious, and short enough not to
// stall the suite if that ever regresses.
const testBackoff = 2 * time.Second

// Cancellation during the backoff has to be honoured, or shutdown stalls for
// up to panicBackoff after any panic - and run() waits on these loops before
// the process exits.
func TestSuperviseReturnsPromptlyWhenCancelledDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	panicked := make(chan struct{})
	done := make(chan struct{})

	_ = captureLog(t, func() {
		go func() {
			defer close(done)
			supervise(ctx, "always-panics", testBackoff, func(context.Context) {
				select {
				case panicked <- struct{}{}:
				default:
				}
				panic("boom")
			})
		}()

		<-panicked // it is now inside the backoff
		start := time.Now()
		cancel()

		select {
		case <-done:
			if waited := time.Since(start); waited >= testBackoff {
				t.Errorf("supervise took %s to notice cancellation; it waited out the "+
					"%s backoff instead of selecting on ctx", waited, testBackoff)
			}
		case <-time.After(10 * time.Second):
			t.Error("supervise never returned after cancellation during backoff")
		}
	})
}

// The real loops, supervised for real: a panicking sweeper must not take the
// process with it, and the store must still be usable afterwards.
func TestAPanickingSweeperDoesNotKillTheProcess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var ticks atomic.Int64
	done := make(chan struct{})

	_ = captureLog(t, func() {
		go func() {
			defer close(done)
			supervise(ctx, "sweeper", time.Millisecond, func(ctx context.Context) {
				ticks.Add(1)
				panic("sweep exploded")
			})
		}()

		deadline := time.After(10 * time.Second)
		for ticks.Load() < 2 {
			select {
			case <-deadline:
				t.Error("the sweeper was not restarted")
				return
			default:
				time.Sleep(5 * time.Millisecond)
			}
		}
		cancel()
		<-done
	})

	// If the panic had escaped, this line would never run.
	s := newStore()
	recordNow(s, "still.here", 1)
	if got, ok := mergeAll(s, "still.here"); !ok || got.Count != 1 {
		t.Errorf("store unusable after a supervised panic: %+v (ok=%v)", got, ok)
	}
}
