package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// --- helpers ---

// postBatch runs handleIngestBatch over a raw body and decodes the reply.
// The response body is meaningful on every status this endpoint returns, so
// it is decoded unconditionally rather than only on 200.
func postBatch(t *testing.T, s *Store, body string) (*httptest.ResponseRecorder, BatchResponse) {
	t.Helper()
	rec := httptest.NewRecorder()
	s.handleIngestBatch(rec, httptest.NewRequest(http.MethodPost, "/ingest/batch", strings.NewReader(body)))

	var resp BatchResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("batch reply is not JSON: %v (body %q)", err, rec.Body.String())
	}
	return rec, resp
}

// ndjson renders events as the wire format: one JSON object per line.
func ndjson(evs ...Event) string {
	var b strings.Builder
	for _, ev := range evs {
		line, err := json.Marshal(ev)
		if err != nil {
			panic(err) // only unmarshalable types, which Event is not
		}
		b.Write(line)
		b.WriteByte('\n')
	}
	return b.String()
}

// validEvents builds n events for one metric, each worth 1, stamped now.
func validEvents(name string, n int) []Event {
	now := time.Now().UnixMilli()
	evs := make([]Event, n)
	for i := range evs {
		evs[i] = Event{Name: name, Value: 1, TS: now}
	}
	return evs
}

// --- the happy path ---

func TestBatchAcceptsEveryValidEvent(t *testing.T) {
	s := newStore()
	now := time.Now().UnixMilli()

	rec, resp := postBatch(t, s, ndjson(
		Event{Name: "cpu.load", Value: 0.5, TS: now},
		Event{Name: "cpu.load", Value: 1.5, TS: now},
		Event{Name: "http.latency_ms", Value: 20, TS: now},
	))

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}
	if resp.Accepted != 3 || resp.Rejected != 0 {
		t.Errorf("accepted/rejected = %d/%d, want 3/0", resp.Accepted, resp.Rejected)
	}
	if len(resp.Errors) != 0 {
		t.Errorf("errors = %+v, want none", resp.Errors)
	}
	if resp.Fatal != "" {
		t.Errorf("fatal = %q, want empty on a clean batch", resp.Fatal)
	}

	// The reply is only worth anything if the events are actually in there.
	cpu, ok := mergeAll(s, "cpu.load")
	if !ok {
		t.Fatal("cpu.load was not recorded")
	}
	if cpu.Count != 2 || cpu.Sum != 2.0 || cpu.Min != 0.5 || cpu.Max != 1.5 {
		t.Errorf("cpu.load = %+v, want Count=2 Sum=2 Min=0.5 Max=1.5", cpu)
	}
	if lat, ok := mergeAll(s, "http.latency_ms"); !ok || lat.Count != 1 {
		t.Errorf("http.latency_ms = %+v (ok=%v), want Count=1", lat, ok)
	}
	if got := metricCount(s); got != 2 {
		t.Errorf("store holds %d metrics, want 2", got)
	}
}

// json.Decoder reads a stream of values, so the newlines in "newline-
// delimited JSON" are a convention for producers rather than something the
// parser depends on. Documented here so the liberal behaviour is deliberate
// and not an accident somebody later "fixes".
func TestBatchAcceptsAnyWhitespaceBetweenEvents(t *testing.T) {
	now := time.Now().UnixMilli()
	one := fmt.Sprintf(`{"name":"cpu.load","value":1,"ts":%d}`, now)

	bodies := map[string]string{
		"newline delimited":   one + "\n" + one + "\n",
		"no trailing newline": one + "\n" + one,
		"all on one line":     one + " " + one,
		"pretty printed":      fmt.Sprintf("{\n  \"name\": \"cpu.load\",\n  \"value\": 1,\n  \"ts\": %d\n}\n%s", now, one),
		"leading blank lines": "\n\n" + one + "\n" + one,
	}

	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			s := newStore()
			rec, resp := postBatch(t, s, body)
			if rec.Code != http.StatusOK || resp.Accepted != 2 {
				t.Errorf("status=%d accepted=%d fatal=%q, want 200 and 2",
					rec.Code, resp.Accepted, resp.Fatal)
			}
		})
	}
}

// Extra fields are not an error (§7): a producer adding a field must not
// start failing against an older server.
func TestBatchIgnoresUnknownFields(t *testing.T) {
	s := newStore()
	body := fmt.Sprintf(`{"name":"cpu.load","value":1,"ts":%d,"host":"web-01","tags":{"env":"prod"}}`+"\n",
		time.Now().UnixMilli())

	rec, resp := postBatch(t, s, body)
	if rec.Code != http.StatusOK || resp.Accepted != 1 {
		t.Errorf("status=%d accepted=%d, want 200 and 1 (fatal %q)", rec.Code, resp.Accepted, resp.Fatal)
	}
}

// --- partial failure, the point of the endpoint ---

// One bad event costs exactly itself. The reported indices are positions in
// the submitted batch, so a client can map them back to what it sent.
func TestBatchRejectsPerEventAndReportsIndices(t *testing.T) {
	s := newStore()
	now := time.Now()
	ms := now.UnixMilli()

	good := func() Event { return Event{Name: "cpu.load", Value: 2, TS: ms} }

	// Indices 1, 3 and 4 are bad, each in a different way.
	rec, resp := postBatch(t, s, ndjson(
		good(),
		Event{Value: 1, TS: ms},           // 1: no name
		good(),                            // 2
		Event{Name: "cpu.load", Value: 1}, // 3: no ts
		Event{Name: "cpu.load", TS: now.Add(-window - time.Hour).UnixMilli()}, // 4: too old
		good(), // 5
	))

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 - a partly good batch is not a failed request", rec.Code)
	}
	if resp.Accepted != 3 || resp.Rejected != 3 {
		t.Errorf("accepted/rejected = %d/%d, want 3/3", resp.Accepted, resp.Rejected)
	}
	if resp.Fatal != "" {
		t.Errorf("fatal = %q, want empty - rejected events are not a fatal condition", resp.Fatal)
	}

	wantIdx := []int{1, 3, 4}
	if len(resp.Errors) != len(wantIdx) {
		t.Fatalf("errors = %+v, want %d of them", resp.Errors, len(wantIdx))
	}
	for i, want := range wantIdx {
		if resp.Errors[i].Index != want {
			t.Errorf("errors[%d].index = %d, want %d", i, resp.Errors[i].Index, want)
		}
		if resp.Errors[i].Error == "" {
			t.Errorf("errors[%d] has no message", i)
		}
	}

	// Exactly the good events landed - no more, no fewer.
	got, ok := mergeAll(s, "cpu.load")
	if !ok {
		t.Fatal("nothing recorded")
	}
	if got.Count != 3 || got.Sum != 6 {
		t.Errorf("cpu.load = %+v, want Count=3 Sum=6 (only the valid events)", got)
	}
}

// A batch where nothing survived is a client error, not a quiet success -
// otherwise a producer sending entirely malformed events looks healthy.
func TestBatchWithNothingValidIs400(t *testing.T) {
	s := newStore()
	rec, resp := postBatch(t, s, ndjson(
		Event{Value: 1, TS: time.Now().UnixMilli()},
		Event{Name: "cpu.load", Value: 1},
	))

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
	if resp.Accepted != 0 || resp.Rejected != 2 {
		t.Errorf("accepted/rejected = %d/%d, want 0/2", resp.Accepted, resp.Rejected)
	}
	if len(resp.Errors) != 2 {
		t.Errorf("errors = %+v, want 2 - the reason must survive the 400", resp.Errors)
	}
	if metricCount(s) != 0 {
		t.Error("store is not empty after a batch in which nothing was valid")
	}
}

func TestBatchEmptyBodyIs400(t *testing.T) {
	for _, body := range []string{"", "\n", "   \n\n  "} {
		s := newStore()
		rec, resp := postBatch(t, s, body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("body %q: status = %d, want 400", body, rec.Code)
		}
		if resp.Accepted != 0 || resp.Rejected != 0 {
			t.Errorf("body %q: accepted/rejected = %d/%d, want 0/0", body, resp.Accepted, resp.Rejected)
		}
		if !strings.Contains(resp.Fatal, "empty batch") {
			t.Errorf("body %q: fatal = %q, want it to say the batch was empty", body, resp.Fatal)
		}
	}
}

// The error list is capped so a client cannot turn a 1 MiB request into a
// much larger reply. The *count* stays exact - that is what a client needs
// to know how much to resend.
func TestBatchCapsTheErrorListButNotTheCount(t *testing.T) {
	const bad = maxBatchErrors + 25

	s := newStore()
	evs := make([]Event, 0, bad+1)
	evs = append(evs, Event{Name: "cpu.load", Value: 1, TS: time.Now().UnixMilli()})
	for i := 0; i < bad; i++ {
		evs = append(evs, Event{Value: 1, TS: time.Now().UnixMilli()}) // no name
	}

	_, resp := postBatch(t, s, ndjson(evs...))

	if resp.Rejected != bad {
		t.Errorf("rejected = %d, want the exact count %d", resp.Rejected, bad)
	}
	if len(resp.Errors) != maxBatchErrors {
		t.Errorf("listed %d errors, want them capped at %d", len(resp.Errors), maxBatchErrors)
	}
	if resp.Accepted != 1 {
		t.Errorf("accepted = %d, want 1", resp.Accepted)
	}
}

// --- the stream stopping early ---

// A syntax error mid-stream cannot be resynced past, so the batch stops.
// What matters is that the events already applied are still reported, so
// the client knows exactly where to resume.
func TestBatchStopsAtMalformedJSONAndReportsWhatLanded(t *testing.T) {
	s := newStore()
	ms := time.Now().UnixMilli()
	good := fmt.Sprintf(`{"name":"cpu.load","value":1,"ts":%d}`, ms)

	body := good + "\n" + good + "\n" + `{"name":"cpu.load","value":` + "\n" + good + "\n"
	rec, resp := postBatch(t, s, body)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
	if resp.Accepted != 2 {
		t.Errorf("accepted = %d, want 2 - the events before the break must still be reported", resp.Accepted)
	}
	if !strings.Contains(resp.Fatal, "malformed JSON at event 2") {
		t.Errorf("fatal = %q, want it to name the event that broke the stream", resp.Fatal)
	}

	// Reported and recorded must agree even when the request failed.
	got, ok := mergeAll(s, "cpu.load")
	if !ok || got.Count != resp.Accepted {
		t.Errorf("recorded %+v (ok=%v), but the reply claimed %d accepted", got, ok, resp.Accepted)
	}
}

// A body cut off mid-object is the same class of failure, and is what a
// dropped connection actually looks like on the wire.
func TestBatchStopsAtTruncatedBody(t *testing.T) {
	s := newStore()
	ms := time.Now().UnixMilli()
	good := fmt.Sprintf(`{"name":"cpu.load","value":1,"ts":%d}`, ms)

	rec, resp := postBatch(t, s, good+"\n"+`{"name":"cpu.lo`)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
	if resp.Accepted != 1 {
		t.Errorf("accepted = %d, want 1", resp.Accepted)
	}
	if !strings.Contains(resp.Fatal, "malformed JSON at event 1") {
		t.Errorf("fatal = %q, want it to name event 1", resp.Fatal)
	}
}

// Past maxBatchEvents the request stops, because one batch holding the
// store's locks for an unbounded stretch is a stall for every other writer
// and for /stats.
func TestBatchStopsAtTheEventCap(t *testing.T) {
	s := newStore()
	rec, resp := postBatch(t, s, ndjson(validEvents("cpu.load", maxBatchEvents+50)...))

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413", rec.Code)
	}
	if resp.Accepted != maxBatchEvents {
		t.Errorf("accepted = %d, want exactly the cap %d", resp.Accepted, maxBatchEvents)
	}
	if !strings.Contains(resp.Fatal, "batch too large") {
		t.Errorf("fatal = %q, want it to say the batch was too large", resp.Fatal)
	}

	got, ok := mergeAll(s, "cpu.load")
	if !ok || got.Count != maxBatchEvents {
		t.Errorf("recorded %+v (ok=%v), want exactly %d", got, ok, maxBatchEvents)
	}
}

// A batch of exactly the cap is fine - the limit is a maximum, not a
// boundary that is off by one.
func TestBatchAcceptsExactlyTheEventCap(t *testing.T) {
	s := newStore()
	rec, resp := postBatch(t, s, ndjson(validEvents("cpu.load", maxBatchEvents)...))

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 (fatal %q)", rec.Code, resp.Fatal)
	}
	if resp.Accepted != maxBatchEvents {
		t.Errorf("accepted = %d, want %d", resp.Accepted, maxBatchEvents)
	}
}

// The byte cap bounds what one request can make the server read, whatever
// the event count says.
func TestBatchStopsAtTheByteCap(t *testing.T) {
	s := newStore()

	// Names as long as the contract allows, so the byte cap is reached in
	// far fewer than maxBatchEvents events and it is unambiguously the byte
	// cap being tested. Sized against maxMetricNameLen rather than picked,
	// so tightening that limit adjusts this test instead of breaking it.
	name := "cpu.load." + strings.Repeat("x", maxMetricNameLen-len("cpu.load."))
	ms := time.Now().UnixMilli()
	var b strings.Builder
	for b.Len() <= maxBatchBody+8<<10 {
		fmt.Fprintf(&b, "{\"name\":%q,\"value\":1,\"ts\":%d}\n", name, ms)
	}

	rec, resp := postBatch(t, s, b.String())
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413 (fatal %q)", rec.Code, resp.Fatal)
	}
	if !strings.Contains(resp.Fatal, "body exceeded") {
		t.Errorf("fatal = %q, want it to say the body was too big", resp.Fatal)
	}
	if resp.Accepted == 0 {
		t.Error("accepted = 0; the events read before the cap should still have been applied")
	}

	got, ok := mergeAll(s, name)
	if !ok || got.Count != resp.Accepted {
		t.Errorf("recorded %+v (ok=%v), but the reply claimed %d accepted", got, ok, resp.Accepted)
	}
}

// --- routing ---

func TestBatchRouteIsWiredAndPostOnly(t *testing.T) {
	s := newStore()
	a, err := newAlerter(time.Now(), s, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := routes(s, a, newLimiter(maxInFlight))

	body := ndjson(Event{Name: "cpu.load", Value: 1, TS: time.Now().UnixMilli()})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/ingest/batch", strings.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Errorf("POST /ingest/batch: status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ingest/batch", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET /ingest/batch: status = %d, want 405", rec.Code)
	}

	// The single-event route must be untouched by the new pattern.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(ingestJSON("cpu.load", 1))))
	if rec.Code != http.StatusOK {
		t.Errorf("POST /ingest still works: status = %d, want 200", rec.Code)
	}
}
