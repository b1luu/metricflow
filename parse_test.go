package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"
)

// --- differential testing against encoding/json ---

// agree checks the hand-written parser against the standard library on one
// input, and is the whole safety argument for having written a parser at all.
//
// The two have different shapes, so "agree" needs defining. json.Unmarshal
// requires the entire input to be one value; parseEvent consumes one value
// and says how many bytes it took. They agree when parseEvent succeeds and
// leaves nothing but whitespace behind exactly where Unmarshal succeeds, and
// produce the same Event when they do.
//
// Errors are compared as accept-or-reject, not by message: the messages are
// this server's to choose, the verdict is not.
func agree(t *testing.T, in string) {
	t.Helper()

	var want Event
	wantErr := json.Unmarshal([]byte(in), &want) != nil

	var got Event
	n, err := parseEvent([]byte(in), &got)
	gotErr := err != nil || strings.TrimLeft(in[n:], " \t\n\r") != ""

	if gotErr != wantErr {
		verdict := func(bad bool) string {
			if bad {
				return "rejected"
			}
			return "accepted"
		}
		t.Errorf("%q: encoding/json %s it, parseEvent %s it (err=%v, consumed %d of %d)",
			in, verdict(wantErr), verdict(gotErr), err, n, len(in))
		return
	}
	if wantErr {
		return // both refused; nothing further to compare
	}

	if got.Name != want.Name {
		t.Errorf("%q: name = %q, encoding/json got %q", in, got.Name, want.Name)
	}
	// Bit comparison, so -0 and 0 are not quietly treated as the same answer.
	if math.Float64bits(got.Value) != math.Float64bits(want.Value) {
		t.Errorf("%q: value = %v, encoding/json got %v", in, got.Value, want.Value)
	}
	if got.TS != want.TS {
		t.Errorf("%q: ts = %d, encoding/json got %d", in, got.TS, want.TS)
	}
}

// The table is where the standard library's quirks are written down. Each
// group is a rule the parser had to be taught rather than one it would have
// arrived at.
func TestParseEventAgreesWithEncodingJSON(t *testing.T) {
	groups := map[string][]string{
		"the ordinary shape": {
			`{"name":"cpu.load","value":1,"ts":1758000000000}`,
			`{"name":"http.latency_ms","value":12.5,"ts":1758000000000}`,
			`{}`,
			`{"name":"only"}`,
		},
		"whitespace is allowed between every token": {
			"  {  \"name\" : \"a\" , \"value\" : 1 , \"ts\" : 2 }  ",
			"{\n\t\"name\": \"a\",\r\n\t\"value\": 1\n}",
		},
		"unknown fields are skipped, whatever they hold": {
			`{"name":"a","host":"web-01"}`,
			`{"tags":{"env":"prod","region":"eu"},"name":"a"}`,
			`{"list":[1,2,{"deep":[true,null,"x"]}],"name":"a"}`,
			`{"flag":true,"other":false,"nothing":null,"name":"a"}`,
			`{"n":-1.5e-3,"name":"a"}`,
			// A brace inside a skipped string must not end the object.
			`{"note":"} not the end {","name":"a"}`,
			`{"note":"escaped \" quote }","name":"a"}`,
		},
		"duplicate keys: the last one wins": {
			`{"name":"first","name":"second"}`,
			`{"ts":1,"ts":2,"ts":3}`,
			`{"value":1,"value":null}`,
		},
		"field names match case-insensitively, as encoding/json does": {
			`{"NAME":"a"}`,
			`{"Name":"a"}`,
			`{"nAmE":"a"}`,
			`{"TS":5}`,
			`{"Value":1.5}`,
			// But a different name is still a different field.
			`{"names":"a"}`,
			`{"nam":"a"}`,
		},
		"null leaves a field at its zero value rather than erroring": {
			`{"name":null}`,
			`{"value":null}`,
			`{"ts":null}`,
			`{"name":null,"ts":7}`,
		},
		"type mismatches are errors": {
			`{"name":1}`,
			`{"name":true}`,
			`{"name":[]}`,
			`{"name":{}}`,
			`{"value":"1"}`,
			`{"value":true}`,
			`{"ts":"1"}`,
			`{"ts":[]}`,
		},
		"numbers follow JSON's grammar exactly": {
			`{"value":0}`,
			`{"value":-0}`,
			`{"value":1e3}`,
			`{"value":1E3}`,
			`{"value":1e+3}`,
			`{"value":1e-3}`,
			`{"value":-1.5}`,
			`{"value":0.0001}`,
			`{"value":01}`,
			`{"value":1.}`,
			`{"value":.5}`,
			`{"value":+1}`,
			`{"value":-}`,
			`{"value":1e}`,
			`{"value":1e+}`,
			`{"value":--1}`,
		},
		"an integer field rejects anything but an integer literal": {
			`{"ts":1e3}`,
			`{"ts":1.0}`,
			`{"ts":1.5}`,
			`{"ts":-1}`,
			`{"ts":0}`,
			`{"ts":-0}`,
			`{"ts":9223372036854775807}`,
			`{"ts":-9223372036854775808}`,
			`{"ts":9223372036854775808}`,
			`{"ts":-9223372036854775809}`,
			`{"ts":99999999999999999999999}`,
		},
		"string escapes": {
			`{"name":"a\"b"}`,
			`{"name":"a\\b"}`,
			`{"name":"a\/b"}`,
			`{"name":"a\bb"}`,
			`{"name":"a\fb"}`,
			`{"name":"a\nb"}`,
			`{"name":"a\rb"}`,
			`{"name":"a\tb"}`,
			`{"name":"\u0041"}`,
			`{"name":"\u00e9"}`,
			`{"name":"\u4e2d\u6587"}`,
			`{"name":"\ud83d\ude00"}`, // a surrogate pair
			`{"name":"\ud83d"}`,       // a lone high surrogate
			`{"name":"\ude00"}`,       // a lone low surrogate
			`{"name":"\ud83dx"}`,      // high surrogate, then something else
			`{"name":"\ud83d\u0041"}`, // high surrogate, then a non-surrogate
			`{"name":"\x41"}`,         // not a JSON escape
			`{"name":"\u00"}`,         // truncated
			`{"name":"\u00zz"}`,       // not hex
		},
		"non-ASCII passes through": {
			`{"name":"café"}`,
			`{"name":"日本語"}`,
			`{"name":"emoji 😀"}`,
		},
		"raw control characters are not allowed in strings": {
			"{\"name\":\"a\nb\"}",
			"{\"name\":\"a\tb\"}",
			"{\"name\":\"a\x00b\"}",
			"{\"name\":\"a\x1fb\"}",
		},
		"structural errors": {
			``,
			`   `,
			`{`,
			`}`,
			`{"name"}`,
			`{"name":}`,
			`{:"a"}`,
			`{"name":"a",}`,
			`{,"name":"a"}`,
			`{"name":"a"`,
			`{"name":"a}`,
			`{name:"a"}`,
			`{'name':'a'}`,
			`[]`,
			`"a string"`,
			`123`,
			`true`,
			`null`,
			`{"a":{"b":}}`,
			`{"a":[1,]}`,
			`{"a":[}`,
		},
		"trailing content is the caller's business, not the parser's": {
			`{"name":"a"} `,
			`{"name":"a"}` + "\n",
			`{"name":"a"}{"name":"b"}`,
			`{"name":"a"} garbage`,
		},
	}

	for group, inputs := range groups {
		t.Run(group, func(t *testing.T) {
			for _, in := range inputs {
				agree(t, in)
			}
		})
	}
}

// Fuzzing is the part that finds the cases a table does not think of. The
// seeds are deliberately nasty rather than realistic - the fuzzer will find
// realistic inputs on its own, but it needs help reaching surrogate pairs and
// number-grammar edges.
func FuzzParseEventAgreesWithEncodingJSON(f *testing.F) {
	seeds := []string{
		`{"name":"cpu.load","value":1,"ts":1758000000000}`,
		`{"name":"a\u00e9b","value":-1.5e-3,"ts":-1}`,
		`{"NAME":null,"tags":{"a":[1,{"b":"}"}]},"ts":0}`,
		`{"name":"\ud83d\ude00","value":1e308,"ts":9223372036854775807}`,
		`{"name":"\ud800","value":0.0,"ts":-9223372036854775808}`,
		`{"value":01,"ts":1.0}`,
		`{"name":"a","name":"b","name":null}`,
		"{\"name\":\"\x80\xff\"}",
		`{`,
		`{"a":`,
		``,
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, in string) {
		var want Event
		wantErr := json.Unmarshal([]byte(in), &want) != nil

		var got Event
		n, err := parseEvent([]byte(in), &got)
		gotErr := err != nil || n > len(in) || strings.TrimLeft(in[n:], " \t\n\r") != ""

		if gotErr != wantErr {
			t.Fatalf("disagreement on %q: encoding/json err=%v, parseEvent err=%v consumed=%d",
				in, wantErr, err, n)
		}
		if wantErr {
			return
		}
		if got.Name != want.Name ||
			math.Float64bits(got.Value) != math.Float64bits(want.Value) ||
			got.TS != want.TS {
			t.Fatalf("different events for %q: parseEvent %+v, encoding/json %+v", in, got, want)
		}
	})
}

// --- the consumed count ---

// The byte count is what lets a stream find the next event, so it has to be
// exact rather than merely enough.
func TestParseEventReportsWhatItConsumed(t *testing.T) {
	one := `{"name":"a","value":1,"ts":2}`

	cases := []struct {
		in   string
		want int
	}{
		{one, len(one)},
		{"  " + one, len(one) + 2},
		{one + "\n", len(one)},
		{one + one, len(one)},
		{one + "   " + one, len(one)},
		{"\n\t " + one + " trailing junk", len(one) + 3},
	}

	for _, c := range cases {
		var ev Event
		n, err := parseEvent([]byte(c.in), &ev)
		if err != nil {
			t.Errorf("%q: %v", c.in, err)
			continue
		}
		if n != c.want {
			t.Errorf("%q: consumed %d, want %d", c.in, n, c.want)
		}
	}
}

// A prefix of a valid value must read as "not yet" rather than "never", or a
// streaming caller would reject an event that merely straddled a read.
func TestParseEventDistinguishesIncompleteFromInvalid(t *testing.T) {
	full := `{"name":"cpu.load","value":-1.5e3,"ts":1758000000000}`

	// Every proper prefix is incomplete, never a syntax error.
	for i := 0; i < len(full); i++ {
		var ev Event
		_, err := parseEvent([]byte(full[:i]), &ev)
		if err == nil {
			t.Errorf("prefix %q parsed as a complete event", full[:i])
			continue
		}
		if err != errIncomplete {
			t.Errorf("prefix %q gave %v, want errIncomplete", full[:i], err)
		}
	}

	// And genuinely broken input is a syntax error, not something a caller
	// should wait for more bytes on.
	for _, bad := range []string{`{"name":]`, `{"name" "a"}`, `{1:2}`, `[1]`, `{"ts":1.5}`} {
		var ev Event
		if _, err := parseEvent([]byte(bad), &ev); err == errIncomplete {
			t.Errorf("%q reported incomplete; a stream would wait forever", bad)
		}
	}
}

// --- cost ---

func BenchmarkParseEvent(b *testing.B) {
	in := []byte(`{"name":"http.latency_ms","value":12.5,"ts":1758000000000}`)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var ev Event
		if _, err := parseEvent(in, &ev); err != nil {
			b.Fatal(err)
		}
	}
}

// The comparison this slice exists to move.
func BenchmarkUnmarshalEvent(b *testing.B) {
	in := []byte(`{"name":"http.latency_ms","value":12.5,"ts":1758000000000}`)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var ev Event
		if err := json.Unmarshal(in, &ev); err != nil {
			b.Fatal(err)
		}
	}
}

// A realistic event from a producer that adds its own fields, since skipping
// them is work the old decoder also did.
func BenchmarkParseEventWithUnknownFields(b *testing.B) {
	in := []byte(`{"name":"http.latency_ms","value":12.5,"ts":1758000000000,` +
		`"host":"web-01","tags":{"env":"prod","region":"eu-west-1"},"trace":[1,2,3]}`)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var ev Event
		if _, err := parseEvent(in, &ev); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUnmarshalEventWithUnknownFields(b *testing.B) {
	in := []byte(`{"name":"http.latency_ms","value":12.5,"ts":1758000000000,` +
		`"host":"web-01","tags":{"env":"prod","region":"eu-west-1"},"trace":[1,2,3]}`)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var ev Event
		if err := json.Unmarshal(in, &ev); err != nil {
			b.Fatal(err)
		}
	}
}

// Nesting is bounded, because without a limit a body of nothing but open
// brackets recurses once per byte and maxBatchBody allows a million of them.
// encoding/json caps at the same depth, so this is agreement as much as
// safety.
func TestParseEventBoundsNesting(t *testing.T) {
	deep := func(n int) string {
		return `{"a":` + strings.Repeat("[", n) + strings.Repeat("]", n) + `}`
	}

	var ev Event
	if _, err := parseEvent([]byte(deep(100)), &ev); err != nil {
		t.Errorf("100 levels was rejected: %v", err)
	}

	in := deep(maxDepth + 100)
	if _, err := parseEvent([]byte(in), &ev); err == nil {
		t.Errorf("%d levels of nesting was accepted", maxDepth+100)
	}
	// And the standard library agrees it is too deep, so this is not a
	// limit invented here that would reject documents it accepts.
	if err := json.Unmarshal([]byte(in), &ev); err == nil {
		t.Error("encoding/json accepted the deep input; the limits disagree")
	}
}

// The parser must not allocate for the parts it throws away. A producer that
// adds host, tags and a trace ID should cost the same as one that does not.
func TestParseEventAllocatesOnlyTheName(t *testing.T) {
	cases := map[string]string{
		"plain": `{"name":"http.latency_ms","value":12.5,"ts":1758000000000}`,
		// A realistic name, not a one-byte one: Go returns a pointer into a
		// static array when converting a single byte to a string, so "a"
		// would allocate nothing and the assertion would prove nothing.
		"unknown fields": `{"name":"svc.api.latency","value":1,"ts":2,"host":"web-01","tags":{"env":"prod"},"trace":[1,2,3]}`,
		"no name at all": `{"value":1,"ts":2,"host":"web-01"}`,
	}

	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			b := []byte(in)
			var ev Event
			got := testing.AllocsPerRun(200, func() {
				if _, err := parseEvent(b, &ev); err != nil {
					t.Fatal(err)
				}
			})

			// One for the metric name, which has to become a string because
			// the store keeps it. Zero when there is no name to keep.
			want := 1.0
			if ev.Name == "" {
				want = 0
			}
			if got != want {
				t.Errorf("%v allocations per parse, want %v", got, want)
			}
		})
	}
}

// --- streaming ---

// drain reads every event from a stream, returning them and how it ended.
func drain(r io.Reader) ([]Event, error) {
	er := newEventReader(r)
	defer er.release()

	var out []Event
	for {
		var ev Event
		if err := er.next(&ev); err != nil {
			return out, err
		}
		out = append(out, ev)
	}
}

// oneByteReader hands over a single byte per Read, so every value in the
// stream is split across a refill at every possible position. This is the
// test that matters for a buffered reader: a parser that resumed from the
// wrong offset, or a refill that lost the bytes it already had, passes every
// other test here and fails this one.
type oneByteReader struct {
	s string
	i int
}

func (r *oneByteReader) Read(p []byte) (int, error) {
	if r.i >= len(r.s) {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	p[0] = r.s[r.i]
	r.i++
	return 1, nil
}

func TestEventReaderSurvivesAByteAtATime(t *testing.T) {
	body := `{"name":"cpu.load","value":1,"ts":100}` + "\n" +
		`{"name":"aéb","value":-1.5e-3,"ts":-1,"tags":{"env":"prod"}}` + "\n" +
		`  {"name":"third","value":0,"ts":3}  `

	whole, err := drain(strings.NewReader(body))
	if err != io.EOF {
		t.Fatalf("reading it whole: %v", err)
	}

	split, err := drain(&oneByteReader{s: body})
	if err != io.EOF {
		t.Fatalf("reading it a byte at a time: %v", err)
	}

	if len(split) != len(whole) {
		t.Fatalf("got %d events a byte at a time, %d in one read", len(split), len(whole))
	}
	for i := range whole {
		if split[i] != whole[i] {
			t.Errorf("event %d differs: %+v a byte at a time, %+v in one read",
				i, split[i], whole[i])
		}
	}
	if len(whole) != 3 {
		t.Errorf("read %d events, want 3", len(whole))
	}
}

// The same property against every split point of a single value, which is
// cheaper to diagnose when it fails than the byte-at-a-time version.
func TestEventReaderHandlesEverySplitPoint(t *testing.T) {
	body := `{"name":"svc.api","value":12.5,"ts":1758000000000,"tags":{"a":"b"}}`

	for cut := 0; cut <= len(body); cut++ {
		r := io.MultiReader(
			strings.NewReader(body[:cut]),
			strings.NewReader(body[cut:]),
		)
		got, err := drain(r)
		if err != io.EOF {
			t.Errorf("split at %d: %v", cut, err)
			continue
		}
		if len(got) != 1 {
			t.Errorf("split at %d: got %d events, want 1", cut, len(got))
			continue
		}
		if got[0].Name != "svc.api" || got[0].Value != 12.5 || got[0].TS != 1758000000000 {
			t.Errorf("split at %d: got %+v", cut, got[0])
		}
	}
}

func TestEventReaderEndings(t *testing.T) {
	one := `{"name":"a","value":1,"ts":2}`

	cases := []struct {
		name    string
		body    string
		want    int
		wantErr error
	}{
		{"empty", "", 0, io.EOF},
		{"whitespace only", "  \n\t ", 0, io.EOF},
		{"one event", one, 1, io.EOF},
		{"trailing newline", one + "\n", 1, io.EOF},
		{"trailing whitespace", one + "  \n\t", 1, io.EOF},
		{"several, newline separated", one + "\n" + one + "\n" + one, 3, io.EOF},
		{"several on one line", one + " " + one, 2, io.EOF},
		{"no separator at all", one + one, 2, io.EOF},

		// A body cut off mid-value is not a clean end, and must not be
		// reported as one - that is the difference between "the client
		// finished" and "the connection died".
		{"truncated mid-object", one + "\n" + `{"name":"b`, 1, io.ErrUnexpectedEOF},
		{"truncated after a comma", one + "\n" + `{"name":"b",`, 1, io.ErrUnexpectedEOF},
		{"just an opening brace", "{", 0, io.ErrUnexpectedEOF},

		// Malformed is malformed, whatever follows it.
		{"syntax error", one + "\n" + `{"name":]}`, 1, nil},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := drain(strings.NewReader(c.body))
			if len(got) != c.want {
				t.Errorf("read %d events, want %d", len(got), c.want)
			}
			if c.wantErr != nil && err != c.wantErr {
				t.Errorf("ended with %v, want %v", err, c.wantErr)
			}
			if c.wantErr == nil && (err == nil || err == io.EOF) {
				t.Errorf("ended with %v, want a syntax error", err)
			}
		})
	}
}

// An event bigger than the buffer must grow it rather than fail, because
// unknown fields are allowed to be large and §7 says a producer adding one
// must not start failing.
func TestEventReaderGrowsForAnOversizedEvent(t *testing.T) {
	padding := strings.Repeat("x", readBufSize*3)
	body := `{"name":"big","value":1,"ts":2,"pad":"` + padding + `"}` + "\n" +
		`{"name":"after","value":2,"ts":3}`

	got, err := drain(strings.NewReader(body))
	if err != io.EOF {
		t.Fatalf("%v", err)
	}
	if len(got) != 2 {
		t.Fatalf("read %d events, want 2", len(got))
	}
	if got[0].Name != "big" || got[1].Name != "after" {
		t.Errorf("got %+v", got)
	}
}

// Growth is bounded. Past the body cap the reader gives up rather than
// buffering whatever a client feels like sending.
func TestEventReaderRefusesAnEventPastTheBodyCap(t *testing.T) {
	// The reader is handed more than maxBatchBody directly, standing in for
	// a MaxBytesReader that was not there - the point is that the buffer
	// itself stops growing.
	body := `{"name":"huge","pad":"` + strings.Repeat("x", maxBatchBody+1024) + `"}`

	got, err := drain(strings.NewReader(body))
	if err == nil || err == io.EOF {
		t.Fatalf("read %d events and ended with %v, want a refusal", len(got), err)
	}
	if !strings.Contains(err.Error(), "exceeds the body limit") {
		t.Errorf("error = %q, want it to name the limit", err)
	}
}

// A read error that is not EOF has to surface, but only after the events
// already in hand have been handed over - §25's accounting contract says a
// batch cut short still reports what it applied.
func TestEventReaderAppliesWhatItHasBeforeReportingAReadError(t *testing.T) {
	one := `{"name":"a","value":1,"ts":2}`
	boom := errors.New("connection reset")

	r := io.MultiReader(
		strings.NewReader(one+"\n"+one+"\n"),
		errReader{boom},
	)

	er := newEventReader(r)
	defer er.release()

	read := 0
	var err error
	for {
		var ev Event
		if err = er.next(&ev); err != nil {
			break
		}
		read++
	}

	if read != 2 {
		t.Errorf("handed over %d events before the error, want 2", read)
	}
	if !errors.Is(err, boom) {
		t.Errorf("ended with %v, want the read error", err)
	}
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

// The buffer is pooled, so a reader must not keep using one it gave back -
// that is the classic pool bug, and it corrupts a different request rather
// than failing its own.
func TestEventReaderReleaseIsSafeAndRepeatable(t *testing.T) {
	er := newEventReader(strings.NewReader(`{"name":"a","value":1,"ts":2}`))

	var ev Event
	if err := er.next(&ev); err != nil {
		t.Fatal(err)
	}
	er.release()
	er.release() // must not double-put the same buffer into the pool

	if er.buf != nil || er.src != nil {
		t.Error("release left the reader usable; a later call would touch a pooled buffer")
	}
}

// Pooling must not let one request see another's bytes.
func TestPooledBuffersDoNotLeakBetweenReaders(t *testing.T) {
	for i := 0; i < 50; i++ {
		body := fmt.Sprintf(`{"name":"metric.%03d","value":%d,"ts":%d}`, i, i, i)

		got, err := drain(strings.NewReader(body))
		if err != io.EOF {
			t.Fatalf("iteration %d: %v", i, err)
		}
		if len(got) != 1 {
			t.Fatalf("iteration %d: read %d events", i, len(got))
		}
		want := fmt.Sprintf("metric.%03d", i)
		if got[0].Name != want || got[0].TS != int64(i) {
			t.Fatalf("iteration %d: got %+v, want name %q ts %d", i, got[0], want, i)
		}
	}
}

func BenchmarkEventReader(b *testing.B) {
	var sb strings.Builder
	for i := 0; i < 1000; i++ {
		fmt.Fprintf(&sb, "{\"name\":\"svc.metric.%d\",\"value\":%d.5,\"ts\":1758000000000}\n", i%16, i%10)
	}
	body := sb.String()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		er := newEventReader(strings.NewReader(body))
		n := 0
		for {
			var ev Event
			if err := er.next(&ev); err != nil {
				break
			}
			n++
		}
		er.release()
		if n != 1000 {
			b.Fatalf("read %d events, want 1000", n)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*1000), "ns/event")
}
