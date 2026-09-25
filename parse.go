package main

// A hand-written JSON parser for the one shape this server ingests.
//
// §25 measured the batch path at ~730 ns per event and found decoding was 536
// of it - 73%, against about 100 ns for everything the store does. A probe
// split that further: json.Unmarshal of a single event is ~353 ns and one
// 48-byte allocation, while validateEvent is 15 ns and none. So the cost is
// reflection, and the allocation is the metric name.
//
// Reflection is the right default for arbitrary structures and the wrong one
// for a fixed schema of three fields. This reads that schema directly.
//
// Writing a JSON parser is also an excellent way to be subtly wrong, so this
// one is held to encoding/json's behaviour by a differential fuzz test rather
// than by reasoning: for any input, the two must agree on both the decoded
// event and whether the input was valid at all. Everything odd below - the
// case-insensitive field fallback, last-duplicate-wins, null leaving a field
// alone - is there because that is what encoding/json does, and a wire format
// that changed under callers would be a worse bug than a slow one.

import (
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"
	"unicode/utf16"
	"unicode/utf8"
)

// errIncomplete means the input holds a prefix of what could be a valid
// value, and a caller reading from a stream should get more bytes and retry.
// It is deliberately distinct from a syntax error: one is "not yet", the
// other is "never".
var errIncomplete = errors.New("incomplete JSON value")

// parseEvent decodes one JSON object into ev and reports how many bytes it
// consumed, including any leading whitespace.
//
// ev is zeroed first, so a field the input omits reads as its zero value -
// which is what makes "missing" and "present but zero" indistinguishable here
// and is why §12 checks for them in validateEvent rather than at parse time.
func parseEvent(b []byte, ev *Event) (int, error) {
	p := parser{b: b}
	if err := p.event(ev); err != nil {
		return 0, err
	}
	return p.i, nil
}

type parser struct {
	b []byte
	i int
}

func (p *parser) atEnd() bool { return p.i >= len(p.b) }

// space skips JSON whitespace: space, tab, newline, carriage return, and
// nothing else. A parser that skipped more would accept documents
// encoding/json rejects.
func (p *parser) space() {
	for p.i < len(p.b) {
		switch p.b[p.i] {
		case ' ', '\t', '\n', '\r':
			p.i++
		default:
			return
		}
	}
}

func (p *parser) event(ev *Event) error {
	*ev = Event{}

	p.space()
	if p.atEnd() {
		return errIncomplete
	}

	// A bare null decodes into the zero Event without error, because that is
	// what encoding/json does for null into any type: it leaves the target
	// alone. Found by the differential table, not by reading the spec.
	if p.b[p.i] == 'n' {
		return p.lit("null")
	}

	if p.b[p.i] != '{' {
		return errors.New("expected a JSON object")
	}
	p.i++

	p.space()
	if p.atEnd() {
		return errIncomplete
	}
	if p.b[p.i] == '}' {
		p.i++
		return nil
	}

	for {
		p.space()
		if p.atEnd() {
			return errIncomplete
		}
		if p.b[p.i] != '"' {
			return errors.New("expected a field name")
		}
		key, err := p.key()
		if err != nil {
			return err
		}

		p.space()
		if p.atEnd() {
			return errIncomplete
		}
		if p.b[p.i] != ':' {
			return errors.New("expected ':' after a field name")
		}
		p.i++
		p.space()
		if p.atEnd() {
			return errIncomplete
		}

		if err := p.field(key, ev); err != nil {
			return err
		}

		p.space()
		if p.atEnd() {
			return errIncomplete
		}
		switch p.b[p.i] {
		case ',':
			p.i++
		case '}':
			p.i++
			return nil
		default:
			return errors.New("expected ',' or '}'")
		}
	}
}

// key reads a field name as bytes rather than a string.
//
// A key is compared and thrown away, never kept, so turning it into a string
// is an allocation bought for nothing - and there are three of them in every
// event. That was most of the gap between this parser's four allocations and
// encoding/json's one.
//
// Escaped keys take the slow path and do allocate. They are allowed, because
// encoding/json allows them, and they never occur: no producer writes
// {"na\u006de": ...}.
func (p *parser) key() ([]byte, error) {
	p.i++ // opening quote
	start := p.i

	for p.i < len(p.b) {
		switch c := p.b[p.i]; {
		case c == '"':
			raw := p.b[start:p.i]
			p.i++
			return raw, nil
		case c == '\\':
			p.i = start - 1 // back to the opening quote
			s, err := p.str()
			if err != nil {
				return nil, err
			}
			return []byte(s), nil
		case c < 0x20:
			return nil, errors.New("control character in string")
		default:
			p.i++
		}
	}
	return nil, errIncomplete
}

// field consumes one value, storing it if the key is one this server reads.
//
// The later of two duplicate keys wins, because that is what encoding/json
// does: it assigns as it goes rather than rejecting the second.
func (p *parser) field(key []byte, ev *Event) error {
	switch {
	case matches(key, "name"):
		return p.strField(&ev.Name)
	case matches(key, "value"):
		return p.floatField(&ev.Value)
	case matches(key, "ts"):
		return p.intField(&ev.TS)
	default:
		// Unknown fields are not an error (§7): a producer adding one must
		// not start failing against an older server. Skipped rather than
		// stored, and skipping still has to parse, or a "}" inside a nested
		// string would end the object early.
		return p.skip(1)
	}
}

// matches reports whether a key names a field, exactly or - failing that -
// case-insensitively, which is encoding/json's own rule.
func matches(key []byte, field string) bool {
	if len(key) != len(field) {
		return false
	}
	if string(key) == field {
		// Compares without allocating: the compiler special-cases
		// string(b) == s into a byte comparison.
		return true
	}
	for i := 0; i < len(key); i++ {
		c := key[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != field[i] {
			return false
		}
	}
	return true
}

// null reports whether the next value is null, consuming it if so.
//
// encoding/json treats null as "leave the field alone" for every type rather
// than as a type error, so every typed field checks for it first.
func (p *parser) null() (bool, error) {
	if p.b[p.i] != 'n' {
		return false, nil
	}
	return true, p.lit("null")
}

func (p *parser) lit(want string) error {
	if len(p.b)-p.i < len(want) {
		// Only a prefix so far: could still become the literal.
		if string(p.b[p.i:]) == want[:len(p.b)-p.i] {
			return errIncomplete
		}
		return errors.New("invalid literal")
	}
	if string(p.b[p.i:p.i+len(want)]) != want {
		return errors.New("invalid literal")
	}
	p.i += len(want)
	return nil
}

func (p *parser) strField(dst *string) error {
	if isNull, err := p.null(); err != nil || isNull {
		return err
	}
	if p.b[p.i] != '"' {
		return errors.New("expected a string")
	}
	s, err := p.str()
	if err != nil {
		return err
	}
	*dst = s
	return nil
}

func (p *parser) floatField(dst *float64) error {
	if isNull, err := p.null(); err != nil || isNull {
		return err
	}
	start, err := p.numberSpan()
	if err != nil {
		return err
	}
	// A small stack buffer, so the literal reaches ParseFloat without a heap
	// allocation. Numbers longer than this are pathological - 48 digits of
	// mantissa and exponent - and fall back to the allocating form rather
	// than being rejected, because encoding/json accepts them.
	lit := p.b[start:p.i]
	var f float64
	if len(lit) <= 48 {
		var buf [48]byte
		n := copy(buf[:], lit)
		f, err = strconv.ParseFloat(string(buf[:n]), 64)
	} else {
		f, err = strconv.ParseFloat(string(lit), 64)
	}
	if err != nil {
		return errors.New("invalid number")
	}
	*dst = f
	return nil
}

func (p *parser) intField(dst *int64) error {
	if isNull, err := p.null(); err != nil || isNull {
		return err
	}
	start, err := p.numberSpan()
	if err != nil {
		return err
	}

	// encoding/json parses an integer field with ParseInt on the literal
	// text, so "1e3" and "1.0" are errors even though they name whole
	// numbers. Matching that matters: a client sending 1.7e12 for ts gets
	// the same refusal it always did.
	n, err := parseInt(p.b[start:p.i])
	if err != nil {
		return errors.New("invalid integer")
	}
	*dst = n
	return nil
}

// parseInt reads a plain JSON integer without allocating. Anything with a
// fraction or exponent is rejected, as encoding/json rejects it for an int64
// field.
func parseInt(b []byte) (int64, error) {
	if len(b) == 0 {
		return 0, errors.New("empty")
	}
	i, neg := 0, false
	if b[0] == '-' {
		neg, i = true, 1
	}
	if i == len(b) {
		return 0, errors.New("no digits")
	}

	var n uint64
	for ; i < len(b); i++ {
		c := b[i]
		if c < '0' || c > '9' {
			return 0, errors.New("not an integer")
		}
		if n > (1<<63)/10 {
			return 0, errors.New("out of range")
		}
		n = n*10 + uint64(c-'0')
		if n > 1<<63 {
			return 0, errors.New("out of range")
		}
	}
	if neg {
		if n > 1<<63 {
			return 0, errors.New("out of range")
		}
		return -int64(n-1) - 1, nil // avoids overflowing at MinInt64
	}
	if n > 1<<63-1 {
		return 0, errors.New("out of range")
	}
	return int64(n), nil
}

// numberSpan advances over a JSON number and returns where it started.
//
// It enforces the grammar rather than grabbing everything numeric-looking,
// because encoding/json does: "01", "1.", "+1" and ".5" are all errors, and a
// parser that accepted them would accept documents the old one refused.
func (p *parser) numberSpan() (int, error) {
	start := p.i

	if !p.atEnd() && p.b[p.i] == '-' {
		p.i++
	}
	// Integer part: a single 0, or a non-zero digit run.
	if p.atEnd() {
		return 0, errIncomplete
	}
	switch c := p.b[p.i]; {
	case c == '0':
		p.i++
	case c >= '1' && c <= '9':
		for !p.atEnd() && isDigit(p.b[p.i]) {
			p.i++
		}
	default:
		return 0, errors.New("expected a number")
	}

	// Fraction.
	if !p.atEnd() && p.b[p.i] == '.' {
		p.i++
		if p.atEnd() {
			return 0, errIncomplete
		}
		if !isDigit(p.b[p.i]) {
			return 0, errors.New("expected digits after '.'")
		}
		for !p.atEnd() && isDigit(p.b[p.i]) {
			p.i++
		}
	}

	// Exponent.
	if !p.atEnd() && (p.b[p.i] == 'e' || p.b[p.i] == 'E') {
		p.i++
		if !p.atEnd() && (p.b[p.i] == '+' || p.b[p.i] == '-') {
			p.i++
		}
		if p.atEnd() {
			return 0, errIncomplete
		}
		if !isDigit(p.b[p.i]) {
			return 0, errors.New("expected digits in the exponent")
		}
		for !p.atEnd() && isDigit(p.b[p.i]) {
			p.i++
		}
	}

	// A number at the very end of the buffer might have more digits coming.
	// The caller distinguishes a stream (refill and retry) from a complete
	// document (this really is the end).
	if p.atEnd() {
		return 0, errIncomplete
	}
	return start, nil
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// str reads a JSON string starting at the opening quote.
//
// The common case - no escapes - is one pass and one allocation for the
// resulting string, the same as encoding/json. Escapes take a second pass
// through a buffer, which is worth nothing to optimise: metric names do not
// contain them.
func (p *parser) str() (string, error) {
	p.i++ // opening quote
	start := p.i

	// Tracked rather than checked afterwards, so a pure-ASCII name - which
	// is very nearly all of them - never pays for a second pass.
	high := false

	for p.i < len(p.b) {
		switch c := p.b[p.i]; {
		case c == '"':
			raw := p.b[start:p.i]
			p.i++
			if high && !utf8.Valid(raw) {
				return coerceUTF8(raw), nil
			}
			return string(raw), nil
		case c == '\\':
			return p.strEscaped(start)
		case c < 0x20:
			// Raw control characters are invalid in a JSON string, and
			// encoding/json rejects them rather than passing them through.
			return "", errors.New("control character in string")
		default:
			if c >= utf8.RuneSelf {
				high = true
			}
			p.i++
		}
	}
	return "", errIncomplete
}

// coerceUTF8 replaces every byte that is not part of a valid UTF-8 sequence
// with U+FFFD.
//
// This exists because the fuzzer found it, on its eighth seed and within a
// tenth of a second. encoding/json does not reject invalid UTF-8 in a string,
// it silently repairs it, so a parser that passed the raw bytes through would
// have stored metric names the old decoder could never have produced - a
// difference no table of hand-written cases here had thought to look for.
func coerceUTF8(b []byte) string {
	var sb strings.Builder
	sb.Grow(len(b))

	for i := 0; i < len(b); {
		if c := b[i]; c < utf8.RuneSelf {
			sb.WriteByte(c)
			i++
			continue
		}
		r, size := utf8.DecodeRune(b[i:])
		if r == utf8.RuneError && size == 1 {
			sb.WriteRune(utf8.RuneError)
			i++
			continue
		}
		sb.Write(b[i : i+size])
		i += size
	}
	return sb.String()
}

// strEscaped finishes a string that contains at least one escape, from the
// backslash onwards.
func (p *parser) strEscaped(start int) (string, error) {
	buf := make([]byte, 0, len(p.b)-start)
	buf = append(buf, p.b[start:p.i]...)

	for p.i < len(p.b) {
		c := p.b[p.i]
		switch {
		case c == '"':
			p.i++
			if !utf8.Valid(buf) {
				return coerceUTF8(buf), nil
			}
			return string(buf), nil
		case c < 0x20:
			return "", errors.New("control character in string")
		case c != '\\':
			buf = append(buf, c)
			p.i++
			continue
		}

		p.i++ // the backslash
		if p.atEnd() {
			return "", errIncomplete
		}
		switch p.b[p.i] {
		case '"', '\\', '/':
			buf = append(buf, p.b[p.i])
			p.i++
		case 'b':
			buf = append(buf, '\b')
			p.i++
		case 'f':
			buf = append(buf, '\f')
			p.i++
		case 'n':
			buf = append(buf, '\n')
			p.i++
		case 'r':
			buf = append(buf, '\r')
			p.i++
		case 't':
			buf = append(buf, '\t')
			p.i++
		case 'u':
			r, err := p.unicodeEscape()
			if err != nil {
				return "", err
			}
			buf = utf8.AppendRune(buf, r)
		default:
			return "", errors.New("invalid escape")
		}
	}
	return "", errIncomplete
}

// unicodeEscape reads \uXXXX, joining a surrogate pair when one follows.
//
// An unpaired surrogate becomes U+FFFD, which is what encoding/json does -
// it does not reject them, so neither can this.
func (p *parser) unicodeEscape() (rune, error) {
	p.i++ // the 'u'
	r, err := p.hex4()
	if err != nil {
		return 0, err
	}

	if !utf16.IsSurrogate(r) {
		return r, nil
	}

	// A low surrogate may follow as another \uXXXX. Anything else leaves
	// this one unpaired.
	if len(p.b)-p.i < 6 {
		if len(p.b) == p.i || (p.b[p.i] == '\\' && (len(p.b)-p.i < 2 || p.b[p.i+1] == 'u')) {
			return 0, errIncomplete
		}
		return utf8.RuneError, nil
	}
	if p.b[p.i] != '\\' || p.b[p.i+1] != 'u' {
		return utf8.RuneError, nil
	}

	save := p.i
	p.i += 2
	r2, err := p.hex4()
	if err != nil {
		return 0, err
	}
	if joined := utf16.DecodeRune(r, r2); joined != utf8.RuneError {
		return joined, nil
	}
	p.i = save // not a pair after all; leave the second escape to the caller
	return utf8.RuneError, nil
}

func (p *parser) hex4() (rune, error) {
	if len(p.b)-p.i < 4 {
		return 0, errIncomplete
	}
	var r rune
	for j := 0; j < 4; j++ {
		c := p.b[p.i+j]
		var v rune
		switch {
		case c >= '0' && c <= '9':
			v = rune(c - '0')
		case c >= 'a' && c <= 'f':
			v = rune(c-'a') + 10
		case c >= 'A' && c <= 'F':
			v = rune(c-'A') + 10
		default:
			return 0, errors.New("invalid \\u escape")
		}
		r = r*16 + v
	}
	p.i += 4
	return r, nil
}

// maxDepth mirrors encoding/json's nesting limit. Without one, a body of
// nothing but open brackets recurses once per byte, and maxBatchBody would
// let a client send a million of them.
const maxDepth = 10000

// skip consumes one value of any type without interpreting it.
//
// It parses rather than scans. The first version counted brackets and was
// caught by the differential table on {"a":{"b":}} and {"a":[1,]} - both of
// which balance perfectly and neither of which is JSON. A skipped value still
// has to be a value, or this server accepts documents the old decoder
// refused, which is a wire-contract change hiding inside an optimisation.
func (p *parser) skip(depth int) error {
	if depth > maxDepth {
		return errors.New("exceeded max depth")
	}

	p.space()
	if p.atEnd() {
		return errIncomplete
	}
	switch c := p.b[p.i]; {
	case c == '"':
		return p.skipString()
	case c == '{':
		return p.skipObject(depth)
	case c == '[':
		return p.skipArray(depth)
	case c == 't':
		return p.lit("true")
	case c == 'f':
		return p.lit("false")
	case c == 'n':
		return p.lit("null")
	default:
		_, err := p.numberSpan()
		return err
	}
}

// skipString walks a string without building one.
//
// Unknown fields are common - a producer adds host and tags and a trace ID -
// and every nested key and string value inside them was being materialised
// just to be dropped. It still validates escapes, because encoding/json's
// scanner does, and a skipped field that was malformed has to stay an error.
func (p *parser) skipString() error {
	p.i++ // opening quote

	for p.i < len(p.b) {
		switch c := p.b[p.i]; {
		case c == '"':
			p.i++
			return nil
		case c == '\\':
			p.i++
			if p.atEnd() {
				return errIncomplete
			}
			switch p.b[p.i] {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
				p.i++
			case 'u':
				p.i++
				if _, err := p.hex4(); err != nil {
					return err
				}
			default:
				return errors.New("invalid escape")
			}
		case c < 0x20:
			return errors.New("control character in string")
		default:
			p.i++
		}
	}
	return errIncomplete
}

func (p *parser) skipObject(depth int) error {
	p.i++ // '{'

	p.space()
	if p.atEnd() {
		return errIncomplete
	}
	if p.b[p.i] == '}' {
		p.i++
		return nil
	}

	for {
		p.space()
		if p.atEnd() {
			return errIncomplete
		}
		if p.b[p.i] != '"' {
			return errors.New("expected a field name")
		}
		if err := p.skipString(); err != nil {
			return err
		}

		p.space()
		if p.atEnd() {
			return errIncomplete
		}
		if p.b[p.i] != ':' {
			return errors.New("expected ':' after a field name")
		}
		p.i++

		if err := p.skip(depth + 1); err != nil {
			return err
		}

		p.space()
		if p.atEnd() {
			return errIncomplete
		}
		switch p.b[p.i] {
		case ',':
			p.i++
		case '}':
			p.i++
			return nil
		default:
			return errors.New("expected ',' or '}'")
		}
	}
}

func (p *parser) skipArray(depth int) error {
	p.i++ // '['

	p.space()
	if p.atEnd() {
		return errIncomplete
	}
	if p.b[p.i] == ']' {
		p.i++
		return nil
	}

	for {
		if err := p.skip(depth + 1); err != nil {
			return err
		}

		p.space()
		if p.atEnd() {
			return errIncomplete
		}
		switch p.b[p.i] {
		case ',':
			p.i++
		case ']':
			p.i++
			return nil
		default:
			return errors.New("expected ',' or ']'")
		}
	}
}

// --- streaming ---

// readBufSize is where an event stream's buffer starts. Events are a few
// hundred bytes, so this holds dozens of them per read syscall; it grows only
// if one event does not fit.
const readBufSize = 16 << 10

// eventReader pulls events one at a time out of a stream of them.
//
// It exists because §25 chose a stream deliberately: a batch body is decoded
// in constant memory however long it runs, rather than being buffered whole
// so its length could be read from the client. Replacing json.Decoder meant
// replacing that too.
//
// It is not line-based, and that is a contract decision rather than an
// oversight. §25 documented and tested that events may be separated by any
// whitespace - pretty-printed, or several to a line - because json.Decoder
// reads a stream of values rather than lines. Going line-based would have
// been simpler and would have silently narrowed what the server accepts.
type eventReader struct {
	src io.Reader
	buf []byte
	pos int   // start of the unconsumed bytes
	end int   // end of the valid bytes
	err error // sticky: the read that ended the stream, EOF or otherwise

	// The pooled array buf started as. Kept separately because growth
	// replaces buf, and it is the original that goes back in the pool.
	pooled *[]byte
}

// readBufPool keeps the read buffers alive between requests.
//
// Without it every request allocated 16 KiB, which a batch of a thousand
// events never notices and a batch of one is dominated by: the first
// measurement of this reader showed size-1 batches getting *slower* than the
// decoder they replaced, at 21 kB per request. The buffer is the same for
// every request and lives exactly as long as one, which is what a pool is
// for.
var readBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, readBufSize)
		return &b
	},
}

func newEventReader(r io.Reader) *eventReader {
	p := readBufPool.Get().(*[]byte)
	return &eventReader{src: r, buf: *p, pooled: p}
}

// release returns the buffer to the pool. Callers defer it.
//
// Only the original is returned, never one that grew to hold an oversized
// event: pooling those would let a single hostile request leave a megabyte
// of buffer resident for the life of the process.
func (er *eventReader) release() {
	if er.pooled != nil {
		readBufPool.Put(er.pooled)
		er.pooled = nil
	}
	er.buf, er.src = nil, nil
}

// next decodes the event after the last one, returning io.EOF at a clean end
// of stream.
//
// A value split across two reads is the case this is all for: parseEvent says
// errIncomplete, more bytes arrive, and the same value is parsed again from
// its start. Re-parsing a prefix is cheap and happens at most once per refill,
// where the alternative - a resumable parser - would complicate every
// function in this file to save nothing measurable.
func (er *eventReader) next(ev *Event) error {
	for {
		n, err := parseEvent(er.buf[er.pos:er.end], ev)
		if err == nil {
			er.pos += n
			return nil
		}
		if err != errIncomplete {
			return err
		}

		// Incomplete, so either more bytes are coming or the stream ended
		// mid-value - or it ended cleanly and what is left is whitespace.
		if er.err != nil {
			if er.err == io.EOF {
				if er.onlySpaceLeft() {
					return io.EOF // the client finished
				}
				// A prefix of a value with nothing following: the batch
				// really was cut short, so not a clean end.
				return io.ErrUnexpectedEOF
			}

			// Any other read failure is reported even when the bytes left
			// over are only whitespace. The events already handed over
			// stand, but the stream did not end - it broke - and the server
			// cannot know whether more events were in flight. Calling that
			// a clean end would tell a client its whole batch was seen.
			return er.err
		}
		if err := er.fill(); err != nil {
			return err
		}
	}
}

func (er *eventReader) onlySpaceLeft() bool {
	for _, c := range er.buf[er.pos:er.end] {
		switch c {
		case ' ', '\t', '\n', '\r':
		default:
			return false
		}
	}
	return true
}

// fill makes room and reads more.
func (er *eventReader) fill() error {
	// Slide the unconsumed bytes to the front before growing: usually the
	// buffer is mostly consumed and no growth is needed at all.
	if er.pos > 0 {
		copy(er.buf, er.buf[er.pos:er.end])
		er.end -= er.pos
		er.pos = 0
	}

	if er.end == len(er.buf) {
		// One event is bigger than the whole buffer. Growing is bounded by
		// the body cap, which MaxBytesReader is enforcing on the reader
		// anyway - so this can only ever double a few times.
		if len(er.buf) >= maxBatchBody {
			return errors.New("single event exceeds the body limit")
		}
		grown := make([]byte, min(len(er.buf)*2, maxBatchBody))
		copy(grown, er.buf[:er.end])
		er.buf = grown
	}

	n, err := er.src.Read(er.buf[er.end:])
	er.end += n
	if err != nil {
		// Recorded rather than returned: the bytes just read may still hold
		// whole events, and they must be applied before the stream's end is
		// reported (§25's accounting contract).
		er.err = err
	}
	return nil
}
