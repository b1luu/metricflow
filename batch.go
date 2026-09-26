package main

// Batch ingest: many events in one request.
//
// The single-event /ingest is a clean contract and a bad firehose. §24 got
// the store to ~100 M events/sec on distinct metrics, but the server answers
// ~53 k requests/sec over loopback, so the engine's throughput is invisible
// from outside: the bottleneck moved off the aggregation path and onto the
// network, and one HTTP request per observation is the wrong unit of work.
// Batching moves it back.

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

const (
	// A batch body is deliberately much larger than a single event's 4 KiB
	// cap - amortising per-request cost is the entire point. It is still
	// bounded, and bounded twice, because the two limits stop different
	// things:
	//
	//   - maxBatchBody bounds the bytes one request can make the server
	//     read and parse, which is what a slow or hostile client abuses.
	//   - maxBatchEvents bounds the work in the unit that actually matters
	//     to everyone else: a batch takes the store's locks once per event,
	//     so an unbounded batch is an unbounded stall for /stats and for
	//     every other writer.
	maxBatchBody   = 1 << 20 // 1 MiB
	maxBatchEvents = 10000

	// Errors are reported per event, so a batch of 10000 malformed events
	// would otherwise produce a 10000-entry response - letting a client
	// turn a 1 MiB request into a much larger reply. The count of rejects
	// is always exact; only the listing is capped.
	maxBatchErrors = 10
)

// BatchError reports one rejected event by its position in the submitted
// batch. Index is 0-based and counts every event in the stream, accepted or
// rejected, so a client can map it straight back to the line it sent.
type BatchError struct {
	Index int    `json:"index"`
	Error string `json:"error"`

	// Retryable separates the two kinds of refusal, which matter very
	// differently to a client. A contract violation is permanent - that
	// event will never be valid, and resending it just wastes both sides'
	// time. A cardinality refusal is about the store being full right now
	// and may succeed later (§27). Without this a client would have to
	// parse the message to tell them apart, which is no contract at all.
	Retryable bool `json:"retryable"`
}

// BatchResponse is the JSON body of POST /ingest/batch.
//
// Accepted and Rejected always describe what actually happened, on every
// status code this endpoint returns. That is the contract: the body tells
// you exactly what landed, whether or not the request as a whole succeeded.
// It matters because the alternative - a 413 with no accounting - leaves a
// client unable to tell which events it must resend, and would break the
// one invariant this project holds everywhere else, that the events the
// server says it accepted are exactly the events it recorded.
//
// Errors is capped at maxBatchErrors, so len(Errors) < Rejected means the
// remaining rejects were not listed. Fatal is set when the stream stopped
// before the body was consumed, and says why; it is empty on a clean run.
type BatchResponse struct {
	Accepted int          `json:"accepted"`
	Rejected int          `json:"rejected"`
	Errors   []BatchError `json:"errors"`
	Fatal    string       `json:"fatal,omitempty"`
}

// handleIngestBatch: POST /ingest/batch - accept many events in one request.
//
// The body is newline-delimited JSON: one event object per line, no
// enclosing array. That choice is about memory, not taste. A JSON array has
// to be buffered and parsed whole before the first event can be looked at,
// and its length comes from the client, so the server would be sizing an
// allocation from untrusted input. A stream of values decodes one at a time
// in constant memory no matter how long the batch is, and lets the server
// stop early - at the event cap, or at a syntax error - without having
// parsed the rest.
//
// Rejection is per event, not per batch. A metrics firehose that discards
// 999 good samples because the 1000th was malformed is worse than useless,
// so a bad event costs exactly itself. It is never silent, though: the
// count is exact and the first few are named (§11's rule - an honest error
// beats quietly wrong data).
func (s *Store) handleIngestBatch(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBatchBody)
	events := newEventReader(r.Body)
	defer events.release()

	// One clock read for the whole batch, matching /ingest's single read
	// per request. Every event is judged against the same now, so a batch
	// can't have its first and last events disagree about where the window
	// edge is. A full batch takes on the order of a millisecond, which is
	// nothing against a 60 s window.
	now := time.Now()
	c, _ := clientFrom(r.Context())

	// Non-nil so an all-rejected batch encodes "errors":[] rather than
	// "errors":null - same reasoning as the alerts endpoint.
	resp := BatchResponse{Errors: []BatchError{}}

	status := http.StatusOK
	retryable := 0 // rejects that may succeed later, i.e. cardinality
	for index := 0; ; index++ {
		var ev Event
		err := events.next(&ev)
		if errors.Is(err, io.EOF) {
			break // clean end of the batch
		}
		if err != nil {
			resp.Fatal, status = describeStreamFailure(err, index)
			break
		}

		// Checked after the decode, not before it. Reaching index ==
		// maxBatchEvents with an event in hand means this is the
		// (cap+1)-th, so the cap is a maximum that a batch may hit exactly;
		// checking first would have rejected a full batch of exactly the
		// cap, which is the off-by-one the boundary test exists to catch.
		if index == maxBatchEvents {
			resp.Fatal = fmt.Sprintf("batch too large (max %d events); "+
				"everything before this point was applied", maxBatchEvents)
			status = http.StatusRequestEntityTooLarge
			break
		}

		if err := s.admitFor(now, c, ev); err != nil {
			resp.Rejected++
			mayRetry := retryableIngestError(err)
			if mayRetry {
				retryable++
			}
			if len(resp.Errors) < maxBatchErrors {
				resp.Errors = append(resp.Errors, BatchError{
					Index:     index,
					Error:     err.Error(),
					Retryable: mayRetry,
				})
			}
			continue
		}

		resp.Accepted++
	}

	// Nothing accepted needs a status, and which one depends on whose fault
	// it was. An empty body is a client that meant to send something, and
	// saying 200 to it would let a broken producer look healthy forever.
	//
	// A batch rejected *entirely* for cardinality is the interesting case:
	// every event was well-formed and the store was simply full. Answering
	// 400 there would tell the client to fix something that is not broken,
	// and a client obeying that would stop retrying data it should retry.
	// It gets the same 429 the single-event path gives (§27).
	if status == http.StatusOK && resp.Accepted == 0 {
		switch {
		case resp.Rejected == 0:
			status = http.StatusBadRequest
			resp.Fatal = "empty batch: no events in body"
		case retryable == resp.Rejected:
			status = http.StatusTooManyRequests
			w.Header().Set("Retry-After", strconv.Itoa(int(window.Seconds())))
		default:
			status = http.StatusBadRequest
		}
	}

	// Once per request with the batch's totals, rather than once per event
	// inside the loop: same numbers, one atomic instead of ten thousand
	// (§31).
	s.counts.addAccepted(resp.Accepted)
	s.counts.addRejected(resp.Rejected)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// Nothing useful to do if this fails: the client hung up mid-read, and
	// the events are already recorded either way.
	_ = json.NewEncoder(w).Encode(resp)
}

// describeStreamFailure turns a decode error into the Fatal message and the
// status for a batch that stopped early.
//
// A syntax error is not recoverable the way a rejected event is. Once the
// decoder is mid-token it has no way to find where the next event begins -
// a stray brace could be a truncated object or the start of a valid one -
// so there is nothing sound to resync to and the rest of the body is
// unreadable. The batch stops, and says so. Events decoded before the
// break stay applied, which is why the response still carries its counts.
func describeStreamFailure(err error, index int) (string, int) {
	var tooBig *http.MaxBytesError
	if errors.As(err, &tooBig) {
		return fmt.Sprintf("body exceeded %d bytes at event %d; "+
			"everything before this point was applied", maxBatchBody, index), http.StatusRequestEntityTooLarge
	}
	return fmt.Sprintf("malformed JSON at event %d (%v); the rest of the "+
		"body could not be read, everything before it was applied",
		index, err), http.StatusBadRequest
}
