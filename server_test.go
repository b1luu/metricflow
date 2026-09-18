package main

import (
	"fmt"
	"io"
	"net"
	"net/http"
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
