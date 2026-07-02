package v2raytransport_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"golang.getoutline.org/sdk/transport"

	"github.com/ohnorobo/outline-v2ray-adapter/internal/freedomcfg"
	"github.com/ohnorobo/outline-v2ray-adapter/v2raytransport"
)

// TestDialStreamEndToEnd proves the adapter moves bytes through v2ray-core:
// it dials a local origin server through a freedom outbound using only the
// Outline transport.StreamDialer interface and checks the response body.
func TestDialStreamEndToEnd(t *testing.T) {
	const want = "ok-through-v2ray"
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, want)
	}))
	defer origin.Close()

	instance, err := freedomcfg.NewInstance()
	if err != nil {
		t.Fatalf("build instance: %v", err)
	}
	defer instance.Close()

	var dialer transport.StreamDialer = v2raytransport.NewStreamDialer(instance)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := dialer.DialStream(ctx, origin.Listener.Addr().String())
	if err != nil {
		t.Fatalf("DialStream: %v", err)
	}
	defer conn.Close()

	host, _, _ := net.SplitHostPort(origin.Listener.Addr().String())
	fmt.Fprintf(conn, "GET / HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", host)
	if err := conn.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}

	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if string(body) != want {
		t.Fatalf("body = %q, want %q", body, want)
	}
}

// TestStreamConnInterface asserts the returned connection is a real
// transport.StreamConn (half-close methods present and non-panicking).
func TestStreamConnInterface(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer origin.Close()

	instance, err := freedomcfg.NewInstance()
	if err != nil {
		t.Fatalf("build instance: %v", err)
	}
	defer instance.Close()

	dialer := v2raytransport.NewStreamDialer(instance)
	conn, err := dialer.DialStream(context.Background(), origin.Listener.Addr().String())
	if err != nil {
		t.Fatalf("DialStream: %v", err)
	}
	defer conn.Close()

	var _ transport.StreamConn = conn // compile-time; also verify at runtime:
	if _, ok := interface{}(conn).(transport.StreamConn); !ok {
		t.Fatal("returned conn does not implement transport.StreamConn")
	}
	// Half-close both ends should not error for a live connection.
	if err := conn.CloseWrite(); err != nil {
		t.Errorf("CloseWrite: %v", err)
	}
	if err := conn.CloseRead(); err != nil {
		t.Errorf("CloseRead: %v", err)
	}
}
