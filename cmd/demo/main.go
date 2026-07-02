// Command demo is a runnable end-to-end proof that the v2raytransport adapter
// satisfies Outline's transport.StreamDialer AND actually moves bytes through
// a v2ray-core instance.
//
// It is fully self-contained — no external proxy server required:
//
//  1. Start a local HTTP server (the "origin" we want to reach).
//  2. Build a v2ray-core instance whose outbound is "freedom" (direct egress).
//  3. Wrap it with v2raytransport.NewStreamDialer -> a transport.StreamDialer.
//  4. Use ONLY the Outline SDK's StreamDialer API to dial the origin and speak
//     HTTP/1.1 over the returned transport.StreamConn (exercising CloseWrite).
//  5. Verify we got the origin's response body back.
//
// Because the dial is dispatched through v2ray's routing core into the freedom
// handler, a successful fetch proves the full in-process data path works, not
// just that the code compiles. Swap the instance's outbound for a real
// vmess/vless config and the exact same adapter code proxies through a server.
package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"time"

	"golang.getoutline.org/sdk/transport"

	"github.com/ohnorobo/outline-v2ray-adapter/internal/freedomcfg"
	"github.com/ohnorobo/outline-v2ray-adapter/v2raytransport"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "demo failed:", err)
		os.Exit(1)
	}
}

func run() error {
	// 1. Local origin server.
	const want = "hello from origin, via v2ray freedom outbound"
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, want)
	}))
	defer origin.Close()

	originHost := origin.Listener.Addr().String() // 127.0.0.1:PORT
	fmt.Println("origin listening at", originHost)

	// 2 + 3. v2ray instance -> Outline StreamDialer.
	instance, err := freedomcfg.NewInstance()
	if err != nil {
		return fmt.Errorf("build v2ray instance: %w", err)
	}
	var dialer transport.StreamDialer = v2raytransport.NewStreamDialer(instance)
	defer instance.Close()

	// 4. Dial through the adapter using only the Outline SDK interface.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := dialer.DialStream(ctx, originHost)
	if err != nil {
		return fmt.Errorf("DialStream: %w", err)
	}
	defer conn.Close()

	host, _, _ := net.SplitHostPort(originHost)
	req := fmt.Sprintf("GET / HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", host)
	if _, err := io.WriteString(conn, req); err != nil {
		return fmt.Errorf("write request: %w", err)
	}
	// Exercise the adapter's half-close: signal we're done writing.
	if err := conn.CloseWrite(); err != nil {
		return fmt.Errorf("CloseWrite: %w", err)
	}

	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read body: %w", err)
	}

	fmt.Printf("status: %s\n", resp.Status)
	fmt.Printf("body:   %q\n", string(body))
	if string(body) != want {
		return fmt.Errorf("body mismatch: got %q want %q", body, want)
	}
	fmt.Println("OK: bytes flowed end-to-end through v2ray-core via the Outline StreamDialer adapter")
	return nil
}
