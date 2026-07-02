package v2raytransport

import (
	"errors"
	"net"
	"os"
	"testing"
	"time"
)

// TestPacketConnDeadline verifies the deadline-emulation wrapper turns a
// deadline-less PacketConn (which is what v2ray's core.DialUDP returns) into
// one whose Read actually honours SetReadDeadline and reports a net timeout.
func TestPacketConnDeadline(t *testing.T) {
	// A plain UDP socket stands in for v2ray's dispatcher PacketConn. Its own
	// SetReadDeadline works, but we don't call it — we exercise the wrapper's
	// emulated deadline path, which is what matters for v2ray.
	udp, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	// remote nobody will send from — Read must time out.
	remote := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9}
	c := newPacketConn(udp, remote)
	defer c.Close()

	c.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	start := time.Now()
	_, err = c.Read(make([]byte, 1500))
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("expected net timeout error, got %v", err)
	}
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Errorf("expected errors.Is(err, os.ErrDeadlineExceeded)")
	}
	if elapsed < 100*time.Millisecond || elapsed > 2*time.Second {
		t.Errorf("timeout fired at %v, expected ~150ms", elapsed)
	}
}

// TestPacketConnRoundTrip verifies the wrapper binds to a remote, writes to it,
// and filters reads to packets from that remote.
func TestPacketConnRoundTrip(t *testing.T) {
	echo, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen echo: %v", err)
	}
	defer echo.Close()
	go func() {
		buf := make([]byte, 1500)
		for {
			n, from, err := echo.ReadFrom(buf)
			if err != nil {
				return
			}
			echo.WriteTo(buf[:n], from)
		}
	}()

	client, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen client: %v", err)
	}
	c := newPacketConn(client, echo.LocalAddr())
	defer c.Close()

	if _, err := c.Write([]byte("ping")); err != nil {
		t.Fatalf("write: %v", err)
	}
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1500)
	n, err := c.Read(buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got := string(buf[:n]); got != "ping" {
		t.Fatalf("read = %q, want %q", got, "ping")
	}
}
