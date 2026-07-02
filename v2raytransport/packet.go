package v2raytransport

import (
	"net"
	"os"
	"sync"
	"time"
)

// packetConn turns v2ray-core's unconnected, deadline-less dispatcher
// PacketConn into a net.Conn bound to a single remote address, with a working
// SetReadDeadline.
//
// Two problems with core.DialUDP's return value that this fixes:
//
//  1. It is a net.PacketConn (ReadFrom/WriteTo), but Outline's PacketDialer
//     contract wants a net.Conn (Read/Write) bound to one destination. We bind
//     to `remote`: Write sends there, Read filters to packets from there.
//
//  2. Its Set*Deadline methods are no-ops (see v2ray common/net.connection).
//     Outline relies on read deadlines for UDP timeouts. We emulate a read
//     deadline with a single background reader goroutine feeding a channel;
//     Read races the channel against a deadline timer.
//
// The background goroutine may remain blocked in ReadFrom until a packet
// arrives or Close() is called (Close unblocks it by closing the underlying
// conn). This is the standard trade-off for emulating deadlines over a
// transport that lacks them.
type packetConn struct {
	pc     net.PacketConn
	remote net.Addr

	reads    chan readResult
	closeOne sync.Once
	closed   chan struct{}

	mu           sync.Mutex
	readDeadline time.Time
}

type readResult struct {
	data []byte
	err  error
}

var _ net.Conn = (*packetConn)(nil)

func newPacketConn(pc net.PacketConn, remote net.Addr) *packetConn {
	c := &packetConn{
		pc:     pc,
		remote: remote,
		reads:  make(chan readResult, 8),
		closed: make(chan struct{}),
	}
	go c.readLoop()
	return c
}

func (c *packetConn) readLoop() {
	buf := make([]byte, 64*1024)
	for {
		n, from, err := c.pc.ReadFrom(buf)
		if err != nil {
			select {
			case c.reads <- readResult{err: err}:
			case <-c.closed:
			}
			return
		}
		// Only surface packets from our bound remote, mirroring the SDK's
		// boundPacketConn. Domain-form remotes (v2rayUDPAddr) match by string.
		if from != nil && c.remote != nil && from.String() != c.remote.String() {
			continue
		}
		cp := make([]byte, n)
		copy(cp, buf[:n])
		select {
		case c.reads <- readResult{data: cp}:
		case <-c.closed:
			return
		}
	}
}

func (c *packetConn) Read(b []byte) (int, error) {
	c.mu.Lock()
	deadline := c.readDeadline
	c.mu.Unlock()

	var timeout <-chan time.Time
	if !deadline.IsZero() {
		d := time.Until(deadline)
		if d <= 0 {
			return 0, timeoutError{}
		}
		t := time.NewTimer(d)
		defer t.Stop()
		timeout = t.C
	}

	select {
	case <-c.closed:
		return 0, net.ErrClosed
	case <-timeout:
		return 0, timeoutError{}
	case r := <-c.reads:
		if r.err != nil {
			return 0, r.err
		}
		return copy(b, r.data), nil
	}
}

func (c *packetConn) Write(b []byte) (int, error) {
	return c.pc.WriteTo(b, c.remote)
}

func (c *packetConn) Close() error {
	c.closeOne.Do(func() { close(c.closed) })
	return c.pc.Close()
}

func (c *packetConn) LocalAddr() net.Addr  { return c.pc.LocalAddr() }
func (c *packetConn) RemoteAddr() net.Addr { return c.remote }

func (c *packetConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	c.readDeadline = t
	c.mu.Unlock()
	return nil
}

func (c *packetConn) SetWriteDeadline(t time.Time) error {
	// v2ray's dispatcher writes do not block on a socket buffer in a way we can
	// deadline meaningfully; accept and no-op, matching the underlying conn.
	return nil
}

func (c *packetConn) SetDeadline(t time.Time) error {
	return c.SetReadDeadline(t)
}

// timeoutError is a net.Error reporting a timeout, so callers that type-assert
// for Timeout() (as Outline's UDP handling does) behave correctly.
type timeoutError struct{}

func (timeoutError) Error() string   { return "v2ray udp: i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }
func (timeoutError) Unwrap() error   { return os.ErrDeadlineExceeded }
