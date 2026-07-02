package v2raytransport

import (
	"net"
	"sync"

	"golang.getoutline.org/sdk/transport"
)

// streamConn adapts a v2ray-core connection (a bare net.Conn returned by
// core.Dial) to transport.StreamConn, which additionally requires
// CloseRead and CloseWrite for half-open support.
//
// v2ray-core's in-process connection (common/net.connection) is a
// bidirectional pipe over the routing dispatcher with a single Close(); it
// does NOT expose native half-close, and it cannot propagate a FIN to the
// remote peer independently of tearing the whole connection down. We therefore
// emulate half-close with local bookkeeping:
//
//   - If the underlying conn happens to implement CloseRead/CloseWrite
//     natively (some engines return a *net.TCPConn), we delegate to it.
//   - Otherwise, CloseRead/CloseWrite mark that half as closed locally, and
//     once BOTH halves are closed the underlying conn is Close()d.
//
// The honest limitation: a lone CloseWrite does not deliver EOF to the remote
// end the way a real TCP half-close would. Protocols that depend on the peer
// observing a write-side FIN (e.g. some request/response schemes that read
// until EOF) may hang. Plain request/response over framed protocols
// (HTTP/1.1, most application traffic) is unaffected. Document this for
// production use; it is a property of v2ray-core, not of this adapter.
type streamConn struct {
	net.Conn
	mu          sync.Mutex
	readClosed  bool
	writeClosed bool
	closed      bool
}

var _ transport.StreamConn = (*streamConn)(nil)

type closeReader interface{ CloseRead() error }
type closeWriter interface{ CloseWrite() error }

func newStreamConn(c net.Conn) *streamConn {
	return &streamConn{Conn: c}
}

func (c *streamConn) CloseRead() error {
	if cr, ok := c.Conn.(closeReader); ok {
		return cr.CloseRead()
	}
	c.mu.Lock()
	c.readClosed = true
	bothClosed := c.writeClosed
	c.mu.Unlock()
	if bothClosed {
		return c.Close()
	}
	return nil
}

func (c *streamConn) CloseWrite() error {
	if cw, ok := c.Conn.(closeWriter); ok {
		return cw.CloseWrite()
	}
	c.mu.Lock()
	c.writeClosed = true
	bothClosed := c.readClosed
	c.mu.Unlock()
	if bothClosed {
		return c.Close()
	}
	return nil
}

func (c *streamConn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()
	return c.Conn.Close()
}
