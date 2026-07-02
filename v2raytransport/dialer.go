// Package v2raytransport adapts a running v2ray-core (v2fly/v2ray-core/v5)
// instance to the Outline SDK transport interfaces
// (golang.getoutline.org/sdk/transport): StreamDialer for TCP and PacketDialer
// for UDP.
//
// It targets github.com/v2fly/v2ray-core/v5 — the maintained successor to the
// abandoned github.com/v2ray/v2ray-core. The in-process dial API (core.Dial /
// core.DialUDP) is identical across v2fly v5, XTLS/Xray-core and the MahsaNet
// GFW-knocker/Xray-core fork, so the same adapter works against any of them by
// swapping the import path (and, for the forks, a go.mod replace).
//
// Design: the adapter is intentionally format-agnostic. It wraps a
// *core.Instance that the caller has already built and started, from whatever
// config source they prefer (programmatic protobuf, v4 JSON, v5 JSON). This
// keeps the adapter's dependency footprint minimal — callers that build
// instances programmatically (as the demo and tests here do) do not pull in
// v2ray's JSON config machinery or the full protocol distro.
package v2raytransport

import (
	"context"
	"fmt"
	"net"

	core "github.com/v2fly/v2ray-core/v5"
	v2net "github.com/v2fly/v2ray-core/v5/common/net"
	"golang.getoutline.org/sdk/transport"
)

// StreamDialer routes TCP stream connections through a v2ray-core instance.
// It implements transport.StreamDialer.
type StreamDialer struct {
	instance *core.Instance
}

var _ transport.StreamDialer = (*StreamDialer)(nil)

// NewStreamDialer wraps an already-started *core.Instance.
//
// The caller owns the instance lifecycle: build it (e.g. via core.New or
// core.StartInstance), Start() it if not already started, and Close() it when
// done. The instance's outbound configuration determines how dialed
// connections egress (vmess, vless, trojan, shadowsocks, freedom, ...).
func NewStreamDialer(instance *core.Instance) *StreamDialer {
	return &StreamDialer{instance: instance}
}

// DialStream implements transport.StreamDialer.
//
// raddr is "host:port"; host may be a domain or IP. The host is passed through
// to v2ray unresolved, so DNS resolution happens remotely at the proxy — the
// behaviour censorship-circumvention transports want.
func (d *StreamDialer) DialStream(ctx context.Context, raddr string) (transport.StreamConn, error) {
	dest, err := toDestination(raddr, v2net.Network_TCP)
	if err != nil {
		return nil, err
	}
	conn, err := core.Dial(ctx, d.instance, dest)
	if err != nil {
		return nil, fmt.Errorf("v2ray dial %s: %w", raddr, err)
	}
	return newStreamConn(conn), nil
}

// PacketDialer routes UDP packet connections through a v2ray-core instance.
// It implements transport.PacketDialer.
type PacketDialer struct {
	instance *core.Instance
}

var _ transport.PacketDialer = (*PacketDialer)(nil)

// NewPacketDialer wraps an already-started *core.Instance for UDP.
func NewPacketDialer(instance *core.Instance) *PacketDialer {
	return &PacketDialer{instance: instance}
}

// DialPacket implements transport.PacketDialer.
//
// core.DialUDP returns a dispatcher-level net.PacketConn that is marked
// api:beta and whose SetReadDeadline/SetWriteDeadline are no-ops. Outline's UDP
// handling relies on read deadlines, so we bind the returned unconnected
// PacketConn to the requested remote address and layer on a deadline-emulating
// wrapper (see packet.go).
func (d *PacketDialer) DialPacket(ctx context.Context, addr string) (net.Conn, error) {
	remote, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		// Fall back to a parsed host:port so domain targets still work; the
		// bound conn writes to this address via the dispatcher.
		host, portStr, splitErr := net.SplitHostPort(addr)
		if splitErr != nil {
			return nil, fmt.Errorf("invalid udp address %q: %w", addr, splitErr)
		}
		port, portErr := v2net.PortFromString(portStr)
		if portErr != nil {
			return nil, fmt.Errorf("invalid port in %q: %w", addr, portErr)
		}
		pc, dialErr := core.DialUDP(ctx, d.instance)
		if dialErr != nil {
			return nil, fmt.Errorf("v2ray dial udp: %w", dialErr)
		}
		return newPacketConn(pc, &v2rayUDPAddr{host: host, port: uint16(port)}), nil
	}
	pc, err := core.DialUDP(ctx, d.instance)
	if err != nil {
		return nil, fmt.Errorf("v2ray dial udp: %w", err)
	}
	return newPacketConn(pc, remote), nil
}

// Close tears down the underlying instance. Provided as a convenience for
// callers that hand the instance to a single dialer; if the instance is shared,
// close it directly instead.
func (d *StreamDialer) Close() error { return d.instance.Close() }

func toDestination(raddr string, network v2net.Network) (v2net.Destination, error) {
	host, portStr, err := net.SplitHostPort(raddr)
	if err != nil {
		return v2net.Destination{}, fmt.Errorf("invalid address %q: %w", raddr, err)
	}
	port, err := v2net.PortFromString(portStr)
	if err != nil {
		return v2net.Destination{}, fmt.Errorf("invalid port in %q: %w", raddr, err)
	}
	addr := v2net.ParseAddress(host)
	if network == v2net.Network_UDP {
		return v2net.UDPDestination(addr, port), nil
	}
	return v2net.TCPDestination(addr, port), nil
}

// v2rayUDPAddr is a net.Addr for a host:port that may be a domain name, used
// when the target could not be resolved to a concrete UDP address locally.
type v2rayUDPAddr struct {
	host string
	port uint16
}

func (a *v2rayUDPAddr) Network() string { return "udp" }
func (a *v2rayUDPAddr) String() string  { return fmt.Sprintf("%s:%d", a.host, a.port) }
