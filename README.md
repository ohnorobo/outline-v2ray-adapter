# Outline SDK adapter for v2fly/v2ray-core/v5 — runnable PoC

A working Outline SDK transport adapter that routes connections through a
[`v2fly/v2ray-core/v5`](https://github.com/v2fly/v2ray-core) instance.

Unlike the earlier `samples/v2ray/` sketch (which compiled but only printed a
`net.Conn` and stopped short of the interface), this PoC:

- **fully implements `transport.StreamDialer` and returns a real
  `transport.StreamConn`** (with a half-close wrapper — v2ray's connection has
  no native `CloseRead`/`CloseWrite`);
- **implements `transport.PacketDialer` with a working `SetReadDeadline`**
  (v2ray's `core.DialUDP` returns a deadline-less, api:beta `PacketConn`);
- **actually moves bytes end-to-end** — the demo and tests dial a local origin
  server *through* v2ray's in-process dispatcher and verify the HTTP response,
  using only the Outline SDK interface;
- **stays lean** — the instance is built programmatically (dispatcher +
  proxyman + freedom), so the demo binary is ~14 MB instead of the ~52 MB you
  get from blank-importing `main/distro/all`.

## Layout

```
v2raytransport/        the adapter (the reusable part)
  dialer.go            StreamDialer / PacketDialer over *core.Instance
  conn.go              transport.StreamConn half-close wrapper
  packet.go            UDP net.Conn with emulated read deadlines
  dialer_test.go       end-to-end TCP through v2ray (freedom outbound)
  packet_internal_test.go  focused tests for the UDP deadline wrapper
internal/freedomcfg/   builds a self-contained direct-egress instance for the demo/tests
cmd/demo/              runnable end-to-end proof
```

## Run it

Requires Go 1.25+.

```sh
go test ./...              # end-to-end + unit tests
go run ./cmd/demo          # prints the proxied HTTP response
```

Expected demo output ends with:

```
status: 200 OK
body:   "hello from origin, via v2ray freedom outbound"
OK: bytes flowed end-to-end through v2ray-core via the Outline StreamDialer adapter
```

## Using it with a real proxy

The adapter is format-agnostic: `v2raytransport.NewStreamDialer(inst)` takes any
started `*core.Instance`. The PoC builds a `freedom` (direct) outbound so it
needs no server; for a real deployment, build the instance with a
vmess/vless/trojan/shadowsocks outbound instead (programmatically, or from JSON
via `core.StartInstance("json", ...)` after importing v2ray's JSON loaders).
The adapter code does not change.

The same adapter also works against **XTLS/Xray-core** and the MahsaNet
**GFW-knocker/Xray-core** fork — they expose the identical `core.Dial` /
`core.DialUDP` API. Swap the import path (and add a `go.mod replace` for the
forks).

## Known limitations (properties of v2ray-core, not the adapter)

- **Half-close is emulated.** v2ray's connection can't deliver a lone write-side
  FIN to the remote peer; a solitary `CloseWrite()` won't be observed as EOF by
  the other end. Framed request/response protocols (HTTP/1.1, etc.) are fine;
  schemes that read until EOF after a half-close may hang. See `conn.go`.
- **UDP deadlines are emulated** with a background reader goroutine, which may
  stay blocked in `ReadFrom` until a packet arrives or `Close()` is called. See
  `packet.go`.
- **License:** v2fly/v2ray-core is MIT (Apache-compatible). Xray-core is MPL-2.0
  (file-level copyleft) — needs a policy sign-off if you target those forks.
