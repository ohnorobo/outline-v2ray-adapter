// Package freedomcfg builds a minimal, self-contained v2ray-core instance whose
// only outbound is "freedom" (direct egress). It exists so the demo and tests
// can prove the Outline adapter actually moves bytes through v2ray's in-process
// dispatcher WITHOUT needing a remote vmess/vless server: DialStream dispatches
// through the routing core into the freedom handler, which makes the real
// outbound connection.
//
// A production adapter would instead build an instance with a vmess/vless/etc.
// outbound (from JSON or protobuf); the adapter code in v2raytransport is
// identical either way — only the instance's outbound config changes.
//
// Building the config programmatically (rather than from JSON) keeps the
// dependency footprint small: no JSON config machinery, no full protocol
// distro — just the dispatcher, the proxyman managers, and freedom.
package freedomcfg

import (
	"google.golang.org/protobuf/types/known/anypb"

	core "github.com/v2fly/v2ray-core/v5"
	"github.com/v2fly/v2ray-core/v5/app/dispatcher"
	"github.com/v2fly/v2ray-core/v5/app/proxyman"
	"github.com/v2fly/v2ray-core/v5/common/serial"
	"github.com/v2fly/v2ray-core/v5/proxy/freedom"

	// Register the feature implementations referenced by the config below.
	_ "github.com/v2fly/v2ray-core/v5/app/dispatcher"
	_ "github.com/v2fly/v2ray-core/v5/app/proxyman/inbound"
	_ "github.com/v2fly/v2ray-core/v5/app/proxyman/outbound"
)

// NewInstance returns a started *core.Instance with a single freedom outbound.
// The caller must Close() it.
func NewInstance() (*core.Instance, error) {
	config := &core.Config{
		App: []*anypb.Any{
			serial.ToTypedMessage(&dispatcher.Config{}),
			serial.ToTypedMessage(&proxyman.InboundConfig{}),
			serial.ToTypedMessage(&proxyman.OutboundConfig{}),
		},
		Outbound: []*core.OutboundHandlerConfig{
			{
				Tag:           "direct",
				ProxySettings: serial.ToTypedMessage(&freedom.Config{}),
			},
		},
	}
	instance, err := core.New(config)
	if err != nil {
		return nil, err
	}
	if err := instance.Start(); err != nil {
		return nil, err
	}
	return instance, nil
}
