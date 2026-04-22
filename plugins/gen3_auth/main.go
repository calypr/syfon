package main

import (
	"context"
	"github.com/hashicorp/go-plugin"
	"github.com/calypr/syfon/internal/api/middleware"
	"net/rpc"
	"os"
)

type Gen3AuthPlugin struct {
	MockEnabled bool
}

func (p *Gen3AuthPlugin) Authenticate(_ context.Context, in *middleware.AuthenticationInput) (*middleware.AuthenticationOutput, error) {
	if p.MockEnabled {
		return &middleware.AuthenticationOutput{Authenticated: true}, nil
	}
	if in.AuthHeader == "" {
		return &middleware.AuthenticationOutput{Authenticated: false, Reason: "missing auth header"}, nil
	}
	// Token extraction and privilege fetching logic would go here
	return &middleware.AuthenticationOutput{Authenticated: true}, nil
}

type Gen3AuthPluginRPC struct {
	plugin.Plugin
	Impl *Gen3AuthPlugin
}

func (p *Gen3AuthPluginRPC) Server(*plugin.MuxBroker) (interface{}, error) {
	return p.Impl, nil
}

func (p *Gen3AuthPluginRPC) Client(b *plugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return nil, nil // Not used in plugin binary
}

func main() {
	mock := os.Getenv("DRS_AUTH_MOCK_ENABLED") == "true"
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: middleware.Handshake,
		Plugins: map[string]plugin.Plugin{
			"authn": &Gen3AuthPluginRPC{Impl: &Gen3AuthPlugin{MockEnabled: mock}},
		},
	})
}
