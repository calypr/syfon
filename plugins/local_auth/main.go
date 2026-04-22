package main

import (
	"context"
	"github.com/hashicorp/go-plugin"
	"github.com/calypr/syfon/internal/api/middleware"
	"net/rpc"
	"os"
)

type LocalAuthPlugin struct {
	BasicUser string
	BasicPass string
}

func (p *LocalAuthPlugin) Authenticate(ctx context.Context, in *middleware.AuthenticationInput) (*middleware.AuthenticationOutput, error) {
	if p.BasicUser != "" || p.BasicPass != "" {
		err := ValidateBasicAuth(in.AuthHeader, p.BasicUser, p.BasicPass)
		if err != nil {
			return &middleware.AuthenticationOutput{Authenticated: false, Reason: err.Error()}, nil
		}
	}
	return &middleware.AuthenticationOutput{Authenticated: true}, nil
}

type LocalAuthPluginRPC struct {
	plugin.Plugin
	Impl *LocalAuthPlugin
}

func (p *LocalAuthPluginRPC) Server(*plugin.MuxBroker) (interface{}, error) {
	return p.Impl, nil
}

func (p *LocalAuthPluginRPC) Client(b *plugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return nil, nil // Not used in plugin binary
}

func main() {
	user := os.Getenv("DRS_BASIC_AUTH_USER")
	pass := os.Getenv("DRS_BASIC_AUTH_PASSWORD")
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: middleware.Handshake,
		Plugins: map[string]plugin.Plugin{
			"authn": &LocalAuthPluginRPC{Impl: &LocalAuthPlugin{BasicUser: user, BasicPass: pass}},
		},
	})
}
