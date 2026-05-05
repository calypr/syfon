package middleware

import (
	"context"
	"net/rpc"
	"os"
	"os/exec"
	"sync"

	hplugin "github.com/hashicorp/go-plugin"

	"github.com/calypr/syfon/plugin"
)

// AuthenticationPluginManager manages the plugin process and calls Authenticate.
type AuthenticationPluginManager struct {
	client *PluginClient
}

// NewAuthenticationPluginManager loads the plugin binary and returns a manager.
func NewAuthenticationPluginManager(pluginPath string) (*AuthenticationPluginManager, error) {
	client := hplugin.NewClient(&hplugin.ClientConfig{
		HandshakeConfig: Handshake,
		Plugins: map[string]hplugin.Plugin{
			"authn": &AuthnPluginRPC{},
		},
		Cmd:              exec.Command(pluginPath),
		AllowedProtocols: []hplugin.Protocol{hplugin.ProtocolNetRPC},
	})

	rpcClient, err := client.Client()
	if err != nil {
		return nil, err
	}

	raw, err := rpcClient.Dispense("authn")
	if err != nil {
		return nil, err
	}

	return &AuthenticationPluginManager{client: &PluginClient{client: client, raw: raw}}, nil
}

// Authenticate delegates to the plugin.
func (pm *AuthenticationPluginManager) Authenticate(ctx context.Context, in *plugin.AuthenticationInput) (*plugin.AuthenticationOutput, error) {
	pm.client.mu.Lock()
	defer pm.client.mu.Unlock()
	pluginImpl, ok := pm.client.raw.(plugin.AuthenticationPlugin)
	if !ok {
		return nil, os.ErrInvalid
	}
	return pluginImpl.Authenticate(ctx, in)
}

// AuthnPluginRPC is the hplugin.Plugin implementation for go-plugin.
type AuthnPluginRPC struct{ hplugin.Plugin }

func (p *AuthnPluginRPC) Server(*hplugin.MuxBroker) (interface{}, error) {
	return nil, nil // Not used in client
}
func (p *AuthnPluginRPC) Client(b *hplugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return &AuthnRPC{client: c}, nil
}

// AuthnRPC implements AuthenticationPlugin over RPC.
type AuthnRPC struct {
	client *rpc.Client
}

func (a *AuthnRPC) Authenticate(ctx context.Context, in *plugin.AuthenticationInput) (*plugin.AuthenticationOutput, error) {
	var out plugin.AuthenticationOutput
	err := a.client.Call("Plugin.Authenticate", in, &out)
	return &out, err
}

// Plugin handshake config for go-plugin
var Handshake = hplugin.HandshakeConfig{
	ProtocolVersion:  1,
	MagicCookieKey:   "SYFON_AUTHZ_PLUGIN",
	MagicCookieValue: "syfon_authz_plugin_v1",
}

// PluginClient is the concrete implementation for plugin communication.
type PluginClient struct {
	client *hplugin.Client
	raw    interface{}
	mu     sync.Mutex
}

// PluginManager manages the plugin process and calls Authorize.
type PluginManager struct {
	client *PluginClient
}

// NewPluginManager loads the plugin binary and returns a manager.
func NewPluginManager(pluginPath string) (*PluginManager, error) {
	client := hplugin.NewClient(&hplugin.ClientConfig{
		HandshakeConfig: Handshake,
		Plugins: map[string]hplugin.Plugin{
			"authz": &AuthzPluginRPC{},
		},
		Cmd:              exec.Command(pluginPath),
		AllowedProtocols: []hplugin.Protocol{hplugin.ProtocolNetRPC},
	})

	rpcClient, err := client.Client()
	if err != nil {
		return nil, err
	}

	raw, err := rpcClient.Dispense("authz")
	if err != nil {
		return nil, err
	}

	return &PluginManager{client: &PluginClient{client: client, raw: raw}}, nil
}

// Authorize delegates to the plugin.
func (pm *PluginManager) Authorize(ctx context.Context, in *plugin.AuthorizationInput) (*plugin.AuthorizationOutput, error) {
	pm.client.mu.Lock()
	defer pm.client.mu.Unlock()
	pluginImpl, ok := pm.client.raw.(plugin.AuthorizationPlugin)
	if !ok {
		return nil, os.ErrInvalid
	}
	return pluginImpl.Authorize(ctx, in)
}

// AuthzPluginRPC is the hplugin.Plugin implementation for go-plugin.
type AuthzPluginRPC struct{ hplugin.Plugin }

func (p *AuthzPluginRPC) Server(*hplugin.MuxBroker) (interface{}, error) {
	return nil, nil // Not used in client
}
func (p *AuthzPluginRPC) Client(b *hplugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return &AuthzRPC{client: c}, nil
}

// AuthzRPC implements AuthorizationPlugin over RPC.
type AuthzRPC struct {
	client *rpc.Client
}

func (a *AuthzRPC) Authorize(ctx context.Context, in *plugin.AuthorizationInput) (*plugin.AuthorizationOutput, error) {
	var out plugin.AuthorizationOutput
	err := a.client.Call("Plugin.Authorize", in, &out)
	return &out, err
}

// DummyPluginManager implements the same interface as PluginManager for testing.
type DummyPluginManager struct{}

func (d *DummyPluginManager) Authorize(ctx context.Context, in *plugin.AuthorizationInput) (*plugin.AuthorizationOutput, error) {
	return &plugin.AuthorizationOutput{Allow: true}, nil
}

// Ensure PluginManager implements pluginManagerInterface
var _ pluginManagerInterface = (*PluginManager)(nil)
