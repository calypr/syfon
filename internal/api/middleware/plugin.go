package middleware

import (
	"context"
	"net/rpc"
	"os"
	"os/exec"
	"sync"

	"github.com/hashicorp/go-plugin"
)

// AuthorizationInput is the request sent to the plugin for an authz decision.
type AuthorizationInput struct {
	RequestID string
	Subject   string
	Action    string
	Resource  string
	Claims    map[string]interface{}
	Metadata  map[string]interface{}
}

// AuthorizationOutput is the plugin's response.
type AuthorizationOutput struct {
	Allow      bool
	Reason     string
	Obligations map[string]interface{}
}

// AuthorizationPlugin is the interface plugins must implement.
type AuthorizationPlugin interface {
	Authorize(ctx context.Context, in *AuthorizationInput) (*AuthorizationOutput, error)
}

// Plugin handshake config for go-plugin
var Handshake = plugin.HandshakeConfig{
	ProtocolVersion:  1,
	MagicCookieKey:   "SYFON_AUTHZ_PLUGIN",
	MagicCookieValue: "syfon_authz_plugin_v1",
}

// PluginClient is the concrete implementation for plugin communication.
type PluginClient struct {
	client *plugin.Client
	raw    interface{}
	mu     sync.Mutex
}

// PluginManager manages the plugin process and calls Authorize.
type PluginManager struct {
	client *PluginClient
}

// NewPluginManager loads the plugin binary and returns a manager.
func NewPluginManager(pluginPath string) (*PluginManager, error) {
	client := plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig: Handshake,
		Plugins: map[string]plugin.Plugin{
			"authz": &AuthzPluginRPC{},
		},
		Cmd:              exec.Command(pluginPath),
		AllowedProtocols: []plugin.Protocol{plugin.ProtocolNetRPC},
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
func (pm *PluginManager) Authorize(ctx context.Context, in *AuthorizationInput) (*AuthorizationOutput, error) {
	pm.client.mu.Lock()
	defer pm.client.mu.Unlock()
	pluginImpl, ok := pm.client.raw.(AuthorizationPlugin)
	if !ok {
		return nil, os.ErrInvalid
	}
	return pluginImpl.Authorize(ctx, in)
}

// AuthzPluginRPC is the plugin.Plugin implementation for go-plugin.
type AuthzPluginRPC struct{ plugin.Plugin }

func (p *AuthzPluginRPC) Server(*plugin.MuxBroker) (interface{}, error) {
	return nil, nil // Not used in client
}
func (p *AuthzPluginRPC) Client(b *plugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return &AuthzRPC{client: c}, nil
}

// AuthzRPC implements AuthorizationPlugin over RPC.
type AuthzRPC struct {
	client *rpc.Client
}

func (a *AuthzRPC) Authorize(ctx context.Context, in *AuthorizationInput) (*AuthorizationOutput, error) {
	var out AuthorizationOutput
	err := a.client.Call("Plugin.Authorize", in, &out)
	return &out, err
}

// DummyPluginManager implements the same interface as PluginManager for testing.
type DummyPluginManager struct{}

func (d *DummyPluginManager) Authorize(ctx context.Context, in *AuthorizationInput) (*AuthorizationOutput, error) {
	return &AuthorizationOutput{Allow: true}, nil
}

// Ensure PluginManager implements pluginManagerInterface
var _ pluginManagerInterface = (*PluginManager)(nil)
