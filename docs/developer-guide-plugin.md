# Syfon Developer Guide: Authoring Authorization Plugins

This guide explains how to implement a custom authorization plugin for Syfon using the HashiCorp go-plugin system.

## Plugin Contract

Your plugin must implement the following Go interface:

```go
type AuthorizationPlugin interface {
    Authorize(ctx context.Context, in *AuthorizationInput) (*AuthorizationOutput, error)
}

// Input struct
 type AuthorizationInput struct {
    RequestID string
    Subject   string
    Action    string
    Resource  string
    Claims    map[string]interface{}
    Metadata  map[string]interface{}
}

// Output struct
 type AuthorizationOutput struct {
    Allow       bool
    Reason      string
    Obligations map[string]interface{}
}
```

- The plugin receives all relevant request context, JWT claims, and metadata.
- It must return `Allow=true` to permit, or `Allow=false` to deny, with an optional reason and obligations.

## Handshake and Communication
- Use `github.com/hashicorp/go-plugin` for plugin RPC and handshake.
- The handshake config must match Syfon’s expectations (see `plugin.go`).

## Example Plugin Skeleton

```go
package main

import (
    "context"
    "github.com/hashicorp/go-plugin"
    "syfon/internal/api/middleware" // adjust import path as needed
)

type MyAuthzPlugin struct{}

func (p *MyAuthzPlugin) Authorize(ctx context.Context, in *middleware.AuthorizationInput) (*middleware.AuthorizationOutput, error) {
    // Implement your policy logic here
    allow := (in.Subject == "admin")
    return &middleware.AuthorizationOutput{
        Allow: allow,
        Reason: "example policy",
        Obligations: map[string]interface{}{
            "resources": []interface{}{in.Resource},
            "privileges": map[string]interface{}{in.Resource: []interface{}{in.Action}},
        },
    }, nil
}

func main() {
    plugin.Serve(&plugin.ServeConfig{
        HandshakeConfig: middleware.PluginHandshake,
        Plugins: map[string]plugin.Plugin{
            "authz": &middleware.AuthzPluginImpl{Impl: &MyAuthzPlugin{}},
        },
        GRPCServer: plugin.DefaultGRPCServer,
    })
}
```

## Testing Your Plugin
- Write unit tests for your plugin logic.
- You can run your plugin standalone for debugging, but it must be launched by Syfon in production.

## Versioning and Compatibility
- Maintain compatibility with the Syfon plugin contract.
- If the contract changes, update both Syfon and your plugin accordingly.

## Resources
- [HashiCorp go-plugin documentation](https://github.com/hashicorp/go-plugin)
- See Syfon’s `internal/api/middleware/plugin.go` for the full contract and handshake details.

