# Syfon Authentication Plugin Developer Guide

## Interface
Implement the following interface:

```go
type AuthenticationPlugin interface {
	Authenticate(ctx context.Context, in *AuthenticationInput) (*AuthenticationOutput, error)
}
```

- Input: `AuthenticationInput` contains the request ID, raw Authorization header, and request metadata.
- Output: `AuthenticationOutput` must set `Authenticated` true/false, and may set `Subject`, `Claims`, and `Reason`.

## Registration
Register your plugin with go-plugin under the key `"authn"`.

## Example Skeleton
```go
package main

import (
	"context"
	"github.com/hashicorp/go-plugin"
	"github.com/calypr/syfon/internal/api/middleware"
)

type MyAuthnPlugin struct{}

func (p *MyAuthnPlugin) Authenticate(ctx context.Context, in *middleware.AuthenticationInput) (*middleware.AuthenticationOutput, error) {
	// Your logic here
	return &middleware.AuthenticationOutput{Authenticated: true, Subject: "user"}, nil
}

func main() {
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: middleware.Handshake,
		Plugins: map[string]plugin.Plugin{
			"authn": &middleware.AuthnPluginRPC{},
		},
	})
}
```

## Testing
- Use Syfon's test suite or inject a dummy plugin manager for rapid iteration.
- Ensure your plugin binary is executable and compatible with Syfon's interface.

