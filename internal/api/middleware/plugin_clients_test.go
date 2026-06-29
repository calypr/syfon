package middleware

import (
	"context"

	"github.com/calypr/syfon/plugin"
)

// DummyPluginManager implements the same interface as PluginManager for testing.
type DummyPluginManager struct{}

func (d *DummyPluginManager) Authorize(ctx context.Context, in *plugin.AuthorizationInput) (*plugin.AuthorizationOutput, error) {
	return &plugin.AuthorizationOutput{Allow: true}, nil
}
