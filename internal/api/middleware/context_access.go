package middleware

import (
	"context"

	"github.com/calypr/syfon/internal/authz"
)

func MissingGen3AuthHeader(ctx context.Context) bool {
	return authz.IsGen3Mode(ctx) && !authz.HasAuthHeader(ctx)
}

func AuthFailureStatus(ctx context.Context) int {
	return authz.AuthStatusCode(ctx)
}
