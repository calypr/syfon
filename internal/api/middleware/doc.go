// Package middleware contains the request-level auth pipeline used by the API
// packages.
//
// The package is organized by responsibility:
//   - middleware.go: top-level AuthzMiddleware construction and request orchestration
//   - local_mode.go and gen3_mode.go: mode-specific request flow
//   - authn_*.go: built-in authentication providers and local CSV loading
//   - plugin_clients.go: external authn/authz plugin process wiring
//   - token.go and jwks.go: JWT parsing and JWKS key discovery/cache
//   - config.go and auth_cache.go: env-driven config and Fence authz cache
//   - context_access.go: small helpers used by API packages when mapping auth failures
package middleware
