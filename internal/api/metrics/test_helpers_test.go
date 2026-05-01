package metrics

import (
	"context"

	internalauth "github.com/calypr/syfon/internal/auth"
)

func metricsTestContext(base context.Context, mode string, headerSet bool, headerValue bool, privileges map[string]map[string]bool) context.Context {
	session := internalauth.NewSession(mode)
	if headerSet {
		session.AuthHeaderPresent = headerValue
	}
	session.AuthzEnforced = mode == "gen3" || mode == "local"
	session.SetAuthorizations(nil, privileges, session.AuthzEnforced)
	return internalauth.WithSession(base, session)
}
