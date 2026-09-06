package metrics

import (
	"context"

	"github.com/calypr/syfon/internal/access"
	"github.com/calypr/syfon/internal/testutils"
	"github.com/calypr/syfon/internal/usage"
	"github.com/gofiber/fiber/v3"
)

func metricsTestContext(base context.Context, mode string, headerSet bool, headerValue bool, privileges map[string]map[string]bool) context.Context {
	session := access.NewSession(mode)
	if headerSet {
		session.AuthHeaderPresent = headerValue
	}
	session.AuthzEnforced = mode == "gen3" || mode == "local"
	session.SetAuthorizations(nil, privileges, session.AuthzEnforced)
	return access.WithSession(base, session)
}

func registerMetricsRoutesForTest(app *fiber.App, database *testutils.MockDatabase) {
	service := usage.NewService(usage.Dependencies{
		Ingest:  database,
		Reports: database,
		Objects: metricsObjectReaderAdapter{db: database},
	})
	RegisterMetricsRoutes(app, service.Reports(), service.Ingest())
}
