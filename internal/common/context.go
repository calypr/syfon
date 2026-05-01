package common

type AuthzContextKey string

const (
	// UserAuthzKey is the context key for the user's authorized resources list
	UserAuthzKey AuthzContextKey = "user_authz"
	// UserPrivilegesKey stores method-aware privileges (resource -> method -> allowed).
	UserPrivilegesKey AuthzContextKey = "user_privileges"
	// AuthHeaderPresentKey indicates whether the incoming request had an Authorization header.
	AuthHeaderPresentKey AuthzContextKey = "auth_header_present"
	// AuthModeKey contains the configured server mode: local or gen3.
	AuthModeKey AuthzContextKey = "auth_mode"
	// AuthzEnforcedKey marks requests that should use method-aware authorization checks.
	AuthzEnforcedKey AuthzContextKey = "authz_enforced"

	// BucketControlResource is the resource path for internal bucket management.
	BucketControlResource = "/services/internal/buckets"
	// MetricsIngestResource is the resource path for trusted provider metrics ingestion.
	MetricsIngestResource = "/services/internal/metrics"

	// SubjectKey is the context key for the authenticated subject (user/principal)
	SubjectKey AuthzContextKey = "subject"
	// ClaimsKey is the context key for the authenticated claims (map[string]interface{})
	ClaimsKey AuthzContextKey = "claims"
)
