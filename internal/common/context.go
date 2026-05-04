package common

type AuthzContextKey string

const (
	// BucketControlResource is the resource path for internal bucket management.
	BucketControlResource = "/services/internal/buckets"
	// MetricsIngestResource is the resource path for trusted provider metrics ingestion.
	MetricsIngestResource = "/services/internal/metrics"
)
