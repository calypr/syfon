package services

// SyfonClient defines the top-level interface for interacting with Syfon services.
type SyfonClient interface {
	Health() *HealthService
	Data() *DataService
	Index() *IndexService
	DRS() *DRSService
	Buckets() *BucketsService
	Metrics() *MetricsService
	LFS() *LFSService
}
