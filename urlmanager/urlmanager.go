package urlmanager

import "context"

// UrlManager is responsible for signing URLs for resource access.
type UrlManager interface {
	// SignURL signs a URL for the given resource.
	// accessId is used to identify credentials or bucket configuration.
	// url is the object location (e.g. s3://bucket/key).
	SignURL(ctx context.Context, accessId string, url string) (string, error)
}
