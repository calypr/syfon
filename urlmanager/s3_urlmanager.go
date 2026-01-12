package urlmanager

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3UrlManager implements UrlManager for AWS S3.
type S3UrlManager struct {
	client        *s3.Client
	presignClient *s3.PresignClient
}

// NewS3UrlManager creating a new S3UrlManager.
// It loads AWS credentials from the environment.
func NewS3UrlManager(ctx context.Context) (*S3UrlManager, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to load SDK config: %w", err)
	}

	client := s3.NewFromConfig(cfg)
	presignClient := s3.NewPresignClient(client)

	return &S3UrlManager{
		client:        client,
		presignClient: presignClient,
	}, nil
}

// SignURL signs a URL for the given resource.
// It expects a URL in the format s3://bucket/key.
func (m *S3UrlManager) SignURL(ctx context.Context, accessId string, urlStr string) (string, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return "", fmt.Errorf("failed to parse url: %w", err)
	}

	if u.Scheme != "s3" {
		return "", fmt.Errorf("unsupported scheme: %s", u.Scheme)
	}

	bucket := u.Host
	key := strings.TrimPrefix(u.Path, "/")

	if bucket == "" || key == "" {
		return "", fmt.Errorf("invalid s3 url: %s", urlStr)
	}

	// Sign the request
	req, err := m.presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	}, s3.WithPresignExpires(15*time.Minute))
	if err != nil {
		return "", fmt.Errorf("failed to sign url: %w", err)
	}

	return req.URL, nil
}
