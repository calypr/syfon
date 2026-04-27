package bucket

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	bucketapi "github.com/calypr/syfon/apigen/client/bucketapi"
	"github.com/calypr/syfon/client/transfer"
	s3driver "github.com/calypr/syfon/client/xfer/providers/s3"
)

// ValidateBucket verifies that the bucket exists and is accessible using native provider SDKs.
func ValidateBucket(ctx context.Context, req bucketapi.PutBucketRequest) error {
	provider := "s3"
	if req.Provider != nil {
		provider = strings.ToLower(*req.Provider)
	}

	switch provider {
	case "s3":
		if err := validateBillingLogConfig(req); err != nil {
			return err
		}
		return validateS3(ctx, req)
	case "gcs", "gs":
		return validateBillingLogConfig(req)
	case "azure":
		return validateBillingLogConfig(req)
	default:
		return nil
	}
}

func validateBillingLogConfig(req bucketapi.PutBucketRequest) error {
	provider := "s3"
	if req.Provider != nil {
		provider = strings.ToLower(strings.TrimSpace(*req.Provider))
	}
	if provider == "file" {
		return nil
	}
	if req.BillingLogBucket == nil || strings.TrimSpace(*req.BillingLogBucket) == "" {
		return fmt.Errorf("billing_log_bucket is required for provider=%s", provider)
	}
	if req.BillingLogPrefix == nil || strings.TrimSpace(*req.BillingLogPrefix) == "" {
		return fmt.Errorf("billing_log_prefix is required for provider=%s", provider)
	}
	return nil
}

func validateS3(ctx context.Context, req bucketapi.PutBucketRequest) error {
	var opts []func(*config.LoadOptions) error
	if req.Region != nil {
		opts = append(opts, config.WithRegion(*req.Region))
	}
	if req.AccessKey != nil && req.SecretKey != nil {
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(*req.AccessKey, *req.SecretKey, "")))
	}

	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return fmt.Errorf("failed to load s3 config: %w", err)
	}

	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if req.Endpoint != nil {
			o.BaseEndpoint = aws.String(*req.Endpoint)
		}
	})

	// Wrap in our new driver to use its Validate logic
	driver := s3driver.NewBackend(transfer.NoOpLogger{}, s3Client, req.Bucket)
	return driver.Validate(ctx, req.Bucket)
}
