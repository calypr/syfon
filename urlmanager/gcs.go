package urlmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/calypr/syfon/config"
	"github.com/calypr/syfon/db/core"
	"github.com/calypr/syfon/internal/provider"
	"github.com/google/uuid"
	"gocloud.dev/blob"
	"google.golang.org/api/option"
)

type gcsMultipartBackend struct {
	m    *Manager
	item *cacheItem
}

func (b *gcsMultipartBackend) Init(ctx context.Context, bucketName string, key string) (string, error) {
	return uuid.NewString(), nil
}

func (b *gcsMultipartBackend) SignPart(ctx context.Context, bucketName string, key string, uploadID string, partNumber int32) (string, error) {
	cred, err := b.m.credentialForBucket(ctx, bucketName)
	if err != nil {
		return "", err
	}
	return gcsSignedUploadPartURL(bucketName, multipartPartObjectKey(key, uploadID, partNumber), cred, b.m.signing)
}

func (b *gcsMultipartBackend) Complete(ctx context.Context, bucketName string, key string, uploadID string, parts []MultipartPart) error {
	if b.item.GCSClient == nil {
		return fmt.Errorf("gcs multipart backend unavailable for bucket %s: missing gcs client", bucketName)
	}
	partList := normalizedMultipartParts(parts)
	partKeys := make([]string, 0, len(partList))
	for _, p := range partList {
		partKeys = append(partKeys, multipartPartObjectKey(key, uploadID, p.PartNumber))
	}
	tempKeys, err := composeGCSObjects(ctx, b.item.GCSClient, bucketName, strings.Trim(strings.TrimSpace(key), "/"), uploadID, partKeys)
	if err != nil {
		return err
	}
	for _, k := range append(partKeys, tempKeys...) {
		_ = b.item.GCSClient.Bucket(bucketName).Object(k).Delete(ctx)
	}
	return nil
}

func composeGCSObjects(ctx context.Context, client *storage.Client, bucket string, destinationKey string, uploadID string, partKeys []string) ([]string, error) {
	if len(partKeys) == 0 {
		return nil, fmt.Errorf("multipart complete requires at least one part")
	}
	current := append([]string(nil), partKeys...)
	tempKeys := []string{}
	round := 0
	for len(current) > 32 {
		next := []string{}
		for i := 0; i < len(current); i += 32 {
			end := i + 32
			if end > len(current) {
				end = len(current)
			}
			tmp := path.Join(".syfon-multipart", strings.TrimSpace(uploadID), strings.Trim(strings.TrimSpace(destinationKey), "/"), "compose", fmt.Sprintf("%d-%d", round, i/32))
			if err := composeGCSBatch(ctx, client, bucket, tmp, current[i:end]); err != nil {
				return tempKeys, err
			}
			tempKeys = append(tempKeys, tmp)
			next = append(next, tmp)
		}
		current = next
		round++
	}
	if err := composeGCSBatch(ctx, client, bucket, destinationKey, current); err != nil {
		return tempKeys, err
	}
	return tempKeys, nil
}

func composeGCSBatch(ctx context.Context, client *storage.Client, bucket string, dst string, src []string) error {
	dstObj := client.Bucket(bucket).Object(dst)
	srcObjs := make([]*storage.ObjectHandle, 0, len(src))
	for _, k := range src {
		srcObjs = append(srcObjs, client.Bucket(bucket).Object(k))
	}
	if _, err := dstObj.ComposerFrom(srcObjs...).Run(ctx); err != nil {
		return fmt.Errorf("failed gcs compose for %s: %w", dst, err)
	}
	return nil
}

type gcsServiceAccountKey struct {
	ClientEmail string `json:"client_email"`
	PrivateKey  string `json:"private_key"`
}

func gcsSignedURL(bucket string, key string, method string, expiry time.Duration, rangeStr string, cred *core.S3Credential, signing config.SigningConfig) (string, error) {
	googleAccessID := gcsGoogleAccessID(cred)
	privateKey := gcsPrivateKey(cred)
	if googleAccessID == "" || privateKey == "" {
		return "", fmt.Errorf("gcs signing requires service account credentials (access_key=client_email, secret_key=private_key or JSON key)")
	}
	opts := &storage.SignedURLOptions{
		GoogleAccessID: googleAccessID,
		PrivateKey:     []byte(privateKey),
		Method:         method,
		Expires:        time.Now().Add(expiry),
		Scheme:         storage.SigningSchemeV4,
	}
	if rangeStr != "" {
		opts.Headers = append(opts.Headers, "Range:"+rangeStr)
	}
	return storage.SignedURL(bucket, key, opts)
}

func gcsSignedUploadPartURL(bucket string, partKey string, cred *core.S3Credential, signing config.SigningConfig) (string, error) {
	googleAccessID := gcsGoogleAccessID(cred)
	privateKey := gcsPrivateKey(cred)
	if googleAccessID == "" || privateKey == "" {
		return "", fmt.Errorf("gcs multipart signing requires service account credentials (access_key=client_email, secret_key=private_key or JSON key)")
	}
	expiry := time.Duration(signing.DefaultExpirySeconds) * time.Second
	if expiry <= 0 {
		expiry = 15 * time.Minute
	}
	return gcsSignedURL(bucket, partKey, http.MethodPut, expiry, "", cred, signing)
}

func gcsGoogleAccessID(cred *core.S3Credential) string {
	googleAccessID := strings.TrimSpace(cred.AccessKey)
	privateKey := strings.TrimSpace(cred.SecretKey)
	var keyJSON gcsServiceAccountKey
	if json.Unmarshal([]byte(privateKey), &keyJSON) == nil && strings.TrimSpace(keyJSON.ClientEmail) != "" {
		return strings.TrimSpace(keyJSON.ClientEmail)
	}
	return googleAccessID
}

func gcsPrivateKey(cred *core.S3Credential) string {
	privateKey := strings.TrimSpace(cred.SecretKey)
	var keyJSON gcsServiceAccountKey
	if json.Unmarshal([]byte(privateKey), &keyJSON) == nil && strings.TrimSpace(keyJSON.PrivateKey) != "" {
		return strings.TrimSpace(keyJSON.PrivateKey)
	}
	return privateKey
}

func (m *Manager) openGCSBucket(ctx context.Context, bucketName string) (*cacheItem, error) {
	bucket, err := blob.OpenBucket(ctx, config.GCSPrefix+bucketName)
	if err != nil {
		return nil, err
	}
	item := &cacheItem{
		Bucket:     bucket,
		Provider:   provider.GCS,
		BucketName: bucketName,
	}
	cred, err := m.credentialForBucket(ctx, bucketName)
	if err != nil || cred == nil {
		return item, nil
	}
	client, err := gcsClientFromCredential(ctx, cred)
	if err == nil {
		item.GCSClient = client
	}
	return item, nil
}

func gcsClientFromCredential(ctx context.Context, cred *core.S3Credential) (*storage.Client, error) {
	secret := strings.TrimSpace(cred.SecretKey)
	if secret != "" {
		if json.Valid([]byte(secret)) {
			return storage.NewClient(ctx, option.WithCredentialsJSON([]byte(secret)))
		}
	}
	return storage.NewClient(ctx)
}
