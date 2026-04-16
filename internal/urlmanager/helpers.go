package urlmanager

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/calypr/syfon/internal/config"
	"github.com/calypr/syfon/internal/db/core"
	"github.com/google/uuid"
	"gocloud.dev/blob"
)

type cacheItem struct {
	AzureSharedKey  *azblob.SharedKeyCredential
	AzureServiceURL string
	GCSClient       *storage.Client
}

type azureMultipartBackend struct {
	m    *Manager
	item *cacheItem
}

func (b *azureMultipartBackend) Init(ctx context.Context, bucketName string, key string) (string, error) {
	return uuid.NewString(), nil
}

func (b *azureMultipartBackend) SignPart(ctx context.Context, bucketName string, key string, uploadID string, partNumber int32) (string, error) {
	if b.item == nil || b.item.AzureSharedKey == nil || strings.TrimSpace(b.item.AzureServiceURL) == "" {
		return "", fmt.Errorf("azure multipart requires shared key credentials and endpoint for bucket %s", bucketName)
	}
	expiry := time.Duration(b.m.signing.DefaultExpirySeconds) * time.Second
	if expiry <= 0 {
		expiry = 15 * time.Minute
	}
	signed, err := azureSignedURL(b.item.AzureServiceURL, bucketName, key, http.MethodPut, expiry, "", b.item.AzureSharedKey)
	if err != nil {
		return "", err
	}
	u, err := url.Parse(signed)
	if err != nil {
		return "", err
	}
	query := u.Query()
	query.Set("comp", "block")
	query.Set("blockid", azureBlockID(uploadID, partNumber))
	u.RawQuery = query.Encode()
	return u.String(), nil
}

func (b *azureMultipartBackend) Complete(ctx context.Context, bucketName string, key string, uploadID string, parts []MultipartPart) error {
	if b.item == nil || b.item.AzureSharedKey == nil || strings.TrimSpace(b.item.AzureServiceURL) == "" {
		return fmt.Errorf("azure multipart requires shared key credentials and endpoint for bucket %s", bucketName)
	}
	blobURL := azureBlobURL(b.item.AzureServiceURL, bucketName, key)
	client, err := blockblob.NewClientWithSharedKeyCredential(blobURL, b.item.AzureSharedKey, nil)
	if err != nil {
		return fmt.Errorf("failed to create azure block blob client: %w", err)
	}
	partList := normalizedMultipartParts(parts)
	blockIDs := make([]string, 0, len(partList))
	for _, p := range partList {
		blockIDs = append(blockIDs, azureBlockID(uploadID, p.PartNumber))
	}
	if _, err := client.CommitBlockList(ctx, blockIDs, nil); err != nil {
		return fmt.Errorf("failed to complete azure multipart upload: %w", err)
	}
	return nil
}

func azureSignedURL(serviceURL string, bucketName string, key string, method string, expiry time.Duration, rangeStr string, sharedKey *azblob.SharedKeyCredential) (string, error) {
	blobURL := azureBlobURL(serviceURL, bucketName, key)
	now := time.Now().UTC()
	perm := (&sas.BlobPermissions{Read: true}).String()
	if method == http.MethodPut {
		perm = (&sas.BlobPermissions{Add: true, Create: true, Write: true}).String()
	}

	qp, err := sas.BlobSignatureValues{
		Protocol:      azureSASProtocol(serviceURL),
		StartTime:     now.Add(-5 * time.Minute),
		ExpiryTime:    now.Add(expiry),
		Permissions:   perm,
		ContainerName: bucketName,
		BlobName:      strings.Trim(strings.TrimSpace(key), "/"),
		Version:       "2021-08-06",
	}.SignWithSharedKey(sharedKey)
	if err != nil {
		return "", err
	}

	return blobURL + "?" + qp.Encode(), nil
}

func azureSASProtocol(serviceURL string) sas.Protocol {
	u, err := url.Parse(strings.TrimSpace(serviceURL))
	if err == nil && strings.EqualFold(strings.TrimSpace(u.Scheme), "http") {
		return sas.ProtocolHTTPSandHTTP
	}
	return sas.ProtocolHTTPS
}

func azureBlockID(uploadID string, partNumber int32) string {
	raw := fmt.Sprintf("%s:%08d", strings.TrimSpace(uploadID), partNumber)
	return base64.StdEncoding.EncodeToString([]byte(raw))
}

func azureBlobURL(serviceURL string, bucket string, key string) string {
	base := strings.TrimRight(strings.TrimSpace(serviceURL), "/")
	cleanKey := strings.Trim(strings.TrimSpace(key), "/")
	if cleanKey == "" {
		return base + "/" + url.PathEscape(bucket)
	}
	segments := strings.Split(cleanKey, "/")
	escaped := make([]string, 0, len(segments))
	for _, s := range segments {
		if s == "" {
			continue
		}
		escaped = append(escaped, url.PathEscape(s))
	}
	return base + "/" + url.PathEscape(bucket) + "/" + strings.Join(escaped, "/")
}

func azureServiceURL(accountName string, endpoint string) string {
	ep := strings.TrimSpace(endpoint)
	if ep != "" {
		if !strings.HasPrefix(ep, "http://") && !strings.HasPrefix(ep, "https://") {
			ep = "https://" + ep
		}
		return strings.TrimRight(ep, "/")
	}
	return "https://" + strings.TrimSpace(accountName) + ".blob.core.windows.net"
}

func azureAccountFromEndpoint(endpoint string) string {
	ep := strings.TrimSpace(endpoint)
	if ep == "" {
		return ""
	}
	if !strings.HasPrefix(ep, "http://") && !strings.HasPrefix(ep, "https://") {
		ep = "https://" + ep
	}
	u, err := url.Parse(ep)
	if err != nil {
		return ""
	}
	host := strings.TrimSpace(u.Hostname())
	if host == "" {
		return ""
	}
	parts := strings.Split(host, ".")
	if len(parts) == 0 {
		return ""
	}
	return parts[0]
}

func (b *azureMultipartBackend) openAzureBucket(ctx context.Context, bucketName string) (*cacheItem, error) {
	cred, err := b.m.credentialForBucket(ctx, bucketName)
	if err != nil || cred == nil {
		return &cacheItem{AzureServiceURL: config.AzurePrefix + bucketName}, nil
	}
	accountName := strings.TrimSpace(cred.AccessKey)
	if accountName == "" {
		accountName = azureAccountFromEndpoint(cred.Endpoint)
	}
	accountKey := strings.TrimSpace(cred.SecretKey)
	serviceURL := azureServiceURL(accountName, cred.Endpoint)
	item := &cacheItem{AzureServiceURL: serviceURL}
	if accountName != "" && accountKey != "" {
		shared, err := azblob.NewSharedKeyCredential(accountName, accountKey)
		if err == nil {
			item.AzureSharedKey = shared
		}
	}
	return item, nil
}

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
	if b.item == nil || b.item.GCSClient == nil {
		return fmt.Errorf("gcs multipart backend unavailable for bucket %s: missing gcs client", bucketName)
	}
	partList := normalizedMultipartParts(parts)
	partKeys := make([]string, 0, len(partList))
	for _, p := range partList {
		partKeys = append(partKeys, multipartPartObjectKey(key, uploadID, p.PartNumber))
	}
	_, err := composeGCSObjects(ctx, b.item.GCSClient, bucketName, strings.Trim(strings.TrimSpace(key), "/"), uploadID, partKeys)
	return err
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
	if endpointURL, ok := gcsEndpointObjectURL(cred, bucket, key, method); ok {
		return endpointURL, nil
	}

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

func gcsEndpointObjectURL(cred *core.S3Credential, bucket string, key string, method string) (string, bool) {
	if cred == nil {
		return "", false
	}
	endpoint := strings.TrimSpace(cred.Endpoint)
	if endpoint == "" {
		return "", false
	}
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		endpoint = "http://" + endpoint
	}
	base, err := url.Parse(endpoint)
	if err != nil || strings.TrimSpace(base.Host) == "" {
		return "", false
	}
	bucketEscaped := url.PathEscape(strings.TrimSpace(bucket))
	cleanKey := strings.Trim(strings.TrimSpace(key), "/")
	keyEscaped := url.PathEscape(cleanKey)
	prefix := strings.TrimRight(strings.TrimSpace(base.Path), "/")
	base.RawQuery = ""
	base.Fragment = ""

	switch strings.ToUpper(strings.TrimSpace(method)) {
	case http.MethodPut:
		builtPath := strings.Join([]string{prefix, "upload", "storage", "v1", "b", bucketEscaped, "o"}, "/")
		builtPath = strings.ReplaceAll(builtPath, "//", "/")
		if !strings.HasPrefix(builtPath, "/") {
			builtPath = "/" + builtPath
		}
		if len(builtPath) > 1 && (builtPath[1] == '/' || builtPath[1] == '\\') {
			return "", false
		}
		base.Path = builtPath
		q := base.Query()
		q.Set("uploadType", "media")
		q.Set("name", cleanKey)
		base.RawQuery = q.Encode()
		return base.String(), true
	default:
		builtPath := strings.Join([]string{prefix, "storage", "v1", "b", bucketEscaped, "o", keyEscaped}, "/")
		builtPath = strings.ReplaceAll(builtPath, "//", "/")
		if !strings.HasPrefix(builtPath, "/") {
			builtPath = "/" + builtPath
		}
		if len(builtPath) > 1 && (builtPath[1] == '/' || builtPath[1] == '\\') {
			return "", false
		}
		base.Path = builtPath
		q := base.Query()
		q.Set("alt", "media")
		base.RawQuery = q.Encode()
		return base.String(), true
	}
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

func multipartPartObjectKey(key string, uploadID string, partNumber int32) string {
	return path.Join(".syfon-multipart", strings.TrimSpace(uploadID), strings.Trim(strings.TrimSpace(key), "/"), fmt.Sprintf("part-%08d", partNumber))
}

func normalizedMultipartParts(parts []MultipartPart) []MultipartPart {
	out := append([]MultipartPart(nil), parts...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].PartNumber == out[j].PartNumber {
			return out[i].ETag < out[j].ETag
		}
		return out[i].PartNumber < out[j].PartNumber
	})
	return out
}

func completeMultipartByStitching(ctx context.Context, bucket *blob.Bucket, key string, uploadID string, parts []MultipartPart) error {
	partList := normalizedMultipartParts(parts)
	var data bytes.Buffer
	for _, part := range partList {
		partKey := multipartPartObjectKey(key, uploadID, part.PartNumber)
		r, err := bucket.NewReader(ctx, partKey, nil)
		if err != nil {
			return fmt.Errorf("open part %d: %w", part.PartNumber, err)
		}
		if _, err := io.Copy(&data, r); err != nil {
			_ = r.Close()
			return fmt.Errorf("read part %d: %w", part.PartNumber, err)
		}
		if err := r.Close(); err != nil {
			return fmt.Errorf("close part %d: %w", part.PartNumber, err)
		}
	}

	w, err := bucket.NewWriter(ctx, key, nil)
	if err != nil {
		return fmt.Errorf("open destination writer: %w", err)
	}
	if _, err := io.Copy(w, &data); err != nil {
		_ = w.Close()
		return fmt.Errorf("write stitched object: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("close destination writer: %w", err)
	}

	for _, part := range partList {
		partKey := multipartPartObjectKey(key, uploadID, part.PartNumber)
		_ = bucket.Delete(ctx, partKey)
	}
	return nil
}

func isSigningNotSupported(err error) bool {
	if err == nil {
		return false
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "unimplemented") || strings.Contains(lower, "not supported")
}
