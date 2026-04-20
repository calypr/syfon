package urlmanager

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/calypr/syfon/config"
	"github.com/calypr/syfon/internal/provider"
	"github.com/google/uuid"
	"gocloud.dev/blob"
	"gocloud.dev/blob/azureblob"
)

type azureMultipartBackend struct {
	m    *Manager
	item *cacheItem
}

func (b *azureMultipartBackend) Init(ctx context.Context, bucketName string, key string) (string, error) {
	return uuid.NewString(), nil
}

func (b *azureMultipartBackend) SignPart(ctx context.Context, bucketName string, key string, uploadID string, partNumber int32) (string, error) {
	if b.item.AzureSharedKey == nil || strings.TrimSpace(b.item.AzureServiceURL) == "" {
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
	if b.item.AzureSharedKey == nil || strings.TrimSpace(b.item.AzureServiceURL) == "" {
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

func (m *Manager) openAzureBucket(ctx context.Context, bucketName string) (*cacheItem, error) {
	item := &cacheItem{Provider: provider.Azure, BucketName: bucketName}
	cred, err := m.credentialForBucket(ctx, bucketName)
	if err != nil || cred == nil {
		bucket, openErr := blob.OpenBucket(ctx, config.AzurePrefix+bucketName)
		if openErr != nil {
			return nil, openErr
		}
		item.Bucket = bucket
		return item, nil
	}
	accountName := strings.TrimSpace(cred.AccessKey)
	if accountName == "" {
		accountName = azureAccountFromEndpoint(cred.Endpoint)
	}
	accountKey := strings.TrimSpace(cred.SecretKey)
	serviceURL := azureServiceURL(accountName, cred.Endpoint)
	if accountName != "" && accountKey != "" {
		shared, err := azblob.NewSharedKeyCredential(accountName, accountKey)
		if err == nil {
			item.AzureSharedKey = shared
			item.AzureServiceURL = serviceURL
			containerURL, joinErr := url.JoinPath(serviceURL, bucketName)
			if joinErr == nil {
				client, clientErr := container.NewClientWithSharedKeyCredential(containerURL, shared, nil)
				if clientErr == nil {
					if opened, openErr := azureblob.OpenBucket(ctx, client, nil); openErr == nil {
						item.Bucket = opened
					}
				}
			}
		}
	}
	if item.Bucket == nil {
		bucket, openErr := blob.OpenBucket(ctx, config.AzurePrefix+bucketName)
		if openErr != nil {
			return nil, openErr
		}
		item.Bucket = bucket
	}
	return item, nil
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
