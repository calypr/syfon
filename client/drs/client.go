package drs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
 
	drsapi "github.com/calypr/syfon/apigen/drs"

	internalapi "github.com/calypr/syfon/apigen/internalapi"
	"github.com/calypr/syfon/client/conf"
	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/pkg/hash"
	"github.com/calypr/syfon/client/pkg/logs"
	"github.com/calypr/syfon/client/pkg/request"
	"github.com/calypr/syfon/client/xfer"
)

type Config struct {
	MultiPartThreshold int64
}

type internalListResponse struct {
	Records []internalapi.InternalRecordResponse `json:"records"`
}

func responseBodyError(resp *http.Response, prefix string) error {
	if resp == nil {
		return fmt.Errorf("%s: no response", prefix)
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	bodyText := strings.TrimSpace(string(body))
	if bodyText == "" {
		return fmt.Errorf("%s: %s", prefix, resp.Status)
	}
	return fmt.Errorf("%s: %s body=%s", prefix, resp.Status, bodyText)
}

type DrsClient struct {
	request.RequestInterface
	xfer.Backend // Embedded for automatic delegation Across S3, GCS, and Azure.
	bucketName   string
	orgName      string
	projectId    string
	baseURL      string
	config       Config
}

// NewDrsClient is the Gen3 resolution layer initialization.
func NewDrsClient(req request.RequestInterface, cred *conf.Credential, logger *logs.Gen3Logger) Client {
	c := &DrsClient{
		RequestInterface: req,
		baseURL:          "",
		config: Config{
			MultiPartThreshold: common.FileSizeLimit,
		},
	}
	if cred != nil {
		c.baseURL = strings.TrimRight(strings.TrimSpace(cred.APIEndpoint), "/")
	}
	c.Backend = xfer.New(logger, c)
	return c
}

// NewLocalDrsClient is the local resolution layer initialization.
func NewLocalDrsClient(req request.RequestInterface, baseURL string, logger *logs.Gen3Logger) Client {
	c := &DrsClient{
		RequestInterface: req,
		baseURL:          strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		config: Config{
			MultiPartThreshold: common.FileSizeLimit,
		},
	}
	c.Backend = xfer.New(logger, c)
	return c
}

func (c *DrsClient) endpoint(path string) string {
	if strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://") {
		return path
	}
	if c.baseURL == "" {
		return path
	}
	if strings.HasPrefix(path, "/") {
		return c.baseURL + path
	}
	return c.baseURL + "/" + path
}

// Name is overridden here to provide DRS identity, while Logger delegates to the backend.
func (c *DrsClient) Name() string { return "DRSClient" }

// Resolve translates a GUID into a physical transfer specification.
func (c *DrsClient) Resolve(ctx context.Context, id string) (*xfer.ResolvedObject, error) {
	drsObject, err := ResolveObject(ctx, c, id)
	if err != nil {
		return nil, err
	}

	resolved := &xfer.ResolvedObject{
		Id:           drsObject.Id,
		Size:         drsObject.Size,
		AccessMethod: "s3",
	}
	if drsObject.Name != nil {
		resolved.Name = *drsObject.Name
	}

	if drsObject.AccessMethods != nil {
		for _, am := range *drsObject.AccessMethods {
			if am.AccessUrl != nil && am.AccessUrl.Url != "" {
				resolved.ProviderURL = am.AccessUrl.Url
				resolved.AccessMethod = string(am.Type)
				break
			}
		}
	}

	return resolved, nil
}

// Core Resolution Layer Implementation (Indexd / DRS).

func (c *DrsClient) GetObject(ctx context.Context, id string) (*DRSObject, error) {
	rb := c.New(http.MethodGet, c.endpoint(fmt.Sprintf("/index/%s", url.PathEscape(id))))
	resp, err := c.Do(ctx, rb)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, responseBodyError(resp, fmt.Sprintf("failed to get metadata for %s", id))
	}

	var rec internalapi.InternalRecordResponse
	if err := json.NewDecoder(resp.Body).Decode(&rec); err != nil {
		return nil, err
	}
	return syfonInternalRecordToDRSObject(rec)
}

func (c *DrsClient) GetObjectByHash(ctx context.Context, ck *hash.Checksum) ([]DRSObject, error) {
	if ck == nil {
		return nil, fmt.Errorf("checksum is required")
	}
	norm := hash.Checksum{
		Type:     string(hash.NormalizeChecksumType(ck.Type)),
		Checksum: strings.TrimSpace(ck.Checksum),
	}
	if err := hash.ValidateChecksum(norm); err != nil {
		return nil, fmt.Errorf("invalid checksum query: %w", err)
	}

	q := url.Values{}
	q.Set("hash", fmt.Sprintf("%s:%s", norm.Type, norm.Checksum))
	rb := c.New(http.MethodGet, c.endpoint("/index?"+q.Encode()))
	resp, err := c.Do(ctx, rb)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, responseBodyError(resp, fmt.Sprintf("failed to get metadata by checksum %s:%s", norm.Type, norm.Checksum))
	}

	var list internalListResponse
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		return nil, err
	}
	records := make([]DRSObject, 0, len(list.Records))
	for _, rec := range list.Records {
		obj, convErr := syfonInternalRecordToDRSObject(rec)
		if convErr != nil {
			return nil, convErr
		}
		records = append(records, *obj)
	}
	return records, nil
}

// GetObjectByChecksum is an explicit typed alias for checksum lookup.
func (c *DrsClient) GetObjectByChecksum(ctx context.Context, ck *hash.Checksum) ([]DRSObject, error) {
	return c.GetObjectByHash(ctx, ck)
}

// BatchGetObjectsByChecksums resolves multiple typed checksum queries.
// Result key format is "<type>:<checksum>".
func (c *DrsClient) BatchGetObjectsByChecksums(ctx context.Context, checksums []*hash.Checksum) (map[string][]DRSObject, error) {
	result := make(map[string][]DRSObject, len(checksums))
	for _, ck := range checksums {
		if ck == nil {
			continue
		}
		norm := hash.Checksum{
			Type:     string(hash.NormalizeChecksumType(ck.Type)),
			Checksum: strings.TrimSpace(ck.Checksum),
		}
		key := fmt.Sprintf("%s:%s", norm.Type, norm.Checksum)
		objs, err := c.GetObjectByHash(ctx, &norm)
		if err != nil {
			continue
		}
		result[key] = objs
	}
	return result, nil
}

func (c *DrsClient) BatchGetObjectsByHash(ctx context.Context, hashes []string) (map[string][]DRSObject, error) {
	result := make(map[string][]DRSObject, len(hashes))
	if len(hashes) == 0 {
		return result, nil
	}

	typed := make([]string, 0, len(hashes))
	normalized := make([]string, 0, len(hashes))
	for _, h := range hashes {
		norm := NormalizeOid(strings.TrimSpace(h))
		if norm == "" {
			continue
		}
		normalized = append(normalized, norm)
		typed = append(typed, fmt.Sprintf("%s:%s", hash.ChecksumTypeSHA256, norm))
		result[norm] = []DRSObject{}
	}
	if len(typed) == 0 {
		return result, nil
	}

	body, _ := json.Marshal(internalapi.BulkHashesRequest{Hashes: typed})
	rb := c.New(http.MethodPost, c.endpoint("/index/bulk/hashes")).
		WithBody(bytes.NewReader(body)).
		WithHeader("Content-Type", "application/json")
	resp, err := c.Do(ctx, rb)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, responseBodyError(resp, "failed to bulk get metadata by hash")
	}

	var list internalapi.ListRecordsResponse
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		return nil, err
	}
	if list.Records != nil {
		for _, rec := range *list.Records {
		obj, convErr := syfonInternalRecordToDRSObjectFromRecord(rec)
		if convErr != nil {
			return nil, convErr
		}
		sha := NormalizeOid(hash.ConvertDrsChecksumsToHashInfo(obj.Checksums).SHA256)
		if sha == "" {
			continue
		}
		if _, ok := result[sha]; !ok {
			result[sha] = []DRSObject{}
		}
		result[sha] = append(result[sha], *obj)
		}
	}
	return result, nil
}

func (c *DrsClient) GetDownloadURL(ctx context.Context, id string, accessID string) (*AccessURL, error) {
	rb := c.New(http.MethodGet, c.endpoint(fmt.Sprintf("/ga4gh/drs/v1/objects/%s/access/%s", url.PathEscape(id), url.PathEscape(accessID))))
	resp, err := c.Do(ctx, rb)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, responseBodyError(resp, fmt.Sprintf("failed to get access URL for %s", id))
	}

	var out AccessURL
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *DrsClient) GetDownloadPartURL(ctx context.Context, id string, start, end int64) (*xfer.SignedURL, error) {
	q := url.Values{}
	q.Set("start", fmt.Sprintf("%d", start))
	q.Set("end", fmt.Sprintf("%d", end))

	endpoint := fmt.Sprintf(common.FenceDataDownloadPartEndpoint, url.PathEscape(id))
	rb := c.New(http.MethodGet, c.endpoint(endpoint+"?"+q.Encode()))
	resp, err := c.Do(ctx, rb)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, responseBodyError(resp, fmt.Sprintf("failed to get download part URL for %s", id))
	}

	var out AccessURL
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}

	headers := make(map[string]string)
	if out.Headers != nil {
		for _, h := range *out.Headers {
			parts := strings.SplitN(h, ":", 2)
			if len(parts) == 2 {
				headers[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
			}
		}
	}

	return &xfer.SignedURL{
		URL:     out.Url,
		Headers: headers,
	}, nil
}

func (c *DrsClient) GetUploadURL(ctx context.Context, id string) (*AccessURL, error) {
	return nil, fmt.Errorf("high-level upload URL resolution not implemented in pure resolver")
}

// MutableMetadataManager Implementation.

func (c *DrsClient) RegisterRecord(ctx context.Context, record *DRSObject) (*DRSObject, error) {
	internalRecord, err := drsObjectToSyfonInternalRecord(record)
	if err != nil {
		return nil, err
	}
	body, _ := json.Marshal(internalRecord)
	rb := c.New(http.MethodPost, c.endpoint("/index")).WithBody(bytes.NewReader(body)).WithHeader(common.HeaderContentType, common.MIMEApplicationJSON)
	resp, err := c.Do(ctx, rb)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return nil, responseBodyError(resp, "failed to register record")
	}

	var rec internalapi.InternalRecordResponse
	if err := json.NewDecoder(resp.Body).Decode(&rec); err != nil {
		return nil, err
	}
	return syfonInternalRecordToDRSObject(rec)
}

func (c *DrsClient) RegisterRecords(ctx context.Context, records []*DRSObject) ([]*DRSObject, error) {
	if len(records) == 0 {
		return []*DRSObject{}, nil
	}

	internalRecords := make([]internalapi.InternalRecord, 0, len(records))
	for i, r := range records {
		internalRecord, err := drsObjectToSyfonInternalRecord(r)
		if err != nil {
			return nil, fmt.Errorf("record[%d] conversion failed: %w", i, err)
		}
		internalRecords = append(internalRecords, *internalRecord)
	}

	body, _ := json.Marshal(internalapi.BulkCreateRequest{Records: internalRecords})
	rb := c.New(http.MethodPost, c.endpoint("/index/bulk")).
		WithBody(bytes.NewReader(body)).
		WithHeader("Content-Type", "application/json")
	resp, err := c.Do(ctx, rb)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return nil, responseBodyError(resp, "failed to register records")
	}

	var out internalapi.ListRecordsResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	results := make([]*DRSObject, 0)
	if out.Records != nil {
		results = make([]*DRSObject, 0, len(*out.Records))
		for _, rec := range *out.Records {
			obj, convErr := syfonInternalRecordToDRSObjectFromRecord(rec)
			if convErr != nil {
				return nil, convErr
			}
			results = append(results, obj)
		}
	}
	return results, nil
}

func (c *DrsClient) UpdateRecord(ctx context.Context, updateInfo *DRSObject, did string) (*DRSObject, error) {
	internalRecord, err := drsObjectToSyfonInternalRecord(updateInfo)
	if err != nil {
		return nil, err
	}
	body, _ := json.Marshal(internalRecord)
	rb := c.New(http.MethodPut, c.endpoint(fmt.Sprintf("/index/%s", url.PathEscape(did)))).
		WithBody(bytes.NewReader(body)).
		WithHeader(common.HeaderContentType, common.MIMEApplicationJSON)
	resp, err := c.Do(ctx, rb)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, responseBodyError(resp, fmt.Sprintf("failed to update record %s", did))
	}

	var rec internalapi.InternalRecordResponse
	if err := json.NewDecoder(resp.Body).Decode(&rec); err != nil {
		return nil, err
	}
	return syfonInternalRecordToDRSObject(rec)
}

func (c *DrsClient) DeleteRecordsByProject(ctx context.Context, projectId string) error {
	org, project := c.resolveScope(projectId)
	q := url.Values{}
	q.Set("organization", org)
	q.Set("project", project)
	rb := c.New(http.MethodDelete, c.endpoint("/index?"+q.Encode()))
	resp, err := c.Do(ctx, rb)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return responseBodyError(resp, fmt.Sprintf("failed to delete project records for %s", projectId))
	}
	return nil
}

func (c *DrsClient) DeleteRecordByOID(ctx context.Context, oid string) error {
	return c.DeleteRecordByChecksum(ctx, &hash.Checksum{
		Type:     string(hash.ChecksumTypeSHA256),
		Checksum: NormalizeOid(strings.TrimSpace(oid)),
	})
}

func (c *DrsClient) DeleteRecordsByChecksums(ctx context.Context, checksums []*hash.Checksum) (int, error) {
	hashes := make([]string, 0, len(checksums))
	for _, ck := range checksums {
		if ck == nil {
			continue
		}
		norm := hash.Checksum{
			Type:     string(hash.NormalizeChecksumType(ck.Type)),
			Checksum: strings.TrimSpace(ck.Checksum),
		}
		hashes = append(hashes, fmt.Sprintf("%s:%s", norm.Type, norm.Checksum))
	}
	if len(hashes) == 0 {
		return 0, nil
	}

	body, _ := json.Marshal(internalapi.BulkHashesRequest{Hashes: hashes})
	rb := c.New(http.MethodPost, c.endpoint("/index/bulk/delete")).
		WithBody(bytes.NewReader(body)).
		WithHeader("Content-Type", "application/json")
	resp, err := c.Do(ctx, rb)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, responseBodyError(resp, "failed to bulk delete records")
	}

	var out struct {
		Deleted *int32 `json:"deleted"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, err
	}
	if out.Deleted == nil {
		return 0, nil
	}
	return int(*out.Deleted), nil
}

func (c *DrsClient) DeleteRecordByChecksum(ctx context.Context, ck *hash.Checksum) error {
	_, err := c.DeleteRecordsByChecksums(ctx, []*hash.Checksum{ck})
	return err
}

func (c *DrsClient) DeleteRecord(ctx context.Context, did string) error {
	rb := c.New(http.MethodDelete, c.endpoint(fmt.Sprintf("/index/%s", url.PathEscape(did))))
	resp, err := c.Do(ctx, rb)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (c *DrsClient) RegisterFile(ctx context.Context, oid, path string) (*DRSObject, error) {
	oid = NormalizeOid(oid)
	if strings.TrimSpace(oid) == "" {
		return nil, fmt.Errorf("oid is required")
	}
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("path is required")
	}
	stat, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(c.projectId) == "" {
		return nil, fmt.Errorf("project ID is required")
	}
	if strings.TrimSpace(c.bucketName) == "" {
		return nil, fmt.Errorf("bucket name is required")
	}
	org, project := c.resolveScope(c.projectId)
	did := DrsUUID(org, project, oid)
	obj, err := BuildDrsObjWithPrefix(filepath.Base(path), oid, stat.Size(), did, c.bucketName, c.orgName, c.projectId, "")
	if err != nil {
		return nil, err
	}
	return c.RegisterRecord(ctx, obj)
}

func (c *DrsClient) AddURL(ctx context.Context, blobURL, sha256 string, opts ...AddURLOption) (*DRSObject, error) {
	sha256 = NormalizeOid(strings.TrimSpace(sha256))
	if sha256 == "" {
		return nil, fmt.Errorf("sha256 is required")
	}
	if strings.TrimSpace(blobURL) == "" {
		return nil, fmt.Errorf("blobURL is required")
	}
	if strings.TrimSpace(c.projectId) == "" {
		return nil, fmt.Errorf("project ID is required")
	}

	parsedURL, err := url.Parse(blobURL)
	if err != nil {
		return nil, err
	}
	name := filepath.Base(strings.TrimSpace(parsedURL.Path))
	if name == "." || name == "/" || name == "" {
		name = sha256
	}

	org, project := c.resolveScope(c.projectId)
	did := DrsUUID(org, project, sha256)
	obj := &DRSObject{
		Id:   did,
		Name: &name,
		Checksums: []Checksum{{
			Type:     "sha256",
			Checksum: sha256,
		}},
		Size: 0,
		AccessMethods: &[]AccessMethod{{
			Type: drsapi.AccessMethodType(parsedURL.Scheme),
			AccessUrl: &struct {
				Headers *[]string "json:\"headers,omitempty\""
				Url     string    "json:\"url\""
			}{
				Url: blobURL,
			},
		}},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(obj)
		}
	}
	return c.RegisterRecord(ctx, obj)
}

func (c *DrsClient) UpsertRecord(ctx context.Context, url string, sha256 string, fileSize int64, projectId string) (*DRSObject, error) {
	sha256 = NormalizeOid(strings.TrimSpace(sha256))
	if sha256 == "" {
		return nil, fmt.Errorf("sha256 is required")
	}
	project := strings.TrimSpace(projectId)
	if project == "" {
		project = strings.TrimSpace(c.projectId)
	}
	if project == "" {
		return nil, fmt.Errorf("project ID is required")
	}
	if strings.TrimSpace(c.bucketName) == "" {
		return nil, fmt.Errorf("bucket name is required")
	}
	org, project := c.resolveScope(projectId)
	did := DrsUUID(org, project, sha256)
	obj, err := BuildDrsObjWithPrefix(filepath.Base(strings.TrimSpace(url)), sha256, fileSize, did, c.bucketName, c.orgName, project, "")
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(url) != "" && obj.AccessMethods != nil && len(*obj.AccessMethods) > 0 {
		(*obj.AccessMethods)[0].AccessUrl.Url = strings.TrimSpace(url)
	}

	recs, err := c.GetObjectByHash(ctx, &hash.Checksum{Type: string(hash.ChecksumTypeSHA256), Checksum: sha256})
	if err == nil && len(recs) > 0 {
		if match, matchErr := FindMatchingRecord(recs, c.orgName, project); matchErr == nil && match != nil {
			return c.UpdateRecord(ctx, obj, match.Id)
		}
	}
	return c.RegisterRecord(ctx, obj)
}

func (c *DrsClient) ResolveUploadURL(ctx context.Context, guid, filename string, metadata common.FileMetadata, bucket string) (string, error) {
	q := url.Values{}
	if bucket != "" {
		q.Set("bucket", bucket)
	}
	if filename != "" {
		q.Set("file_name", filename)
	}

	path := fmt.Sprintf("/data/upload/%s", url.PathEscape(guid))
	if encoded := q.Encode(); encoded != "" {
		path += "?" + encoded
	}

	rb := c.New(http.MethodGet, c.endpoint(path))
	resp, err := c.Do(ctx, rb)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", responseBodyError(resp, fmt.Sprintf("failed to resolve upload URL for %s", guid))
	}

	var out struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	if strings.TrimSpace(out.URL) == "" {
		return "", fmt.Errorf("empty upload URL for %s", guid)
	}
	return out.URL, nil
}

func (c *DrsClient) InitMultipartUpload(ctx context.Context, guid string, filename string, bucket string) (string, string, error) {
	body, err := json.Marshal(map[string]string{
		"guid":      guid,
		"file_name": filename,
		"bucket":    bucket,
	})
	if err != nil {
		return "", "", err
	}
	rb := c.New(http.MethodPost, c.endpoint("/data/multipart/init")).
		WithBody(bytes.NewReader(body)).
		WithHeader("Content-Type", "application/json")
	resp, err := c.Do(ctx, rb)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return "", "", responseBodyError(resp, "failed to init multipart upload")
	}
	var out struct {
		GUID     string `json:"guid"`
		UploadID string `json:"uploadId"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", "", err
	}
	if strings.TrimSpace(out.UploadID) == "" {
		return "", "", fmt.Errorf("multipart init missing uploadId")
	}
	return out.UploadID, out.GUID, nil
}

func (c *DrsClient) GetMultipartUploadURL(ctx context.Context, key string, uploadID string, partNumber int32, bucket string) (string, error) {
	body, err := json.Marshal(map[string]any{
		"key":        key,
		"bucket":     bucket,
		"uploadId":   uploadID,
		"partNumber": partNumber,
	})
	if err != nil {
		return "", err
	}
	rb := c.New(http.MethodPost, c.endpoint("/data/multipart/upload")).
		WithBody(bytes.NewReader(body)).
		WithHeader("Content-Type", "application/json")
	resp, err := c.Do(ctx, rb)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return "", responseBodyError(resp, "failed to resolve multipart upload URL")
	}
	var out struct {
		PresignedURL string `json:"presigned_url"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	if strings.TrimSpace(out.PresignedURL) == "" {
		return "", fmt.Errorf("multipart upload URL response missing presigned_url")
	}
	return out.PresignedURL, nil
}

func (c *DrsClient) CompleteMultipartUpload(ctx context.Context, key string, uploadID string, parts []internalapi.InternalMultipartPart, bucket string) error {
	body, err := json.Marshal(map[string]any{
		"key":      key,
		"bucket":   bucket,
		"uploadId": uploadID,
		"parts":    parts,
	})
	if err != nil {
		return err
	}
	rb := c.New(http.MethodPost, c.endpoint("/data/multipart/complete")).
		WithBody(bytes.NewReader(body)).
		WithHeader("Content-Type", "application/json")
	resp, err := c.Do(ctx, rb)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		return responseBodyError(resp, "failed to complete multipart upload")
	}
	return nil
}

// Orchestrators.

func (c *DrsClient) ResolveDownloadURL(ctx context.Context, guid string, accessID string) (string, error) {
	return ResolveDownloadURL(ctx, c, guid, accessID)
}

func (c *DrsClient) Download(ctx context.Context, signedURL string, rangeStart, rangeEnd *int64) (*http.Response, error) {
	return xfer.GenericDownload(ctx, c.RequestInterface, signedURL, rangeStart, rangeEnd)
}

func (c *DrsClient) Upload(ctx context.Context, signedURL string, body io.Reader, size int64) error {
	_, err := xfer.DoUpload(ctx, c.RequestInterface, signedURL, body, size)
	return err
}

func (c *DrsClient) UploadPart(ctx context.Context, signedURL string, body io.Reader, size int64) (string, error) {
	return xfer.DoUpload(ctx, c.RequestInterface, signedURL, body, size)
}

func (c *DrsClient) DeleteFile(ctx context.Context, guid string) (string, error) {
	if err := c.DeleteRecord(ctx, guid); err != nil {
		return "", err
	}
	return "deleted", nil
}

func (c *DrsClient) ListObjects(ctx context.Context) (chan DRSObjectResult, error) {
	q := url.Values{}
	q.Set("limit", "1000")
	return c.listObjects(ctx, q)
}

func (c *DrsClient) ListObjectsByProject(ctx context.Context, pid string) (chan DRSObjectResult, error) {
	org, project := c.resolveScope(pid)
	q := url.Values{}
	q.Set("organization", org)
	q.Set("project", project)
	q.Set("limit", "1000")
	return c.listObjects(ctx, q)
}

func (c *DrsClient) GetProjectSample(ctx context.Context, pid string, l int) ([]DRSObject, error) {
	org, project := c.resolveScope(pid)
	limit := l
	if limit <= 0 {
		limit = 1
	}
	q := url.Values{}
	q.Set("organization", org)
	q.Set("project", project)
	q.Set("limit", fmt.Sprintf("%d", limit))
	resp, err := c.getListPage(ctx, q)
	if err != nil {
		return nil, err
	}
	records := make([]DRSObject, 0, len(resp.Records))
	for _, rec := range resp.Records {
		obj, convErr := syfonInternalRecordToDRSObject(rec)
		if convErr != nil {
			return nil, convErr
		}
		records = append(records, *obj)
	}
	return records, nil
}

// GetStorageLocation implements transfer.Provider across S3, GCS, and Azure.
func (c *DrsClient) GetStorageLocation(ctx context.Context, guid string) (bucket, key string, err error) {
	obj, err := c.GetObject(ctx, guid)
	if err != nil {
		return "", "", err
	}
	if obj.AccessMethods == nil || len(*obj.AccessMethods) == 0 {
		return "", "", fmt.Errorf("no access methods found")
	}
	u := (*obj.AccessMethods)[0].AccessUrl.Url
	u = strings.TrimPrefix(u, "s3://")
	parts := strings.SplitN(u, "/", 2)
	if len(parts) < 2 {
		return "", "", fmt.Errorf("invalid storage URL: %s", u)
	}
	return parts[0], parts[1], nil
}

// Fluent context helpers.

func (c *DrsClient) GetProjectId() string    { return c.projectId }
func (c *DrsClient) GetBucketName() string   { return c.bucketName }
func (c *DrsClient) GetOrganization() string { return c.orgName }

func (c *DrsClient) WithProject(projectId string) Client {
	c.projectId = projectId
	return c
}
func (c *DrsClient) WithOrganization(orgName string) Client {
	c.orgName = orgName
	return c
}
func (c *DrsClient) WithBucket(bucketName string) Client {
	c.bucketName = bucketName
	return c
}

func (c *DrsClient) resolveScope(projectId string) (organization string, project string) {
	project = strings.TrimSpace(projectId)
	if project == "" {
		project = strings.TrimSpace(c.projectId)
	}
	organization = strings.TrimSpace(c.orgName)
	if organization != "" {
		return organization, project
	}
	if strings.Contains(project, "-") {
		parts := strings.SplitN(project, "-", 2)
		if len(parts) == 2 && strings.TrimSpace(parts[0]) != "" && strings.TrimSpace(parts[1]) != "" {
			return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
		}
	}
	return "default", project
}

func (c *DrsClient) getListPage(ctx context.Context, q url.Values) (*internalListResponse, error) {
	path := "/index"
	if q != nil && len(q) > 0 {
		path += "?" + q.Encode()
	}
	rb := c.New(http.MethodGet, c.endpoint(path))
	resp, err := c.Do(ctx, rb)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, responseBodyError(resp, "failed to list records")
	}
	var out internalListResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *DrsClient) listObjects(ctx context.Context, q url.Values) (chan DRSObjectResult, error) {
	out := make(chan DRSObjectResult, 1)
	go func() {
		defer close(out)
		page := 0
		for {
			qCopy := url.Values{}
			for k, vv := range q {
				for _, v := range vv {
					qCopy.Add(k, v)
				}
			}
			qCopy.Set("page", fmt.Sprintf("%d", page))
			resp, err := c.getListPage(ctx, qCopy)
			if err != nil {
				out <- DRSObjectResult{Error: err}
				return
			}
			if len(resp.Records) == 0 {
				return
			}
			for _, rec := range resp.Records {
				obj, convErr := syfonInternalRecordToDRSObject(rec)
				if convErr != nil {
					out <- DRSObjectResult{Error: convErr}
					return
				}
				out <- DRSObjectResult{Object: obj}
			}
			page++
		}
	}()
	return out, nil
}
