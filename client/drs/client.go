package drs

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	drsapi "github.com/calypr/syfon/apigen/drs"

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
	Records []InternalRecordResponse `json:"records"`
}

type DrsClient struct {
	request.Requester
	xfer.Backend // Embedded for automatic delegation Across S3, GCS, and Azure.
	bucketName   string
	orgName      string
	projectId    string
	baseURL      string
	config       Config
}

// NewDrsClient is the Gen3 resolution layer initialization.
func NewDrsClient(req request.Requester, cred *conf.Credential, logger *logs.Gen3Logger) Client {
	c := &DrsClient{
		Requester: req,
		baseURL:   "",
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
func NewLocalDrsClient(req request.Requester, baseURL string, logger *logs.Gen3Logger) Client {
	c := &DrsClient{
		Requester: req,
		baseURL:   strings.TrimRight(strings.TrimSpace(baseURL), "/"),
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
	var rec InternalRecordResponse
	err := c.Do(ctx, http.MethodGet, c.endpoint(fmt.Sprintf(common.IndexdIndexRecordEndpointTemplate, url.PathEscape(id))), nil, &rec)
	if err != nil {
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
	q.Set(common.QueryParamHash, fmt.Sprintf("%s:%s", norm.Type, norm.Checksum))
	var list internalListResponse
	err := c.Do(ctx, http.MethodGet, c.endpoint(common.IndexdIndexEndpoint+"?"+q.Encode()), nil, &list)
	if err != nil {
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
	for _, h := range hashes {
		norm := NormalizeOid(strings.TrimSpace(h))
		if norm == "" {
			continue
		}
		typed = append(typed, fmt.Sprintf("%s:%s", hash.ChecksumTypeSHA256, norm))
		result[norm] = []DRSObject{}
	}
	if len(typed) == 0 {
		return result, nil
	}

	var list ListRecordsResponse
	err := c.Do(ctx, http.MethodPost, c.endpoint(common.IndexdIndexBulkHashesEndpoint), BulkHashesRequest{Hashes: typed}, &list)
	if err != nil {
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
	var out AccessURL
	err := c.Do(ctx, http.MethodGet, c.endpoint(fmt.Sprintf(common.GA4GHDRSObjectAccessEndpointTemplate, url.PathEscape(id), url.PathEscape(accessID))), nil, &out)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *DrsClient) GetDownloadPartURL(ctx context.Context, id string, start, end int64) (*xfer.SignedURL, error) {
	q := url.Values{}
	q.Set(common.QueryParamStart, fmt.Sprintf("%d", start))
	q.Set(common.QueryParamEnd, fmt.Sprintf("%d", end))

	var out AccessURL
	endpoint := fmt.Sprintf(common.DataDownloadPartEndpoint, url.PathEscape(id))
	err := c.Do(ctx, http.MethodGet, c.endpoint(endpoint+"?"+q.Encode()), nil, &out)
	if err != nil {
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
	var rec InternalRecordResponse
	err = c.Do(ctx, http.MethodPost, c.endpoint(common.IndexdIndexEndpoint), internalRecord, &rec)
	if err != nil {
		return nil, err
	}
	return syfonInternalRecordToDRSObject(rec)
}

func (c *DrsClient) RegisterRecords(ctx context.Context, records []*DRSObject) ([]*DRSObject, error) {
	if len(records) == 0 {
		return []*DRSObject{}, nil
	}

	internalRecords := make([]InternalRecordRequest, 0, len(records))
	for i, r := range records {
		internalRecord, err := drsObjectToSyfonInternalRecord(r)
		if err != nil {
			return nil, fmt.Errorf("record[%d] conversion failed: %w", i, err)
		}
		internalRecords = append(internalRecords, *internalRecord)
	}

	var out ListRecordsResponse
	err := c.Do(ctx, http.MethodPost, c.endpoint(common.IndexdIndexBulkEndpoint), BulkCreateRequest{Records: internalRecords}, &out)
	if err != nil {
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
	var rec InternalRecordResponse
	err = c.Do(ctx, http.MethodPut, c.endpoint(fmt.Sprintf(common.IndexdIndexRecordEndpointTemplate, url.PathEscape(did))), internalRecord, &rec)
	if err != nil {
		return nil, err
	}
	return syfonInternalRecordToDRSObject(rec)
}

func (c *DrsClient) DeleteRecordsByProject(ctx context.Context, projectId string) error {
	org, project := c.resolveScope(projectId)
	q := url.Values{}
	q.Set(common.QueryParamOrganization, org)
	q.Set(common.QueryParamProject, project)
	return c.Do(ctx, http.MethodDelete, c.endpoint(common.IndexdIndexEndpoint+"?"+q.Encode()), nil, nil)
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

	var out struct {
		Deleted *int32 `json:"deleted"`
	}
	err := c.Do(ctx, http.MethodPost, c.endpoint(common.IndexdIndexBulkDeleteEndpoint), BulkHashesRequest{Hashes: hashes}, &out)
	if err != nil {
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
	return c.Do(ctx, http.MethodDelete, c.endpoint(fmt.Sprintf(common.IndexdIndexRecordEndpointTemplate, url.PathEscape(did))), nil, nil)
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
		q.Set(common.QueryParamBucket, bucket)
	}
	if filename != "" {
		q.Set(common.QueryParamFileName, filename)
	}

	var out struct {
		URL string `json:"url"`
	}
	err := c.Do(ctx, http.MethodGet, c.endpoint(fmt.Sprintf(common.DataRecordEndpointTemplate, url.PathEscape(guid))), nil, &out, request.WithQueryValues(q))
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(out.URL) == "" {
		return "", fmt.Errorf("empty upload URL for %s", guid)
	}
	return out.URL, nil
}

func (c *DrsClient) InitMultipartUpload(ctx context.Context, guid string, filename string, bucket string) (string, string, error) {
	var out struct {
		GUID     string `json:"guid"`
		UploadID string `json:"uploadId"`
	}
	reqBody := map[string]string{
		"guid":      guid,
		"file_name": filename,
		"bucket":    bucket,
	}
	err := c.Do(ctx, http.MethodPost, c.endpoint(common.DataMultipartInitEndpoint), reqBody, &out)
	if err != nil {
		return "", "", err
	}
	if strings.TrimSpace(out.UploadID) == "" {
		return "", "", fmt.Errorf("multipart init missing uploadId")
	}
	return out.UploadID, out.GUID, nil
}

func (c *DrsClient) GetMultipartUploadURL(ctx context.Context, key string, uploadID string, partNumber int32, bucket string) (string, error) {
	var out struct {
		PresignedURL string `json:"presigned_url"`
	}
	reqBody := map[string]any{
		"key":        key,
		"bucket":     bucket,
		"uploadId":   uploadID,
		"partNumber": partNumber,
	}
	err := c.Do(ctx, http.MethodPost, c.endpoint(common.DataMultipartUploadEndpoint), reqBody, &out)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(out.PresignedURL) == "" {
		return "", fmt.Errorf("multipart upload URL response missing presigned_url")
	}
	return out.PresignedURL, nil
}

func (c *DrsClient) CompleteMultipartUpload(ctx context.Context, key string, uploadID string, parts []MultipartPart, bucket string) error {
	reqBody := map[string]any{
		"key":      key,
		"bucket":   bucket,
		"uploadId": uploadID,
		"parts":    parts,
	}
	return c.Do(ctx, http.MethodPost, c.endpoint(common.DataMultipartCompleteEndpoint), reqBody, nil)
}

// Orchestrators.

func (c *DrsClient) ResolveDownloadURL(ctx context.Context, guid string, accessID string) (string, error) {
	return ResolveDownloadURL(ctx, c, guid, accessID)
}

func (c *DrsClient) getListPage(ctx context.Context, q url.Values) (*internalListResponse, error) {
	var out internalListResponse
	err := c.Do(ctx, http.MethodGet, c.endpoint(common.IndexdIndexEndpoint), nil, &out, request.WithQueryValues(q))
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *DrsClient) Download(ctx context.Context, signedURL string, rangeStart, rangeEnd *int64) (*http.Response, error) {
	return xfer.GenericDownload(ctx, c.Requester, signedURL, rangeStart, rangeEnd)
}

func (c *DrsClient) Upload(ctx context.Context, signedURL string, body io.Reader, size int64) error {
	_, err := xfer.DoUpload(ctx, c.Requester, signedURL, body, size)
	return err
}

func (c *DrsClient) UploadPart(ctx context.Context, signedURL string, body io.Reader, size int64) (string, error) {
	return xfer.DoUpload(ctx, c.Requester, signedURL, body, size)
}

func (c *DrsClient) DeleteFile(ctx context.Context, guid string) (string, error) {
	if err := c.DeleteRecord(ctx, guid); err != nil {
		return "", err
	}
	return "deleted", nil
}

func (c *DrsClient) ListObjects(ctx context.Context) (chan DRSObjectResult, error) {
	q := url.Values{}
	q.Set(common.QueryParamLimit, "1000")
	return c.listObjects(ctx, q)
}

func (c *DrsClient) ListObjectsByProject(ctx context.Context, pid string) (chan DRSObjectResult, error) {
	org, project := c.resolveScope(pid)
	q := url.Values{}
	q.Set(common.QueryParamOrganization, org)
	q.Set(common.QueryParamProject, project)
	q.Set(common.QueryParamLimit, "1000")
	return c.listObjects(ctx, q)
}

func (c *DrsClient) GetProjectSample(ctx context.Context, pid string, l int) ([]DRSObject, error) {
	org, project := c.resolveScope(pid)
	limit := l
	if limit <= 0 {
		limit = 1
	}
	q := url.Values{}
	q.Set(common.QueryParamOrganization, org)
	q.Set(common.QueryParamProject, project)
	q.Set(common.QueryParamLimit, fmt.Sprintf("%d", limit))
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
	first := (*obj.AccessMethods)[0]
	raw := ""
	if first.AccessUrl != nil {
		raw = strings.TrimSpace(first.AccessUrl.Url)
	}
	if raw == "" {
		return "", "", fmt.Errorf("invalid storage URL: %s", raw)
	}
	parsed, parseErr := url.Parse(raw)
	if parseErr == nil && parsed != nil {
		switch strings.ToLower(strings.TrimSpace(parsed.Scheme)) {
		case "s3", "gs", "azblob":
			bucket := strings.TrimSpace(parsed.Host)
			key := strings.Trim(strings.TrimSpace(parsed.Path), "/")
			if bucket == "" || key == "" {
				return "", "", fmt.Errorf("invalid storage URL: %s", raw)
			}
			return bucket, key, nil
		}
	}

	// Backward-compatible fallback for plain or s3-prefixed values.
	legacy := strings.TrimPrefix(raw, "s3://")
	parts := strings.SplitN(legacy, "/", 2)
	if len(parts) < 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", "", fmt.Errorf("invalid storage URL: %s", raw)
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), nil
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
			qCopy.Set(common.QueryParamPage, fmt.Sprintf("%d", page))
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
