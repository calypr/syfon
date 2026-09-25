package services

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/errorapi"
	"github.com/calypr/syfon/apigen/internalapi"
	"github.com/calypr/syfon/client/apierror"
	"github.com/calypr/syfon/client/common"
	clienthash "github.com/calypr/syfon/client/hash"
	"github.com/calypr/syfon/client/transfer"
)

const maxDownloadErrorPreview = 4 << 10

func (d *DataService) Stat(ctx context.Context, guid string) (*transfer.ObjectMetadata, error) {
	if d.drs != nil {
		obj, err := d.drs.GetObject(ctx, guid)
		if err == nil {
			md := &transfer.ObjectMetadata{
				Size:      obj.Size,
				SizeKnown: true,
				Identity:  downloadObjectIdentity(&obj),
			}
			if obj.AccessMethods != nil && len(*obj.AccessMethods) > 0 {
				md.AcceptRanges = true
			}
			return md, nil
		}
		if !errorapi.IsNotFoundError(err) {
			return nil, fmt.Errorf("get DRS metadata for %s: %w", guid, err)
		}
	}
	_, err := d.ResolveDownloadURL(ctx, guid, "")
	if err != nil {
		return nil, err
	}
	return &transfer.ObjectMetadata{
		AcceptRanges: true,
		Size:         0,
	}, nil
}

func downloadObjectIdentity(object *drs.DrsObject) string {
	if object == nil {
		return ""
	}
	checksum := clienthash.ConvertDrsChecksumsToHashInfo(object.Checksums).SHA256
	value := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(checksum)), "sha256:")
	decoded, err := hex.DecodeString(value)
	if err == nil && len(decoded) == 32 {
		return "sha256:" + value
	}
	return ""
}

func (d *DataService) GetReader(ctx context.Context, guid string) (io.ReadCloser, error) {
	signedURL, err := d.ResolveDownloadURL(ctx, guid, "")
	if err != nil {
		return nil, err
	}
	resp, err := d.Download(ctx, signedURL, nil, nil)
	if err != nil {
		return nil, err
	}
	return readBackendResponse(resp)
}

func (d *DataService) GetRangeReader(ctx context.Context, guid string, offset, length int64) (io.ReadCloser, error) {
	if offset < 0 || length < 0 {
		return nil, fmt.Errorf("invalid range request: offset=%d length=%d", offset, length)
	}
	var end *int64
	if length > 0 {
		e := offset + length - 1
		if e < offset {
			return nil, fmt.Errorf("invalid range request: offset=%d length=%d overflows", offset, length)
		}
		end = &e
	}
	signedURL, err := d.ResolveDownloadURL(ctx, guid, "")
	if err != nil {
		return nil, err
	}
	resp, err := d.Download(ctx, signedURL, &offset, end)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("download response is nil")
	}
	if resp.Body == nil {
		return nil, fmt.Errorf("download response body is nil")
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return nil, failedDownloadResponse(resp)
	}
	if resp.StatusCode == http.StatusOK {
		_ = resp.Body.Close()
		return nil, transfer.ErrRangeIgnored
	}
	if resp.StatusCode != http.StatusPartialContent {
		return nil, failedDownloadResponse(resp)
	}
	if err := validateContentRange(resp.Header.Get("Content-Range"), offset, end); err != nil {
		closeErr := resp.Body.Close()
		if closeErr != nil {
			return nil, fmt.Errorf("%w: close invalid range response: %v", err, closeErr)
		}
		return nil, err
	}
	return resp.Body, nil
}

func readBackendResponse(resp *http.Response) (io.ReadCloser, error) {
	if resp == nil {
		return nil, fmt.Errorf("download response is nil")
	}
	if resp.Body == nil {
		return nil, fmt.Errorf("download response body is nil")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, failedDownloadResponse(resp)
	}
	return resp.Body, nil
}

func validateContentRange(value string, requestedStart int64, requestedEnd *int64) error {
	invalid := func(reason string) error {
		return fmt.Errorf("invalid Content-Range %q: %s", value, reason)
	}
	unit, rangeValue, ok := strings.Cut(strings.TrimSpace(value), " ")
	if !ok || !strings.EqualFold(unit, "bytes") || strings.Contains(rangeValue, " ") {
		return invalid("expected bytes start-end/total")
	}
	span, totalValue, ok := strings.Cut(rangeValue, "/")
	if !ok || strings.Contains(totalValue, "/") || totalValue == "*" {
		return invalid("a known total is required")
	}
	startValue, endValue, ok := strings.Cut(span, "-")
	if !ok || strings.Contains(endValue, "-") {
		return invalid("expected bytes start-end/total")
	}
	start, err := parseContentRangeNumber(startValue)
	if err != nil {
		return invalid("invalid start")
	}
	end, err := parseContentRangeNumber(endValue)
	if err != nil || end < start {
		return invalid("invalid end")
	}
	total, err := parseContentRangeNumber(totalValue)
	if err != nil || total <= end {
		return invalid("total must be greater than the returned end")
	}
	if start != requestedStart {
		return invalid(fmt.Sprintf("returned start %d does not match requested start %d", start, requestedStart))
	}
	if requestedEnd == nil {
		if end != total-1 {
			return invalid("open-ended request did not reach the known total")
		}
		return nil
	}
	if end == *requestedEnd {
		return nil
	}
	if end == total-1 && end < *requestedEnd {
		return nil
	}
	return invalid(fmt.Sprintf("returned end %d does not match requested end %d or a proven EOF clamp", end, *requestedEnd))
}

func parseContentRangeNumber(value string) (int64, error) {
	if value == "" {
		return 0, fmt.Errorf("empty number")
	}
	for _, digit := range value {
		if digit < '0' || digit > '9' {
			return 0, fmt.Errorf("invalid decimal number")
		}
	}
	return strconv.ParseInt(value, 10, 64)
}

func failedDownloadResponse(resp *http.Response) error {
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxDownloadErrorPreview))
	closeErr := resp.Body.Close()
	var err error = apierror.FromResponse(redactedResponse(resp), body)
	if readErr != nil {
		err = fmt.Errorf("read download error response: %w: %v", err, readErr)
	}
	if closeErr != nil && readErr == nil {
		err = fmt.Errorf("close download error response: %w: %v", err, closeErr)
	}
	return err
}

func redactedResponse(resp *http.Response) *http.Response {
	if resp == nil || resp.Request == nil || resp.Request.URL == nil {
		return resp
	}
	copyResponse := *resp
	copyRequest := *resp.Request
	copyURL := *resp.Request.URL
	copyURL.RawQuery = ""
	copyURL.ForceQuery = false
	copyURL.Fragment = ""
	copyRequest.URL = &copyURL
	copyResponse.Request = &copyRequest
	return &copyResponse
}

func (d *DataService) ResolveDownloadURL(ctx context.Context, guid string, accessID string) (string, error) {
	resp, err := d.DownloadURL(ctx, guid, 0, false)
	if err != nil {
		return "", err
	}
	if resp.Url == nil {
		return "", fmt.Errorf("response missing URL")
	}
	return *resp.Url, nil
}

func (d *DataService) Download(ctx context.Context, signedURL string, rangeStart, rangeEnd *int64) (*http.Response, error) {
	return transfer.GenericDownload(ctx, d.httpClient, signedURL, rangeStart, rangeEnd)
}

func (d *DataService) ResolveUploadURL(ctx context.Context, guid, filename string, metadata common.FileMetadata, bucket string) (string, error) {
	organization, project, err := uploadScopeFromMetadata(metadata)
	if err != nil {
		return "", err
	}
	params := &internalapi.InternalUploadURLParams{Key: &filename}
	if organization != "" {
		params.Organization = &organization
	}
	if project != "" {
		params.Project = &project
	}
	resp, err := d.UploadURL(ctx, guid, params)
	if err != nil {
		return "", err
	}
	if resp.Url == nil {
		return "", fmt.Errorf("response missing URL")
	}
	return *resp.Url, nil
}

type uploadScope struct {
	organization string
	project      string
}

func uploadScopeFromMetadata(metadata common.FileMetadata) (string, string, error) {
	if len(metadata.Authorizations) == 0 {
		return "", "", nil
	}

	scopes := make([]uploadScope, 0, len(metadata.Authorizations))
	for org, projects := range metadata.Authorizations {
		org = strings.TrimSpace(org)
		if org == "" {
			continue
		}
		projectSeen := false
		broadScopeSeen := false
		for _, project := range projects {
			project = strings.TrimSpace(project)
			if project == "" {
				broadScopeSeen = true
				continue
			}
			projectSeen = true
			scopes = append(scopes, uploadScope{organization: org, project: project})
		}
		if broadScopeSeen || !projectSeen {
			scopes = append(scopes, uploadScope{organization: org})
		}
	}

	sort.Slice(scopes, func(i, j int) bool {
		if scopes[i].organization != scopes[j].organization {
			return scopes[i].organization < scopes[j].organization
		}
		return scopes[i].project < scopes[j].project
	})
	unique := scopes[:0]
	for _, scope := range scopes {
		if len(unique) > 0 && unique[len(unique)-1] == scope {
			continue
		}
		unique = append(unique, scope)
	}
	if len(unique) == 0 {
		return "", "", nil
	}
	if len(unique) > 1 {
		return "", "", fmt.Errorf("%w: upload metadata must identify one organization-wide or project scope", errorapi.ErrInvalidInput)
	}
	return unique[0].organization, unique[0].project, nil
}

func (d *DataService) Upload(ctx context.Context, url string, body io.Reader, size int64) error {
	_, err := transfer.DoUpload(ctx, d.httpClient, url, body, size)
	return err
}

func (d *DataService) UploadPart(ctx context.Context, url string, body io.Reader, size int64) (string, error) {
	return transfer.DoUpload(ctx, d.httpClient, url, body, size)
}

func (d *DataService) Logger() transfer.TransferLogger {
	return d.logger
}
