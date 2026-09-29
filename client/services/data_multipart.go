package services

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	internalapi "github.com/calypr/syfon/apigen/internalapi"
	"github.com/calypr/syfon/client/apierror"
	"github.com/calypr/syfon/client/common"
	"github.com/calypr/syfon/client/transfer"
)

var _ interface {
	MultipartAbort(context.Context, string) error
	MultipartCompletionReplaySafe(context.Context, string, string, []transfer.MultipartPart) bool
} = (*DataService)(nil)

func (s *DataService) multipartInitRequest(ctx context.Context, req internalapi.InternalMultipartInitRequest) (internalapi.InternalMultipartInitOutput, error) {
	resp, err := s.gen.InternalMultipartInitWithResponse(ctx, internalapi.InternalMultipartInitJSONRequestBody(req))
	if err != nil {
		return internalapi.InternalMultipartInitOutput{}, err
	}
	if resp.JSON200 == nil {
		return internalapi.InternalMultipartInitOutput{}, apierror.FromResponse(resp.HTTPResponse, resp.Body)
	}
	return *resp.JSON200, nil
}

func (d *DataService) multipartUploadRequest(ctx context.Context, req internalapi.InternalMultipartUploadRequest) (internalapi.InternalMultipartUploadOutput, error) {
	resp, err := d.gen.InternalMultipartUploadWithResponse(ctx, internalapi.InternalMultipartUploadJSONRequestBody(req))
	if err != nil {
		return internalapi.InternalMultipartUploadOutput{}, err
	}
	if resp.JSON200 == nil {
		return internalapi.InternalMultipartUploadOutput{}, apierror.FromResponse(resp.HTTPResponse, resp.Body)
	}
	return *resp.JSON200, nil
}

func (d *DataService) multipartCompleteRequest(ctx context.Context, req internalapi.InternalMultipartCompleteRequest) (string, error) {
	resp, err := d.gen.InternalMultipartCompleteWithResponse(ctx, internalapi.InternalMultipartCompleteJSONRequestBody(req))
	if err != nil {
		return "", err
	}
	if resp.StatusCode() != http.StatusOK && resp.StatusCode() != http.StatusCreated {
		return "", apierror.FromResponse(resp.HTTPResponse, resp.Body)
	}
	// Read the optional extension from the body so the standalone client also
	// builds against published bindings from before completion returned a URL.
	if strings.Contains(resp.HTTPResponse.Header.Get("Content-Type"), "json") && len(bytes.TrimSpace(resp.Body)) > 0 {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(resp.Body, &fields); err != nil {
			return "", fmt.Errorf("decode multipart completion: %w", err)
		}
		if raw, ok := fields["object_url"]; ok {
			var location string
			if err := json.Unmarshal(raw, &location); err != nil {
				return "", fmt.Errorf("decode completed object location: %w", err)
			}
			return location, nil
		}
	}
	return "", nil
}

func (d *DataService) InitMultipartUploadWithMetadata(ctx context.Context, guid, filename, bucket string, metadata common.FileMetadata) (string, string, error) {
	organization, project, err := uploadScopeFromMetadata(metadata)
	if err != nil {
		return "", "", err
	}
	req := internalapi.InternalMultipartInitRequest{
		Guid:         &guid,
		Key:          &filename,
		Organization: nil,
		Project:      nil,
	}
	if organization != "" {
		req.Organization = &organization
	}
	if project != "" {
		req.Project = &project
	}
	resp, err := d.multipartInitRequest(ctx, req)
	if err != nil {
		return "", "", err
	}
	uploadID := ""
	if resp.UploadId != nil {
		uploadID = *resp.UploadId
	}
	respGuid := ""
	if resp.Guid != nil {
		respGuid = *resp.Guid
	}
	return uploadID, respGuid, nil
}

func (d *DataService) MultipartInit(ctx context.Context, guid string) (string, error) {
	uploadID, _, err := d.InitMultipartUploadWithMetadata(ctx, guid, "", "", common.FileMetadata{})
	return uploadID, err
}

func (d *DataService) MultipartPart(ctx context.Context, guid string, uploadID string, partNum int, body io.Reader) (string, error) {
	bucket := ""
	resp, err := d.multipartUploadRequest(ctx, internalapi.InternalMultipartUploadRequest{
		Key:        guid,
		UploadId:   uploadID,
		PartNumber: int32(partNum),
		Bucket:     &bucket,
	})
	if err != nil {
		return "", err
	}
	if resp.PresignedUrl == nil {
		return "", fmt.Errorf("response missing presigned URL")
	}
	url := *resp.PresignedUrl
	if sized, ok := body.(interface{ Size() int64 }); ok {
		return d.UploadPart(ctx, url, body, sized.Size())
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}
	return d.UploadPart(ctx, url, bytes.NewReader(data), int64(len(data)))
}

type multipartPartURLsRequest struct {
	Key         string  `json:"key"`
	UploadID    string  `json:"uploadId"`
	PartNumber  int32   `json:"partNumber"`
	PartNumbers []int32 `json:"partNumbers,omitempty"`
}

type multipartPartURLResponse struct {
	PresignedURL *string         `json:"presigned_url,omitempty"`
	Parts        json.RawMessage `json:"parts"`
}

type multipartPartURLItem struct {
	PartNumber   int32           `json:"partNumber"`
	PresignedURL string          `json:"presigned_url"`
	ExpiresIn    json.RawMessage `json:"expires_in"`
}

const maxMultipartPartExpirySeconds = int64((1<<63 - 1) / int64(time.Second))

func (d *DataService) MultipartPartURLs(ctx context.Context, guid string, uploadID string, partNumbers []int32) (transfer.MultipartPartURLBatch, error) {
	if len(partNumbers) == 0 {
		return transfer.MultipartPartURLBatch{}, fmt.Errorf("multipart part numbers are required")
	}
	if len(partNumbers) > 32 {
		return transfer.MultipartPartURLBatch{}, fmt.Errorf("multipart part batch exceeds 32 parts")
	}
	requested := make(map[int32]struct{}, len(partNumbers))
	for _, partNumber := range partNumbers {
		if partNumber <= 0 {
			return transfer.MultipartPartURLBatch{}, fmt.Errorf("multipart part number must be positive")
		}
		if _, exists := requested[partNumber]; exists {
			return transfer.MultipartPartURLBatch{}, fmt.Errorf("multipart part number %d is duplicated", partNumber)
		}
		requested[partNumber] = struct{}{}
	}
	payload, err := json.Marshal(multipartPartURLsRequest{
		Key: guid, UploadID: uploadID, PartNumber: partNumbers[0], PartNumbers: partNumbers,
	})
	if err != nil {
		return transfer.MultipartPartURLBatch{}, fmt.Errorf("encode multipart part batch: %w", err)
	}
	requestStarted := time.Now()
	resp, err := d.gen.InternalMultipartUploadWithBodyWithResponse(ctx, common.MIMEApplicationJSON, bytes.NewReader(payload))
	if err != nil {
		return transfer.MultipartPartURLBatch{}, err
	}
	if resp.StatusCode() != http.StatusOK {
		return transfer.MultipartPartURLBatch{}, apierror.FromResponse(resp.HTTPResponse, resp.Body)
	}
	var output multipartPartURLResponse
	if err := json.Unmarshal(resp.Body, &output); err != nil {
		return transfer.MultipartPartURLBatch{}, fmt.Errorf("decode multipart part batch: %w", err)
	}
	if len(bytes.TrimSpace(output.Parts)) > 0 {
		var provided []multipartPartURLItem
		if err := json.Unmarshal(output.Parts, &provided); err != nil {
			return transfer.MultipartPartURLBatch{}, fmt.Errorf("decode multipart part URLs: %w", err)
		}
		if len(provided) != len(partNumbers) {
			return transfer.MultipartPartURLBatch{}, fmt.Errorf("multipart part batch returned %d of %d parts", len(provided), len(partNumbers))
		}
		byPart := make(map[int32]multipartPartURLItem, len(provided))
		expirySeconds := make(map[int32]int64, len(provided))
		knownExpiry := make(map[int32]bool, len(provided))
		for _, part := range provided {
			if _, ok := requested[part.PartNumber]; !ok {
				return transfer.MultipartPartURLBatch{}, fmt.Errorf("multipart part batch returned unexpected part %d", part.PartNumber)
			}
			if _, exists := byPart[part.PartNumber]; exists {
				return transfer.MultipartPartURLBatch{}, fmt.Errorf("multipart part batch returned duplicate part %d", part.PartNumber)
			}
			if strings.TrimSpace(part.PresignedURL) == "" {
				return transfer.MultipartPartURLBatch{}, fmt.Errorf("multipart part batch returned empty URL for part %d", part.PartNumber)
			}
			if rawExpiry := bytes.TrimSpace(part.ExpiresIn); len(rawExpiry) > 0 {
				if bytes.Equal(rawExpiry, []byte("null")) {
					return transfer.MultipartPartURLBatch{}, fmt.Errorf("multipart part %d has null expires_in", part.PartNumber)
				}
				var seconds int64
				if err := json.Unmarshal(rawExpiry, &seconds); err != nil {
					return transfer.MultipartPartURLBatch{}, fmt.Errorf("multipart part %d has invalid expires_in: %w", part.PartNumber, err)
				}
				if seconds < 0 || seconds > maxMultipartPartExpirySeconds {
					return transfer.MultipartPartURLBatch{}, fmt.Errorf("multipart part %d has invalid expires_in %d", part.PartNumber, seconds)
				}
				expirySeconds[part.PartNumber] = seconds
				knownExpiry[part.PartNumber] = true
			}
			byPart[part.PartNumber] = part
		}
		parts := make([]transfer.MultipartPartURL, len(partNumbers))
		allHaveExpiry := true
		for i, partNumber := range partNumbers {
			part, ok := byPart[partNumber]
			if !ok {
				return transfer.MultipartPartURLBatch{}, fmt.Errorf("multipart part batch omitted part %d", partNumber)
			}
			parts[i] = transfer.MultipartPartURL{PartNumber: partNumber, URL: part.PresignedURL}
			if !knownExpiry[partNumber] {
				allHaveExpiry = false
				continue
			}
			parts[i].ExpiresAt = requestStarted.Add(time.Duration(expirySeconds[partNumber]) * time.Second)
		}
		if !allHaveExpiry {
			first := parts[0]
			first.ExpiresAt = requestStarted
			return transfer.MultipartPartURLBatch{
				Parts: []transfer.MultipartPartURL{first}, BatchSupported: false,
			}, nil
		}
		return transfer.MultipartPartURLBatch{Parts: parts, BatchSupported: true}, nil
	}
	if output.PresignedURL == nil || strings.TrimSpace(*output.PresignedURL) == "" {
		return transfer.MultipartPartURLBatch{}, fmt.Errorf("multipart response missing presigned URL")
	}
	return transfer.MultipartPartURLBatch{
		Parts:          []transfer.MultipartPartURL{{PartNumber: partNumbers[0], URL: *output.PresignedURL}},
		BatchSupported: false,
	}, nil
}

func (d *DataService) MultipartComplete(ctx context.Context, guid string, uploadID string, parts []transfer.MultipartPart) error {
	_, err := d.MultipartCompleteWithLocation(ctx, guid, uploadID, parts)
	return err
}

// MultipartAbort releases a server-side multipart session. It is safe to
// repeat and does not run automatically for transient upload failures, so
// callers retain resumability until they explicitly abandon an upload.
func (d *DataService) MultipartAbort(ctx context.Context, uploadID string) error {
	resp, err := d.gen.InternalMultipartAbortWithResponse(ctx, internalapi.InternalMultipartAbortJSONRequestBody{UploadId: uploadID})
	if err != nil {
		return err
	}
	if resp.StatusCode() != http.StatusNoContent {
		return apierror.FromResponse(resp.HTTPResponse, resp.Body)
	}
	return nil
}

// MultipartCompletionReplaySafe reports that the server makes completion retries idempotent.
func (d *DataService) MultipartCompletionReplaySafe(context.Context, string, string, []transfer.MultipartPart) bool {
	return true
}

// MultipartCompleteWithLocation returns the storage location selected by the upload session.
func (d *DataService) MultipartCompleteWithLocation(ctx context.Context, guid string, uploadID string, parts []transfer.MultipartPart) (string, error) {
	reqParts := make([]internalapi.InternalMultipartPart, 0, len(parts))
	for _, p := range parts {
		reqParts = append(reqParts, internalapi.InternalMultipartPart{
			PartNumber: p.PartNumber,
			ETag:       p.ETag,
		})
	}
	return d.multipartCompleteRequest(ctx, internalapi.InternalMultipartCompleteRequest{
		Key:      guid,
		UploadId: uploadID,
		Parts:    reqParts,
	})
}
