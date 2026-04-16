package service

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/internal/db/core"
	"github.com/calypr/syfon/internal/urlmanager"
)

func (s *ObjectsAPIService) PostUploadRequest(ctx context.Context, uploadRequest drs.UploadRequest) (ImplResponse, error) {
	targetResources := []string{"/data_file"}
	if core.IsGen3Mode(ctx) && !core.HasMethodAccess(ctx, "file_upload", targetResources) && !core.HasMethodAccess(ctx, "create", targetResources) {
		code := unauthorizedStatus(ctx)
		return ImplResponse{
			Code: code,
			Body: drsError("forbidden: missing file_upload/create permission on /data_file", code),
		}, nil
	}

	creds, err := s.db.ListS3Credentials(ctx)
	if err != nil {
		return ImplResponse{Code: http.StatusInternalServerError, Body: drsError(err.Error(), http.StatusInternalServerError)}, err
	}
	if len(creds) == 0 {
		return ImplResponse{
			Code: http.StatusInternalServerError,
			Body: drsError("no bucket credentials configured for upload", http.StatusInternalServerError),
		}, nil
	}

	credByBucket := make(map[string]core.S3Credential, len(creds))
	for _, cred := range creds {
		b := strings.TrimSpace(cred.Bucket)
		if b == "" {
			continue
		}
		credByBucket[b] = cred
	}
	if len(credByBucket) == 0 {
		return ImplResponse{
			Code: http.StatusInternalServerError,
			Body: drsError("no bucket credentials configured for upload", http.StatusInternalServerError),
		}, nil
	}

	// Pick fallback credential deterministically to avoid map-order instability.
	selected := core.S3Credential{}
	keys := make([]string, 0, len(credByBucket))
	for b := range credByBucket {
		keys = append(keys, b)
	}
	sort.Strings(keys)
	if len(keys) > 0 {
		selected = credByBucket[keys[0]]
	}

	if scopes, scopeErr := s.db.ListBucketScopes(ctx); scopeErr == nil {
		for _, scope := range scopes {
			resource := core.ResourcePathForScope(scope.Organization, scope.ProjectID)
			if resource == "" {
				continue
			}
			if !core.HasMethodAccess(ctx, "file_upload", []string{resource}) &&
				!core.HasMethodAccess(ctx, "create", []string{resource}) &&
				!core.HasMethodAccess(ctx, "update", []string{resource}) {
				continue
			}
			if scopedCred, ok := credByBucket[strings.TrimSpace(scope.Bucket)]; ok {
				selected = scopedCred
				break
			}
		}
	}

	bucket := strings.TrimSpace(selected.Bucket)
	region := strings.TrimSpace(selected.Region)
	if region == "" {
		region = "us-east-1"
	}

	out := drs.UploadResponse{
		Responses: make([]drs.UploadResponseObject, 0, len(uploadRequest.Requests)),
	}
	for i, req := range uploadRequest.Requests {
		key := uploadObjectKey(req, i)
		s3URL := fmt.Sprintf("s3://%s/%s", bucket, key)
		signedURL, signErr := s.urlManager.SignUploadURL(ctx, "", s3URL, urlmanager.SignOptions{})
		if signErr != nil {
			return ImplResponse{
				Code: http.StatusInternalServerError,
				Body: drsError(signErr.Error(), http.StatusInternalServerError),
			}, signErr
		}

		respObj := drs.UploadResponseObject{
			Name:        req.Name,
			Size:        req.Size,
			MimeType:    req.MimeType,
			Checksums:   req.Checksums,
			Description: req.Description,
			Aliases:     req.Aliases,
			UploadMethods: &[]drs.UploadMethod{
				{
					Type: drs.UploadMethodType("s3"),
					AccessUrl: struct {
						Headers *[]string `json:"headers,omitempty"`
						Url     string    `json:"url"`
					}{Url: signedURL},
					Region: &region,
					UploadDetails: func() *map[string]interface{} {
						details := map[string]interface{}{
							"bucket": bucket,
							"key":    key,
						}
						return &details
					}(),
				},
			},
		}
		out.Responses = append(out.Responses, respObj)
	}

	return ImplResponse{Code: http.StatusOK, Body: out}, nil
}

func uploadObjectKey(req drs.UploadRequestObject, index int) string {
	// Simple key generation for now.
	// In a real system, this would likely include organization/project path prefix from bucket_scope.
	return fmt.Sprintf("uploads/%s/%s", time.Now().Format("2006/01/02"), req.Name)
}
