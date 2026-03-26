package service

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/calypr/drs-server/apigen/drs"
	"github.com/calypr/drs-server/db/core"
	"github.com/calypr/drs-server/urlmanager"
)

func (s *ObjectsAPIService) PostUploadRequest(ctx context.Context, uploadRequest drs.UploadRequest) (drs.ImplResponse, error) {
	targetResources := []string{"/data_file"}
	if core.IsGen3Mode(ctx) && !core.HasMethodAccess(ctx, "file_upload", targetResources) && !core.HasMethodAccess(ctx, "create", targetResources) {
		code := unauthorizedStatus(ctx)
		return drs.ImplResponse{
			Code: code,
			Body: drs.Error{Msg: "forbidden: missing file_upload/create permission on /data_file", StatusCode: int32(code)},
		}, nil
	}

	creds, err := s.db.ListS3Credentials(ctx)
	if err != nil {
		return drs.ImplResponse{Code: http.StatusInternalServerError, Body: drs.Error{Msg: err.Error(), StatusCode: http.StatusInternalServerError}}, err
	}
	if len(creds) == 0 {
		return drs.ImplResponse{
			Code: http.StatusInternalServerError,
			Body: drs.Error{Msg: "no bucket credentials configured for upload", StatusCode: http.StatusInternalServerError},
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
		return drs.ImplResponse{
			Code: http.StatusInternalServerError,
			Body: drs.Error{Msg: "no bucket credentials configured for upload", StatusCode: http.StatusInternalServerError},
		}, nil
	}

	selected := creds[0]
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
			return drs.ImplResponse{
				Code: http.StatusInternalServerError,
				Body: drs.Error{Msg: signErr.Error(), StatusCode: http.StatusInternalServerError},
			}, signErr
		}

		respObj := drs.UploadResponseObject{
			Name:        req.Name,
			Size:        req.Size,
			MimeType:    req.MimeType,
			Checksums:   req.Checksums,
			Description: req.Description,
			Aliases:     req.Aliases,
			UploadMethods: []drs.UploadMethod{
				{
					Type:      "s3",
					AccessUrl: drs.UploadMethodAccessUrl{Url: signedURL},
					Region:    region,
					UploadDetails: map[string]interface{}{
						"bucket": bucket,
						"key":    key,
					},
				},
			},
		}
		out.Responses = append(out.Responses, respObj)
	}

	return drs.ImplResponse{Code: http.StatusOK, Body: out}, nil
}

func uploadObjectKey(req drs.UploadRequestObject, index int) string {
	// Simple key generation for now.
	// In a real system, this would likely include organization/project path prefix from bucket_scope.
	return fmt.Sprintf("uploads/%s/%s", time.Now().Format("2006/01/02"), req.Name)
}
