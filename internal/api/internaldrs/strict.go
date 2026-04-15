package internaldrs

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/calypr/syfon/apigen/internalapi"
	"github.com/calypr/syfon/config"
	"github.com/calypr/syfon/db/core"
	"github.com/calypr/syfon/internal/provider"
	"github.com/calypr/syfon/urlmanager"
)

func resolveFirstSignedURL(obj *core.InternalObject) (string, bool) {
	if obj.AccessMethods == nil {
		return "", false
	}
	for _, am := range *obj.AccessMethods {
		if am.AccessUrl == nil || am.AccessUrl.Url == "" {
			continue
		}
		u, err := url.Parse(am.AccessUrl.Url)
		if err != nil {
			continue
		}
		if provider.FromScheme(u.Scheme) != "" {
			return am.AccessUrl.Url, true
		}
	}
	return "", false
}

func (s *InternalServer) InternalDownload(ctx context.Context, request internalapi.InternalDownloadRequestObject) (internalapi.InternalDownloadResponseObject, error) {
	obj, err := s.database.GetObject(ctx, request.FileId)
	if err != nil {
		if errors.Is(err, core.ErrUnauthorized) {
			return internalapi.InternalDownload401Response{}, nil
		}
		if errors.Is(err, core.ErrNotFound) {
			return internalapi.InternalDownload404Response{}, nil
		}
		return nil, err
	}
	if !core.HasMethodAccess(ctx, "read", obj.Authorizations) {
		if core.IsGen3Mode(ctx) && !core.HasAuthHeader(ctx) {
			return internalapi.InternalDownload401Response{}, nil
		}
		return internalapi.InternalDownload403Response{}, nil
	}
	objectURL, ok := resolveFirstSignedURL(obj)
	if !ok {
		return internalapi.InternalDownload404Response{}, nil
	}
	opts := urlmanager.SignOptions{ExpiresIn: config.DefaultSigningExpirySeconds}
	if request.Params.ExpiresIn != nil {
		opts.ExpiresIn = *request.Params.ExpiresIn
	}
	bucketID := ""
	if parsed, parseErr := url.Parse(objectURL); parseErr == nil {
		bucketID = parsed.Host
	}
	signedURL, err := s.uM.SignURL(ctx, bucketID, objectURL, opts)
	if err != nil {
		return internalapi.InternalDownload500Response{}, err
	}
	if request.Params.Redirect != nil && *request.Params.Redirect {
		return internalapi.InternalDownload302Response{}, nil
	}
	return internalapi.InternalDownload200JSONResponse{Url: &signedURL}, nil
}

func (s *InternalServer) InternalDownloadPart(ctx context.Context, request internalapi.InternalDownloadPartRequestObject) (internalapi.InternalDownloadPartResponseObject, error) {
	obj, err := s.database.GetObject(ctx, request.FileId)
	if err != nil {
		if errors.Is(err, core.ErrUnauthorized) {
			return internalapi.InternalDownloadPart401Response{}, nil
		}
		if errors.Is(err, core.ErrNotFound) {
			return internalapi.InternalDownloadPart404Response{}, nil
		}
		return nil, err
	}
	if !core.HasMethodAccess(ctx, "read", obj.Authorizations) {
		if core.IsGen3Mode(ctx) && !core.HasAuthHeader(ctx) {
			return internalapi.InternalDownloadPart401Response{}, nil
		}
		return internalapi.InternalDownloadPart403Response{}, nil
	}
	objectURL, ok := resolveFirstSignedURL(obj)
	if !ok {
		return internalapi.InternalDownloadPart404Response{}, nil
	}
	bucketID := ""
	if parsed, parseErr := url.Parse(objectURL); parseErr == nil {
		bucketID = parsed.Host
	}
	opts := urlmanager.SignOptions{ExpiresIn: config.DefaultSigningExpirySeconds}
	start := request.Params.Start
	end := request.Params.End
	signedURL, err := s.uM.SignDownloadPart(ctx, bucketID, objectURL, start, end, opts)
	if err != nil {
		return internalapi.InternalDownloadPart500Response{}, err
	}
	return internalapi.InternalDownloadPart200JSONResponse{Url: &signedURL}, nil
}

func (s *InternalServer) InternalMultipartInit(ctx context.Context, request internalapi.InternalMultipartInitRequestObject) (internalapi.InternalMultipartInitResponseObject, error) {
	req := request.Body
	if req == nil {
		return internalapi.InternalMultipartInit400Response{}, nil
	}
	guid := ""
	if req.Guid != nil {
		guid = strings.TrimSpace(*req.Guid)
	}
	fileName := ""
	if req.FileName != nil {
		fileName = strings.TrimSpace(*req.FileName)
	}
	if guid == "" && fileName == "" {
		return internalapi.InternalMultipartInit400Response{}, nil
	}
	bucket, err := resolveBucket(nilToRequest(ctx), s.database, core.StringVal(req.Bucket))
	if err != nil || bucket == "" {
		return internalapi.InternalMultipartInit500Response{}, nil
	}
	return internalapi.InternalMultipartInit200JSONResponse{
		Guid:     &guid,
		UploadId: core.Ptr(""),
	}, nil
}

func (s *InternalServer) InternalMultipartUpload(ctx context.Context, request internalapi.InternalMultipartUploadRequestObject) (internalapi.InternalMultipartUploadResponseObject, error) {
	req := request.Body
	if req == nil {
		return internalapi.InternalMultipartUpload400Response{}, nil
	}
	return internalapi.InternalMultipartUpload200JSONResponse{PresignedUrl: core.Ptr("")}, nil
}

func (s *InternalServer) InternalMultipartComplete(ctx context.Context, request internalapi.InternalMultipartCompleteRequestObject) (internalapi.InternalMultipartCompleteResponseObject, error) {
	req := request.Body
	if req == nil {
		return internalapi.InternalMultipartComplete400Response{}, nil
	}
	if req.UploadId == "" || req.Key == "" {
		return internalapi.InternalMultipartComplete400Response{}, nil
	}
	return internalapi.InternalMultipartComplete200Response{}, nil
}

func nilToRequest(ctx context.Context) *http.Request {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "/", nil)
	return req
}

func authStatusCodeForContext(ctx context.Context) int {
	if core.IsGen3Mode(ctx) && !core.HasAuthHeader(ctx) {
		return http.StatusUnauthorized
	}
	return http.StatusForbidden
}
