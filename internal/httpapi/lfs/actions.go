package lfs

import (
	"context"
	"net/http"
	"strings"

	"github.com/calypr/syfon/apigen/server/lfsapi"
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/faults"
	apimiddleware "github.com/calypr/syfon/internal/httpapi/middleware"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/storage"
	"github.com/calypr/syfon/internal/transfers"
	"github.com/calypr/syfon/internal/usage"
)

func prepareDownloadActions(ctx context.Context, objectService *objects.Service, transferService *transfers.Service, counters usage.FileCounterRecorder, oid string) (*lfsapi.BatchActions, *lfsapi.ObjectError) {
	object, err := objectService.GetObject(ctx, oid, "read")
	if err != nil {
		return nil, dbErrToBatchError(ctx, err)
	}

	var sourceURL, accessID string
	if object.AccessMethods != nil {
		for _, method := range *object.AccessMethods {
			if method.AccessUrl == nil || strings.TrimSpace(method.AccessUrl.Url) == "" {
				continue
			}
			sourceURL = method.AccessUrl.Url
			if method.AccessId != nil && strings.TrimSpace(*method.AccessId) != "" {
				accessID = strings.TrimSpace(*method.AccessId)
			} else {
				accessID = strings.TrimSpace(method.Type)
			}
			break
		}
	}
	if sourceURL == "" {
		return nil, &lfsapi.ObjectError{Code: int32(http.StatusNotFound), Message: "no object location available"}
	}

	signedURL, err := transferService.SignObjectURL(ctx, object, sourceURL, storage.AccessOptions{})
	if err != nil {
		return nil, &lfsapi.ObjectError{Code: int32(http.StatusInternalServerError), Message: err.Error()}
	}
	if counters == nil {
		return nil, &lfsapi.ObjectError{Code: int32(http.StatusInternalServerError), Message: "file counters are not configured"}
	}
	if err := counters.RecordFileDownload(ctx, oid); err != nil {
		return nil, &lfsapi.ObjectError{Code: int32(http.StatusInternalServerError), Message: err.Error()}
	}
	if err := transferService.RecordAccessIssued(ctx, transfers.AccessRequest{
		Object:     object,
		AccessID:   accessID,
		Direction:  usage.ProviderTransferDirectionDownload,
		StorageURL: sourceURL,
	}); err != nil {
		return nil, &lfsapi.ObjectError{Code: int32(http.StatusInternalServerError), Message: err.Error()}
	}
	return &lfsapi.BatchActions{Download: &lfsapi.Action{Href: signedURL}}, nil
}

func prepareUploadActions(ctx context.Context, objectService *objects.Service, credentials buckets.CredentialReader, reqSize int64, oid, baseURL string) (*lfsapi.BatchActions, int64, *lfsapi.ObjectError) {
	existing, err := objectService.GetObject(ctx, oid, "read")
	if err == nil {
		return nil, existing.Size, nil
	}
	if !faults.IsNotFoundError(err) {
		return nil, reqSize, dbErrToBatchError(ctx, err)
	}
	if err := objectService.RequireObjectResources(ctx, "create", []string{"/data_file"}); err != nil {
		return nil, reqSize, dbErrToBatchError(ctx, err)
	}
	if credentials == nil {
		return nil, reqSize, &lfsapi.ObjectError{Code: int32(http.StatusInsufficientStorage), Message: "no bucket configured"}
	}
	configured, err := credentials.ListS3Credentials(ctx)
	if err != nil || len(configured) == 0 || strings.TrimSpace(configured[0].Bucket) == "" {
		return nil, reqSize, &lfsapi.ObjectError{Code: int32(http.StatusInsufficientStorage), Message: "no bucket configured"}
	}
	size := reqSize
	if size < 0 {
		size = 0
	}
	return &lfsapi.BatchActions{
		Upload: &lfsapi.Action{Href: baseURL + "/info/lfs/objects/" + oid},
		Verify: &lfsapi.Action{Href: baseURL + "/info/lfs/verify"},
	}, size, nil
}

func dbErrToBatchError(ctx context.Context, err error) *lfsapi.ObjectError {
	if faults.IsNotFoundError(err) {
		return &lfsapi.ObjectError{Code: int32(http.StatusNotFound), Message: "object not found"}
	}
	if err == faults.ErrUnauthorized {
		return &lfsapi.ObjectError{Code: int32(apimiddleware.AuthFailureStatus(ctx)), Message: "unauthorized"}
	}
	return &lfsapi.ObjectError{Code: int32(http.StatusInternalServerError), Message: err.Error()}
}
