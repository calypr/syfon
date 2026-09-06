package lfs

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/server/lfsapi"
	syfoncommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/faults"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/storage"
	"github.com/calypr/syfon/internal/transfers"
	"github.com/calypr/syfon/internal/usage"
)

var errNoBucketConfigured = errors.New("no bucket configured")

type LFSServer struct {
	objectService    *objects.Service
	transferService  *transfers.Service
	fileCounters     usage.FileCounterRecorder
	credentials      buckets.CredentialReader
	opts             Options
	uploadWorkflow   *transfers.LFSUploadWorkflow
	metadataWorkflow *transfers.LFSMetadataWorkflow
}

func NewLFSServer(deps Dependencies, opts Options) *LFSServer {
	partUploader := deps.PartUploader
	if partUploader == nil {
		partUploader = storage.UploadSignedMultipartPart
	}
	return &LFSServer{
		objectService:    deps.ObjectService,
		transferService:  deps.TransferService,
		fileCounters:     deps.FileCounters,
		credentials:      deps.Credentials,
		opts:             opts,
		uploadWorkflow:   transfers.NewLFSUploadWorkflow(deps.TransferService, transfers.LFSUploadPartUploader(partUploader), deps.FileCounters),
		metadataWorkflow: transfers.NewLFSMetadataWorkflow(deps.TransferService, deps.ObjectService, deps.FileCounters),
	}
}

func (s *LFSServer) LfsBatch(ctx context.Context, request lfsapi.LfsBatchRequestObject) (lfsapi.LfsBatchResponseObject, error) {
	req := request.Body
	if req == nil {
		return lfsapi.LfsBatch500ApplicationVndGitLfsPlusJSONResponse{Message: "missing request body"}, nil
	}
	req.Operation = lfsapi.BatchRequestOperation(strings.ToLower(strings.TrimSpace(string(req.Operation))))
	if req.Operation != "download" && req.Operation != "upload" {
		return lfsapi.LfsBatch422ApplicationVndGitLfsPlusJSONResponse{Message: "operation must be 'download' or 'upload'"}, nil
	}
	if len(req.Objects) == 0 {
		return lfsapi.LfsBatch422ApplicationVndGitLfsPlusJSONResponse{Message: "objects cannot be empty"}, nil
	}
	if s.opts.MaxBatchObjects > 0 && len(req.Objects) > s.opts.MaxBatchObjects {
		return lfsapi.LfsBatch413ApplicationVndGitLfsPlusJSONResponse{Message: "batch contains too many objects"}, nil
	}

	transfer := "basic"
	responseObjects := make([]lfsapi.BatchResponseObject, 0, len(req.Objects))
	hashAlgorithm := "sha256"
	for _, input := range req.Objects {
		objectResponse := lfsapi.BatchResponseObject{Oid: input.Oid, Size: input.Size}
		oid := syfoncommon.NormalizeOid(input.Oid)
		if oid == "" {
			objectResponse.Error = &lfsapi.ObjectError{Code: int32(http.StatusBadRequest), Message: "invalid oid"}
			responseObjects = append(responseObjects, objectResponse)
			continue
		}
		objectResponse.Oid = oid
		if req.Operation == "download" {
			actions, objectError := prepareDownloadActions(ctx, s.objectService, s.transferService, s.fileCounters, oid)
			if objectError != nil {
				objectResponse.Error = objectError
			} else {
				objectResponse.Actions = actions
			}
		} else {
			actions, size, objectError := prepareUploadActions(ctx, s.objectService, s.credentials, input.Size, oid, GetBaseURL(ctx))
			objectResponse.Size = size
			if objectError != nil {
				objectResponse.Error = objectError
			} else {
				objectResponse.Actions = actions
			}
		}
		responseObjects = append(responseObjects, objectResponse)
	}

	return lfsapi.LfsBatch200ApplicationVndGitLfsPlusJSONResponse{
		Transfer: &transfer,
		Objects:  responseObjects,
		HashAlgo: &hashAlgorithm,
	}, nil
}

func (s *LFSServer) LfsVerify(ctx context.Context, request lfsapi.LfsVerifyRequestObject) (lfsapi.LfsVerifyResponseObject, error) {
	if request.Body == nil {
		return lfsapi.LfsVerify400ApplicationVndGitLfsPlusJSONResponse{Message: "missing request body"}, nil
	}
	oid := syfoncommon.NormalizeOid(request.Body.Oid)
	if oid == "" {
		return lfsapi.LfsVerify400ApplicationVndGitLfsPlusJSONResponse{Message: "invalid oid"}, nil
	}

	if err := s.metadataWorkflow.Verify(ctx, oid); err != nil {
		var candidateErr *transfers.LFSMetadataCandidateError
		if errors.As(err, &candidateErr) {
			return lfsapi.LfsVerify400ApplicationVndGitLfsPlusJSONResponse{Message: err.Error()}, nil
		}
		if faults.IsNotFoundError(err) {
			return lfsapi.LfsVerify404ApplicationVndGitLfsPlusJSONResponse{Message: "Object not found"}, nil
		}
		return lfsapi.LfsVerify500ApplicationVndGitLfsPlusJSONResponse{Message: err.Error()}, nil
	}
	return lfsapi.LfsVerify200Response{}, nil
}

func (s *LFSServer) LfsStageMetadata(ctx context.Context, request lfsapi.LfsStageMetadataRequestObject) (lfsapi.LfsStageMetadataResponseObject, error) {
	var input *lfsapi.MetadataSubmitRequest
	if request.JSONBody != nil {
		input = request.JSONBody
	} else if request.ApplicationVndGitLfsPlusJSONBody != nil {
		input = request.ApplicationVndGitLfsPlusJSONBody
	}
	if input == nil || len(input.Candidates) == 0 {
		return lfsapi.LfsStageMetadata400JSONResponse{Message: "candidates cannot be empty"}, nil
	}

	now := time.Now().UTC()
	entries := make([]transfers.PendingMetadata, 0, len(input.Candidates))
	for index, candidate := range input.Candidates {
		domainCandidate := FromGeneratedCandidate(candidate)
		internalObject, err := objects.CandidateToRecord(domainCandidate, now)
		if err != nil {
			return lfsapi.LfsStageMetadata400JSONResponse{Message: fmt.Sprintf("candidate[%d] invalid: %v", index, err)}, nil
		}
		oid, ok := objects.CanonicalSHA256(internalObject.Checksums)
		if !ok {
			return lfsapi.LfsStageMetadata400JSONResponse{Message: fmt.Sprintf("candidate[%d] missing canonical sha256", index)}, nil
		}
		entries = append(entries, transfers.PendingMetadata{
			OID:       oid,
			Candidate: domainCandidate,
			CreatedAt: now,
			ExpiresAt: now.Add(transfers.PendingMetadataTTL),
		})
	}
	if err := s.transferService.SavePendingLFSMeta(ctx, entries); err != nil {
		return lfsapi.LfsStageMetadata500JSONResponse{Message: err.Error()}, nil
	}
	return lfsapi.LfsStageMetadata200JSONResponse{Staged: int32(len(entries))}, nil
}

func (s *LFSServer) LfsUploadProxy(ctx context.Context, request lfsapi.LfsUploadProxyRequestObject) (lfsapi.LfsUploadProxyResponseObject, error) {
	oid := syfoncommon.NormalizeOid(request.Oid)
	if oid == "" {
		return lfsapi.LfsUploadProxy400TextResponse("invalid oid"), nil
	}
	bucket, key, objectID, err := s.resolveUploadProxyTarget(ctx, oid)
	if err != nil {
		if errors.Is(err, errNoBucketConfigured) {
			return lfsapi.LfsUploadProxy507TextResponse(err.Error()), nil
		}
		return lfsapi.LfsUploadProxy500TextResponse(err.Error()), nil
	}
	if err := s.uploadWorkflow.Upload(ctx, request.Body, bucket, key, objectID); err != nil {
		return lfsapi.LfsUploadProxy500TextResponse(err.Error()), nil
	}
	return lfsapi.LfsUploadProxy200Response{}, nil
}

func (s *LFSServer) resolveUploadProxyTarget(ctx context.Context, oid string) (string, string, string, error) {
	if s.credentials == nil {
		return "", "", "", errNoBucketConfigured
	}
	credentials, err := s.credentials.ListS3Credentials(ctx)
	if err != nil {
		return "", "", "", err
	}
	if len(credentials) == 0 || strings.TrimSpace(credentials[0].Bucket) == "" {
		return "", "", "", errNoBucketConfigured
	}
	defaultBucket := strings.TrimSpace(credentials[0].Bucket)
	if object, getErr := s.objectService.GetObject(ctx, oid, "read"); getErr == nil {
		target, targetErr := s.transferService.ResolveCanonicalStorageTarget(ctx, transfers.CanonicalStorageTargetRequest{
			Object:         object,
			PreferChecksum: true,
		})
		if targetErr != nil {
			return "", "", "", targetErr
		}
		if target.Bucket == "" || target.Key == "" {
			return "", "", "", fmt.Errorf("canonical LFS upload location is not an s3 url")
		}
		return target.Bucket, target.Key, string(object.Id), nil
	} else if !faults.IsNotFoundError(getErr) {
		return "", "", "", getErr
	}

	if pending, getErr := s.transferService.GetPendingLFSMeta(ctx, oid); getErr == nil {
		object, conversionErr := objects.CandidateToRecord(pending.Candidate, time.Now().UTC())
		if conversionErr != nil {
			return "", "", "", conversionErr
		}
		target, targetErr := s.transferService.ResolveCanonicalStorageTarget(ctx, transfers.CanonicalStorageTargetRequest{
			Object:         &object,
			PreferChecksum: true,
		})
		if targetErr != nil {
			return "", "", "", targetErr
		}
		if target.Bucket == "" || target.Key == "" {
			return "", "", "", fmt.Errorf("canonical LFS upload location is not an s3 url")
		}
		return target.Bucket, target.Key, string(object.Id), nil
	} else if !faults.IsNotFoundError(getErr) {
		return "", "", "", getErr
	}
	return defaultBucket, oid, oid, nil
}
