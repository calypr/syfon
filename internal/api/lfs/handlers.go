package lfs

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/server/lfsapi"
	sycommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/models"
	"github.com/calypr/syfon/internal/urlmanager"
)

type LFSServer struct {
	om   *core.ObjectManager
	opts Options
}

func NewLFSServer(om *core.ObjectManager, opts Options) *LFSServer {
	return &LFSServer{
		om:   om,
		opts: opts,
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
	respObjects := make([]lfsapi.BatchResponseObject, 0, len(req.Objects))
	hashAlgo := "sha256"

	for _, in := range req.Objects {
		objResp := lfsapi.BatchResponseObject{Oid: in.Oid, Size: in.Size}
		oid := sycommon.NormalizeOid(in.Oid)
		if oid == "" {
			objResp.Error = &lfsapi.ObjectError{Code: int32(http.StatusBadRequest), Message: "invalid oid"}
			respObjects = append(respObjects, objResp)
			continue
		}
		objResp.Oid = oid

		switch req.Operation {
		case "download":
			actions, errResp := prepareDownloadActions(ctx, s.om, oid)
			if errResp != nil {
				objResp.Error = errResp
			} else {
				objResp.Actions = actions
			}
		case "upload":
			baseURL := core.GetBaseURL(ctx)
			actions, size, errResp := prepareUploadActions(ctx, s.om, oid, in.Size, baseURL)
			objResp.Size = size
			if errResp != nil {
				objResp.Error = errResp
			} else {
				objResp.Actions = actions
			}
		}
		respObjects = append(respObjects, objResp)
	}

	return lfsapi.LfsBatch200ApplicationVndGitLfsPlusJSONResponse{
		Transfer: &transfer,
		Objects:  respObjects,
		HashAlgo: &hashAlgo,
	}, nil
}

func (s *LFSServer) LfsVerify(ctx context.Context, request lfsapi.LfsVerifyRequestObject) (lfsapi.LfsVerifyResponseObject, error) {
	req := request.Body
	if req == nil {
		return lfsapi.LfsVerify400ApplicationVndGitLfsPlusJSONResponse{Message: "missing request body"}, nil
	}
	oid := sycommon.NormalizeOid(req.Oid)
	if oid == "" {
		return lfsapi.LfsVerify400ApplicationVndGitLfsPlusJSONResponse{Message: "invalid oid"}, nil
	}

	obj, err := s.om.GetObject(ctx, oid, "read")
	if err == nil {
		_ = s.om.RecordUpload(ctx, obj.Id)
		return lfsapi.LfsVerify200Response{}, nil
	}

	if !common.IsNotFoundError(err) {
		return lfsapi.LfsVerify500ApplicationVndGitLfsPlusJSONResponse{Message: err.Error()}, nil
	}

	pending, err := s.om.PopPendingLFSMeta(ctx, oid)
	if err != nil {
		if common.IsNotFoundError(err) {
			return lfsapi.LfsVerify404ApplicationVndGitLfsPlusJSONResponse{Message: "Object not found"}, nil
		}
		return lfsapi.LfsVerify500ApplicationVndGitLfsPlusJSONResponse{Message: err.Error()}, nil
	}

	internalObj, err := core.CandidateToInternalObject(pending.Candidate, time.Now().UTC())
	if err != nil {
		return lfsapi.LfsVerify400ApplicationVndGitLfsPlusJSONResponse{Message: err.Error()}, nil
	}

	if err := s.om.RegisterObjects(ctx, []models.InternalObject{internalObj}); err != nil {
		return lfsapi.LfsVerify500ApplicationVndGitLfsPlusJSONResponse{Message: err.Error()}, nil
	}

	_ = s.om.RecordUpload(ctx, internalObj.Id)
	return lfsapi.LfsVerify200Response{}, nil
}

func (s *LFSServer) LfsStageMetadata(ctx context.Context, request lfsapi.LfsStageMetadataRequestObject) (lfsapi.LfsStageMetadataResponseObject, error) {
	var req *lfsapi.MetadataSubmitRequest
	if request.JSONBody != nil {
		req = request.JSONBody
	} else if request.ApplicationVndGitLfsPlusJSONBody != nil {
		req = request.ApplicationVndGitLfsPlusJSONBody
	}

	if req == nil || len(req.Candidates) == 0 {
		return lfsapi.LfsStageMetadata400JSONResponse{Message: "candidates cannot be empty"}, nil
	}

	now := time.Now().UTC()
	entries := make([]models.PendingLFSMeta, 0, len(req.Candidates))
	for i, c := range req.Candidates {
		drsCandidate := core.LFSCandidateToDRS(c)
		internalObj, err := core.CandidateToInternalObject(drsCandidate, now)
		if err != nil {
			return lfsapi.LfsStageMetadata400JSONResponse{Message: fmt.Sprintf("candidate[%d] invalid: %v", i, err)}, nil
		}

		oid, _ := common.CanonicalSHA256(internalObj.Checksums)
		entries = append(entries, models.PendingLFSMeta{
			OID:       oid,
			Candidate: drsCandidate,
			CreatedAt: now,
			ExpiresAt: now.Add(20 * time.Minute),
		})
	}

	if err := s.om.SavePendingLFSMeta(ctx, entries); err != nil {
		return lfsapi.LfsStageMetadata500JSONResponse{Message: err.Error()}, nil
	}
	return lfsapi.LfsStageMetadata200JSONResponse{Staged: int32(len(entries))}, nil
}

func (s *LFSServer) LfsUploadProxy(ctx context.Context, request lfsapi.LfsUploadProxyRequestObject) (lfsapi.LfsUploadProxyResponseObject, error) {
	oid := sycommon.NormalizeOid(request.Oid)
	if oid == "" {
		return lfsapi.LfsUploadProxy400TextResponse("invalid oid"), nil
	}

	creds, err := s.om.ListS3Credentials(ctx)
	if err != nil || len(creds) == 0 {
		return lfsapi.LfsUploadProxy507TextResponse("no bucket configured"), nil
	}
	bucket := creds[0].Bucket
	key := oid

	if err := s.handleUploadInternal(ctx, request.Body, bucket, key, oid); err != nil {
		return lfsapi.LfsUploadProxy500TextResponse(err.Error()), nil
	}

	return lfsapi.LfsUploadProxy200Response{}, nil
}

func (s *LFSServer) handleUploadInternal(ctx context.Context, body io.Reader, bucket, key, usageObjectID string) error {
	const multipartPartSize = 64 * 1024 * 1024 // 64 MiB

	uploadID, err := s.om.InitMultipartUpload(ctx, bucket, key)
	if err != nil {
		return fmt.Errorf("failed to initialize multipart upload: %w", err)
	}

	parts := make([]urlmanager.MultipartPart, 0, 16)
	partNum := int32(1)
	buf := make([]byte, multipartPartSize)
	for {
		n, readErr := io.ReadFull(body, buf)
		if readErr == io.EOF {
			break
		}
		if readErr == io.ErrUnexpectedEOF {
			if n == 0 {
				break
			}
		} else if readErr != nil {
			return fmt.Errorf("failed reading upload stream: %w", readErr)
		}

		partURL, err := s.om.SignMultipartPart(ctx, bucket, key, uploadID, partNum)
		if err != nil {
			return fmt.Errorf("failed to sign multipart part: %w", err)
		}
		etag, err := uploadPartToSignedURL(ctx, partURL, buf[:n])
		if err != nil {
			return fmt.Errorf("failed uploading multipart part: %w", err)
		}
		parts = append(parts, urlmanager.MultipartPart{PartNumber: partNum, ETag: etag})
		partNum++

		if readErr == io.ErrUnexpectedEOF {
			break
		}
	}

	if err := s.om.CompleteMultipartUpload(ctx, bucket, key, uploadID, parts); err != nil {
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	_ = s.om.RecordUpload(ctx, usageObjectID)
	return nil
}
