package lfs

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/lfsapi"
	"github.com/calypr/syfon/db/core"
	"github.com/calypr/syfon/urlmanager"
)

type LFSServer struct {
	database core.DatabaseInterface
	uM       urlmanager.UrlManager
	opts     Options
}

func NewLFSServer(database core.DatabaseInterface, uM urlmanager.UrlManager, opts Options) *LFSServer {
	return &LFSServer{
		database: database,
		uM:       uM,
		opts:     opts,
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

	totalBytes := int64(0)
	for _, in := range req.Objects {
		if in.Size > 0 {
			totalBytes += in.Size
		}
	}

	if req.Operation == "upload" && !hasGlobalAccess(ctx, "file_upload") && !hasGlobalAccess(ctx, "create") {
		return lfsapi.LfsBatch403ApplicationVndGitLfsPlusJSONResponse{Message: "forbidden"}, nil
	}

	transfer := "basic"
	respObjects := make([]lfsapi.BatchResponseObject, 0, len(req.Objects))
	hashAlgo := "sha256"

	for _, in := range req.Objects {
		objResp := lfsapi.BatchResponseObject{Oid: in.Oid, Size: in.Size}
		oid := normalizeOID(in.Oid)
		if oid == "" {
			objResp.Error = &lfsapi.ObjectError{Code: int32(http.StatusBadRequest), Message: "invalid oid"}
			respObjects = append(respObjects, objResp)
			continue
		}
		objResp.Oid = oid

		switch req.Operation {
		case "download":
			actions, errResp := prepareDownloadActions(ctx, s.database, s.uM, oid)
			if errResp != nil {
				objResp.Error = errResp
			} else {
				objResp.Actions = actions
			}
		case "upload":
			baseURL, _ := ctx.Value(baseURLKey).(string)
			actions, size, errResp := prepareUploadActions(ctx, s.database, s.uM, oid, in.Size, baseURL)
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
	oid := normalizeOID(req.Oid)
	if oid == "" {
		return lfsapi.LfsVerify400ApplicationVndGitLfsPlusJSONResponse{Message: "invalid oid"}, nil
	}
	obj, err := resolveObjectForOID(ctx, s.database, oid)
	if err == nil {
		if len(obj.Authorizations) > 0 && !hasCtxMethodAccess(ctx, "read", obj.Authorizations) {
			return lfsapi.LfsVerify403ApplicationVndGitLfsPlusJSONResponse{Message: "Unauthorized"}, nil
		}
		usageObjectID := oid
		if strings.TrimSpace(obj.Id) != "" {
			usageObjectID = strings.TrimSpace(obj.Id)
		}
		if recErr := s.database.RecordFileUpload(ctx, usageObjectID); recErr != nil {
			// Log but don't fail
		}
		return lfsapi.LfsVerify200Response{}, nil
	}
	if !isNotFound(err) {
		return lfsapi.LfsVerify500ApplicationVndGitLfsPlusJSONResponse{Message: err.Error()}, nil
	}
	pending, popErr := s.database.PopPendingLFSMeta(ctx, oid)
	if popErr != nil {
		if isNotFound(popErr) {
			return lfsapi.LfsVerify404ApplicationVndGitLfsPlusJSONResponse{Message: "Object not found"}, nil
		} else {
			return lfsapi.LfsVerify500ApplicationVndGitLfsPlusJSONResponse{Message: popErr.Error()}, nil
		}
	}
	now := time.Now().UTC()
	internalObj, convErr := candidateToInternalObject(pending.Candidate, now)
	if convErr != nil {
		return lfsapi.LfsVerify400ApplicationVndGitLfsPlusJSONResponse{Message: convErr.Error()}, nil
	}
	targetResources := append([]string(nil), internalObj.Authorizations...)
	if len(targetResources) == 0 {
		targetResources = []string{"/data_file"}
	}
	if !hasCtxMethodAccess(ctx, "create", targetResources) && !hasGlobalAccess(ctx, "file_upload") {
		return lfsapi.LfsVerify403ApplicationVndGitLfsPlusJSONResponse{Message: "Unauthorized"}, nil
	}
	if regErr := s.database.RegisterObjects(ctx, []core.InternalObject{internalObj}); regErr != nil && !isAlreadyExists(regErr) {
		return lfsapi.LfsVerify500ApplicationVndGitLfsPlusJSONResponse{Message: regErr.Error()}, nil
	}
	usageObjectID := oid
	if strings.TrimSpace(internalObj.Id) != "" {
		usageObjectID = strings.TrimSpace(internalObj.Id)
	}
	if recErr := s.database.RecordFileUpload(ctx, usageObjectID); recErr != nil {
		// Log but don't fail
	}
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

	ttl := int64(20 * 60) // 20 minutes default.
	if req.TtlSeconds != nil {
		ttl = *req.TtlSeconds
	}
	if ttl < 30 {
		ttl = 30
	}
	if ttl > 24*60*60 {
		ttl = 24 * 60 * 60
	}

	now := time.Now().UTC()
	entries := make([]core.PendingLFSMeta, 0, len(req.Candidates))
	for i, c := range req.Candidates {
		drsCandidate := lfsCandidateToDRS(c)
		oid, ok := canonicalSHA256(drsCandidate.Checksums)
		if !ok || normalizeOID(oid) == "" {
			return lfsapi.LfsStageMetadata400JSONResponse{Message: "candidate[" + strconv.Itoa(i) + "] must include valid sha256 checksum"}, nil
		}
		targetResources := []string{}
		if drsCandidate.AccessMethods != nil {
			targetResources = uniqueAuthz(*drsCandidate.AccessMethods)
		}
		if len(targetResources) == 0 {
			targetResources = []string{"/data_file"}
			if !hasGlobalAccess(ctx, "file_upload") && !hasGlobalAccess(ctx, "create") {
				return lfsapi.LfsStageMetadata403JSONResponse{Message: "Unauthorized"}, nil
			}
		} else if !hasCtxMethodAccess(ctx, "create", targetResources) && !hasGlobalAccess(ctx, "file_upload") {
			return lfsapi.LfsStageMetadata403JSONResponse{Message: "Unauthorized"}, nil
		}
		entries = append(entries, core.PendingLFSMeta{
			OID:       oid,
			Candidate: drsCandidate,
			CreatedAt: now,
			ExpiresAt: now.Add(time.Duration(ttl) * time.Second),
		})
	}

	if err := s.database.SavePendingLFSMeta(ctx, entries); err != nil {
		return lfsapi.LfsStageMetadata500JSONResponse{Message: err.Error()}, nil
	}
	return lfsapi.LfsStageMetadata200JSONResponse{Staged: int32(len(entries))}, nil
}

func (s *LFSServer) LfsUploadProxy(ctx context.Context, request lfsapi.LfsUploadProxyRequestObject) (lfsapi.LfsUploadProxyResponseObject, error) {
	oid := normalizeOID(request.Oid)
	if oid == "" {
		return lfsapi.LfsUploadProxy400TextResponse("invalid oid"), nil
	}
	targetResources := []string{"/data_file"}
	if !hasCtxMethodAccess(ctx, "file_upload", targetResources) && !hasCtxMethodAccess(ctx, "create", targetResources) {
		return lfsapi.LfsUploadProxy403TextResponse("Unauthorized"), nil
	}

	creds, err := s.database.ListS3Credentials(ctx)
	if err != nil || len(creds) == 0 || strings.TrimSpace(creds[0].Bucket) == "" {
		if err == nil {
			err = fmt.Errorf("no bucket configured")
		}
		return lfsapi.LfsUploadProxy507TextResponse("failed to resolve upload bucket: " + err.Error()), nil
	}
	bucket := strings.TrimSpace(creds[0].Bucket)
	key := oid // default CAS key
	usageObjectID := oid
	if obj, getErr := resolveObjectForOID(ctx, s.database, oid); getErr == nil {
		if strings.TrimSpace(obj.Id) != "" {
			usageObjectID = strings.TrimSpace(obj.Id)
		}
		if resolvedKey, ok := s3KeyFromObjectForBucket(obj, bucket); ok {
			key = resolvedKey
		}
	} else if isNotFound(getErr) {
		if pending, pErr := s.database.GetPendingLFSMeta(ctx, oid); pErr == nil {
			if resolvedKey, ok := s3KeyFromCandidateForBucket(pending.Candidate, bucket); ok {
				key = resolvedKey
			}
		}
	}

	// Read body and handle upload.
	// We need to implement the proxy logic here.
	// Since we are in StrictServer, we can use request.Body (io.Reader).

	if err := s.handleUploadInternal(ctx, request.Body, bucket, key, usageObjectID); err != nil {
		return lfsapi.LfsUploadProxy500TextResponse(err.Error()), nil
	}

	return lfsapi.LfsUploadProxy200Response{}, nil
}

func (s *LFSServer) handleUploadInternal(ctx context.Context, body io.Reader, bucket, key, usageObjectID string) error {
	const multipartPartSize = 64 * 1024 * 1024 // 64 MiB

	uploadID, err := s.uM.InitMultipartUpload(ctx, bucket, key)
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

		partURL, err := s.uM.SignMultipartPart(ctx, bucket, key, uploadID, partNum)
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

	if len(parts) == 0 {
		if err := s.uM.CompleteMultipartUpload(ctx, bucket, key, uploadID, parts); err != nil {
			return fmt.Errorf("failed to complete empty multipart upload: %w", err)
		}
	} else {
		if err := s.uM.CompleteMultipartUpload(ctx, bucket, key, uploadID, parts); err != nil {
			return fmt.Errorf("failed to complete multipart upload: %w", err)
		}
	}

	if recErr := s.database.RecordFileUpload(ctx, usageObjectID); recErr != nil {
		// Log but don't fail
	}
	return nil
}

func hasGlobalAccess(ctx context.Context, method string) bool {
	return core.HasMethodAccess(ctx, method, []string{"/data_file"})
}

func hasCtxMethodAccess(ctx context.Context, method string, resources []string) bool {
	return core.HasMethodAccess(ctx, method, resources)
}
