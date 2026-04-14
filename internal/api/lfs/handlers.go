package lfs

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/lfsapi"
	"github.com/calypr/syfon/db/core"
	"github.com/calypr/syfon/urlmanager"
	"github.com/gorilla/mux"
)

func handleBatch(database core.DatabaseInterface, uM urlmanager.UrlManager, opts Options) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !validateLFSRequestHeaders(w, r, true, true) {
			return
		}
		if core.IsGen3Mode(r.Context()) && !core.HasAuthHeader(r.Context()) {
			writeLFSError(w, r, http.StatusUnauthorized, "Credentials needed", true)
			return
		}
		if opts.MaxBatchBodyBytes > 0 && r.ContentLength > opts.MaxBatchBodyBytes {
			writeLFSError(w, r, http.StatusRequestEntityTooLarge, "batch request too large", false)
			return
		}
		var req lfsapi.BatchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeLFSError(w, r, http.StatusBadRequest, "invalid request body", false)
			return
		}
		req.Operation = strings.ToLower(strings.TrimSpace(req.Operation))
		if req.Operation != "download" && req.Operation != "upload" {
			writeLFSError(w, r, http.StatusBadRequest, "operation must be 'download' or 'upload'", false)
			return
		}
		if len(req.Objects) == 0 {
			writeLFSError(w, r, http.StatusBadRequest, "objects cannot be empty", false)
			return
		}
		if opts.MaxBatchObjects > 0 && len(req.Objects) > opts.MaxBatchObjects {
			writeLFSError(w, r, http.StatusRequestEntityTooLarge, "batch contains too many objects", false)
			return
		}
		totalBytes := int64(0)
		for _, in := range req.Objects {
			if in.Size > 0 {
				totalBytes += in.Size
			}
		}
		if !enforceRequestLimit(w, r, opts) {
			return
		}
		if !enforceBandwidthLimit(w, r, opts, totalBytes) {
			return
		}
		if req.Operation == "upload" && !hasMethodAccess(r, "file_upload", []string{"/data_file"}) && !hasMethodAccess(r, "create", []string{"/data_file"}) {
			writeLFSError(w, r, unauthorizedStatus(r), "forbidden", core.IsGen3Mode(r.Context()) && !core.HasAuthHeader(r.Context()))
			return
		}

		transfer := "basic"
		resp := lfsapi.BatchResponse{
			Transfer: &transfer,
			Objects:  make([]lfsapi.BatchResponseObject, 0, len(req.Objects)),
		}
		hashAlgo := "sha256"
		resp.HashAlgo = &hashAlgo

		for _, in := range req.Objects {
			objResp := lfsapi.BatchResponseObject{Oid: in.Oid, Size: in.Size}
			oid := normalizeOID(in.Oid)
			if oid == "" {
				objResp.Error = &lfsapi.ObjectError{Code: int32(http.StatusBadRequest), Message: "invalid oid"}
				resp.Objects = append(resp.Objects, objResp)
				continue
			}
			objResp.Oid = oid

			switch req.Operation {
			case "download":
				actions, errResp := prepareDownloadActions(r, database, uM, oid)
				if errResp != nil {
					objResp.Error = errResp
				} else {
					objResp.Actions = actions
				}
			case "upload":
				actions, size, errResp := prepareUploadActions(r, database, uM, oid, in.Size)
				objResp.Size = size
				if errResp != nil {
					objResp.Error = errResp
				} else {
					objResp.Actions = actions
				}
			}
			resp.Objects = append(resp.Objects, objResp)
		}

		w.Header().Set("Content-Type", "application/vnd.git-lfs+json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			slog.Error("lfs encode response failed", "request_id", core.GetRequestID(r.Context()), "path", r.URL.Path, "err", err)
		}
	}
}

func handleVerify(database core.DatabaseInterface) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !validateLFSRequestHeaders(w, r, true, true) {
			return
		}
		if core.IsGen3Mode(r.Context()) && !core.HasAuthHeader(r.Context()) {
			writeLFSError(w, r, http.StatusUnauthorized, "Credentials needed", true)
			return
		}
		var req lfsapi.VerifyRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeLFSError(w, r, http.StatusBadRequest, "invalid request body", false)
			return
		}
		oid := normalizeOID(req.Oid)
		if oid == "" {
			writeLFSError(w, r, http.StatusBadRequest, "invalid oid", false)
			return
		}
		obj, err := resolveObjectForOID(r.Context(), database, oid)
		if err == nil {
			if len(obj.Authorizations) > 0 && !hasMethodAccess(r, "read", obj.Authorizations) {
				writeLFSError(w, r, unauthorizedStatus(r), "Unauthorized", unauthorizedStatus(r) == http.StatusUnauthorized)
				return
			}
			usageObjectID := oid
			if strings.TrimSpace(obj.Id) != "" {
				usageObjectID = strings.TrimSpace(obj.Id)
			}
			if recErr := database.RecordFileUpload(r.Context(), usageObjectID); recErr != nil {
				// Log but don't fail
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		if !isNotFound(err) {
			writeLFSError(w, r, http.StatusInternalServerError, err.Error(), false)
			return
		}
		pending, popErr := database.PopPendingLFSMeta(r.Context(), oid)
		if popErr != nil {
			if isNotFound(popErr) {
				writeLFSError(w, r, http.StatusNotFound, "Object not found", false)
			} else {
				writeLFSError(w, r, http.StatusInternalServerError, popErr.Error(), false)
			}
			return
		}
		now := time.Now().UTC()
		internalObj, convErr := candidateToInternalObject(pending.Candidate, now)
		if convErr != nil {
			writeLFSError(w, r, http.StatusBadRequest, convErr.Error(), false)
			return
		}
		targetResources := append([]string(nil), internalObj.Authorizations...)
		if len(targetResources) == 0 {
			targetResources = []string{"/data_file"}
		}
		if !hasMethodAccess(r, "create", targetResources) && !hasMethodAccess(r, "file_upload", []string{"/data_file"}) {
			writeLFSError(w, r, unauthorizedStatus(r), "Unauthorized", unauthorizedStatus(r) == http.StatusUnauthorized)
			return
		}
		if regErr := database.RegisterObjects(r.Context(), []core.InternalObject{internalObj}); regErr != nil && !isAlreadyExists(regErr) {
			writeLFSError(w, r, http.StatusInternalServerError, regErr.Error(), false)
			return
		}
		usageObjectID := oid
		if strings.TrimSpace(internalObj.Id) != "" {
			usageObjectID = strings.TrimSpace(internalObj.Id)
		}
		if recErr := database.RecordFileUpload(r.Context(), usageObjectID); recErr != nil {
			// Log but don't fail
		}
		w.WriteHeader(http.StatusOK)
	}
}

func handleMetadata(database core.DatabaseInterface) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req lfsapi.MetadataSubmitRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeHTTPError(w, r, http.StatusBadRequest, "invalid request body", err)
			return
		}
		if len(req.Candidates) == 0 {
			writeHTTPError(w, r, http.StatusBadRequest, "candidates cannot be empty", nil)
			return
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
				writeHTTPError(w, r, http.StatusBadRequest, "candidate["+strconv.Itoa(i)+"] must include valid sha256 checksum", nil)
				return
			}
			targetResources := uniqueAuthz(drsCandidate.AccessMethods)
			if len(targetResources) == 0 {
				targetResources = []string{"/data_file"}
				if !hasMethodAccess(r, "file_upload", targetResources) && !hasMethodAccess(r, "create", targetResources) {
					writeAuthError(w, r)
					return
				}
			} else if !hasMethodAccess(r, "create", targetResources) && !hasMethodAccess(r, "file_upload", []string{"/data_file"}) {
				writeAuthError(w, r)
				return
			}
			entries = append(entries, core.PendingLFSMeta{
				OID:       oid,
				Candidate: drsCandidate,
				CreatedAt: now,
				ExpiresAt: now.Add(time.Duration(ttl) * time.Second),
			})
		}

		if err := database.SavePendingLFSMeta(r.Context(), entries); err != nil {
			writeDBError(w, r, err)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.git-lfs+json")
		_ = json.NewEncoder(w).Encode(lfsapi.MetadataSubmitResponse{Staged: int32(len(entries))})
	}
}

func handleUploadProxy(database core.DatabaseInterface, uM urlmanager.UrlManager) http.HandlerFunc {
	const multipartPartSize = 64 * 1024 * 1024 // 64 MiB

	return func(w http.ResponseWriter, r *http.Request) {
		oid := normalizeOID(mux.Vars(r)["oid"])
		if oid == "" {
			writeHTTPError(w, r, http.StatusBadRequest, "invalid oid", nil)
			return
		}
		targetResources := []string{"/data_file"}
		if !hasMethodAccess(r, "file_upload", targetResources) && !hasMethodAccess(r, "create", targetResources) {
			writeAuthError(w, r)
			return
		}

		creds, err := database.ListS3Credentials(r.Context())
		if err != nil || len(creds) == 0 || strings.TrimSpace(creds[0].Bucket) == "" {
			if err == nil {
				err = fmt.Errorf("no bucket configured")
			}
			writeHTTPError(w, r, http.StatusInsufficientStorage, "failed to resolve upload bucket", err)
			return
		}
		bucket := strings.TrimSpace(creds[0].Bucket)
		key := oid // default CAS key
		usageObjectID := oid
		if obj, getErr := resolveObjectForOID(r.Context(), database, oid); getErr == nil {
			if strings.TrimSpace(obj.Id) != "" {
				usageObjectID = strings.TrimSpace(obj.Id)
			}
			if resolvedKey, ok := s3KeyFromObjectForBucket(obj, bucket); ok {
				key = resolvedKey
			}
		} else if isNotFound(getErr) {
			if pending, pErr := database.GetPendingLFSMeta(r.Context(), oid); pErr == nil {
				if resolvedKey, ok := s3KeyFromCandidateForBucket(pending.Candidate, bucket); ok {
					key = resolvedKey
				}
			}
		}

		if r.ContentLength == 0 {
			if err := proxySinglePut(r, uM, bucket, key); err != nil {
				writeHTTPError(w, r, http.StatusBadGateway, "upload failed", err)
				return
			}
			if recErr := database.RecordFileUpload(r.Context(), usageObjectID); recErr != nil {
				// Log but don't fail
			}
			w.WriteHeader(http.StatusOK)
			return
		}

		uploadID, err := uM.InitMultipartUpload(r.Context(), bucket, key)
		if err != nil {
			writeHTTPError(w, r, http.StatusInternalServerError, "failed to initialize multipart upload", err)
			return
		}

		parts := make([]urlmanager.MultipartPart, 0, 16)
		partNum := int32(1)
		buf := make([]byte, multipartPartSize)
		for {
			n, readErr := io.ReadFull(r.Body, buf)
			if readErr == io.EOF {
				break
			}
			if readErr == io.ErrUnexpectedEOF {
				if n == 0 {
					break
				}
			} else if readErr != nil {
				writeHTTPError(w, r, http.StatusBadGateway, "failed reading upload stream", readErr)
				return
			}

			partURL, err := uM.SignMultipartPart(r.Context(), bucket, key, uploadID, partNum)
			if err != nil {
				writeHTTPError(w, r, http.StatusInternalServerError, "failed to sign multipart part", err)
				return
			}
			etag, err := uploadPartToSignedURL(r.Context(), partURL, buf[:n])
			if err != nil {
				writeHTTPError(w, r, http.StatusBadGateway, "failed uploading multipart part", err)
				return
			}
			parts = append(parts, urlmanager.MultipartPart{PartNumber: partNum, ETag: etag})
			partNum++

			if readErr == io.ErrUnexpectedEOF {
				break
			}
		}

		if len(parts) == 0 {
			if err := proxySinglePut(r, uM, bucket, key); err != nil {
				writeHTTPError(w, r, http.StatusBadGateway, "upload failed", err)
				return
			}
			if recErr := database.RecordFileUpload(r.Context(), usageObjectID); recErr != nil {
				// Log but don't fail
			}
			w.WriteHeader(http.StatusOK)
			return
		}

		if err := uM.CompleteMultipartUpload(r.Context(), bucket, key, uploadID, parts); err != nil {
			writeHTTPError(w, r, http.StatusBadGateway, "failed to complete multipart upload", err)
			return
		}
		if recErr := database.RecordFileUpload(r.Context(), usageObjectID); recErr != nil {
			// Log but don't fail
		}
		w.WriteHeader(http.StatusOK)
	}
}
