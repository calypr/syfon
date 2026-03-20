package lfs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/calypr/drs-server/apigen/drs"
	"github.com/calypr/drs-server/apigen/lfsapi"
	"github.com/calypr/drs-server/db/core"
	"github.com/calypr/drs-server/urlmanager"
	"github.com/gorilla/mux"
)

type Options struct {
	MaxBatchObjects              int
	MaxBatchBodyBytes            int64
	RequestLimitPerMinute        int
	BandwidthLimitBytesPerMinute int64
}

func DefaultOptions() Options {
	return Options{
		MaxBatchObjects:              1000,
		MaxBatchBodyBytes:            10 * 1024 * 1024,
		RequestLimitPerMinute:        1200,
		BandwidthLimitBytesPerMinute: 0,
	}
}

func RegisterLFSRoutes(router *mux.Router, database core.DatabaseInterface, uM urlmanager.UrlManager, opts ...Options) {
	effective := DefaultOptions()
	if len(opts) > 0 {
		effective = opts[0]
	}
	router.HandleFunc("/info/lfs/objects/batch", handleBatch(database, uM, effective)).Methods(http.MethodPost)
	router.HandleFunc("/info/lfs/objects/metadata", handleMetadata(database)).Methods(http.MethodPost)
	router.HandleFunc("/info/lfs/objects/{oid}", handleUploadProxy(database, uM)).Methods(http.MethodPut)
	router.HandleFunc("/info/lfs/verify", handleVerify(database)).Methods(http.MethodPost)
}

var (
	limitMu            sync.Mutex
	requestWindowMap   = map[string]windowCounter{}
	bandwidthWindowMap = map[string]windowBytes{}
)

type windowCounter struct {
	Minute int64
	Count  int
}

type windowBytes struct {
	Minute int64
	Bytes  int64
}

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
		obj, err := database.GetObject(r.Context(), oid)
		if err == nil {
			if len(obj.Authorizations) > 0 && !hasMethodAccess(r, "read", obj.Authorizations) {
				writeLFSError(w, r, unauthorizedStatus(r), "Unauthorized", unauthorizedStatus(r) == http.StatusUnauthorized)
				return
			}
			if recErr := database.RecordFileUpload(r.Context(), oid); recErr != nil {
				slog.Debug("failed to record file upload metric", "request_id", core.GetRequestID(r.Context()), "oid", oid, "err", recErr)
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
		if recErr := database.RecordFileUpload(r.Context(), oid); recErr != nil {
			slog.Debug("failed to record file upload metric", "request_id", core.GetRequestID(r.Context()), "oid", oid, "err", recErr)
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

func prepareDownloadActions(r *http.Request, database core.DatabaseInterface, uM urlmanager.UrlManager, oid string) (*lfsapi.BatchActions, *lfsapi.ObjectError) {
	obj, err := database.GetObject(r.Context(), oid)
	if err != nil {
		return nil, dbErrToBatchError(err, r)
	}
	if len(obj.Authorizations) > 0 && !hasMethodAccess(r, "read", obj.Authorizations) {
		return nil, &lfsapi.ObjectError{Code: int32(unauthorizedStatus(r)), Message: "unauthorized"}
	}

	var src string
	for _, am := range obj.AccessMethods {
		if strings.TrimSpace(am.AccessUrl.Url) != "" {
			src = am.AccessUrl.Url
			break
		}
	}
	if src == "" {
		return nil, &lfsapi.ObjectError{Code: int32(http.StatusNotFound), Message: "no object location available"}
	}
	signed, err := uM.SignURL(r.Context(), "", src, urlmanager.SignOptions{})
	if err != nil {
		return nil, &lfsapi.ObjectError{Code: int32(http.StatusInternalServerError), Message: err.Error()}
	}
	if recErr := database.RecordFileDownload(r.Context(), oid); recErr != nil {
		slog.Debug("failed to record file download metric", "request_id", core.GetRequestID(r.Context()), "oid", oid, "err", recErr)
	}
	action := lfsapi.Action{Href: signed}
	return &lfsapi.BatchActions{Download: &action}, nil
}

func prepareUploadActions(r *http.Request, database core.DatabaseInterface, uM urlmanager.UrlManager, oid string, reqSize int64) (*lfsapi.BatchActions, int64, *lfsapi.ObjectError) {
	existing, err := database.GetObject(r.Context(), oid)
	if err == nil {
		if len(existing.Authorizations) > 0 && !hasMethodAccess(r, "read", existing.Authorizations) {
			return nil, existing.Size, &lfsapi.ObjectError{Code: int32(unauthorizedStatus(r)), Message: "unauthorized"}
		}
		return nil, existing.Size, nil
	}
	if !isNotFound(err) {
		return nil, reqSize, dbErrToBatchError(err, r)
	}

	targetResources := []string{"/data_file"}
	if !hasMethodAccess(r, "file_upload", targetResources) && !hasMethodAccess(r, "create", targetResources) {
		return nil, reqSize, &lfsapi.ObjectError{Code: int32(unauthorizedStatus(r)), Message: "unauthorized"}
	}

	creds, credErr := database.ListS3Credentials(r.Context())
	if credErr != nil || len(creds) == 0 || strings.TrimSpace(creds[0].Bucket) == "" {
		if credErr == nil {
			credErr = fmt.Errorf("no bucket configured")
		}
		return nil, reqSize, &lfsapi.ObjectError{Code: int32(http.StatusInsufficientStorage), Message: credErr.Error()}
	}
	size := reqSize
	if size < 0 {
		size = 0
	}
	base := requestBaseURL(r)
	uploadURL := base + "/info/lfs/objects/" + oid
	verifyURL := base + "/info/lfs/verify"
	uploadAction := lfsapi.Action{Href: uploadURL}
	verifyAction := lfsapi.Action{Href: verifyURL}
	return &lfsapi.BatchActions{
		Upload: &uploadAction,
		Verify: &verifyAction,
	}, size, nil
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
		key := oid // CAS key

		// 0-byte object: single signed PUT is simpler than multipart.
		if r.ContentLength == 0 {
			if err := proxySinglePut(r, uM, bucket, key); err != nil {
				writeHTTPError(w, r, http.StatusBadGateway, "upload failed", err)
				return
			}
			if recErr := database.RecordFileUpload(r.Context(), oid); recErr != nil {
				slog.Debug("failed to record file upload metric", "request_id", core.GetRequestID(r.Context()), "oid", oid, "err", recErr)
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
			// Defensive fallback when content-length is unknown but body is empty.
			if err := proxySinglePut(r, uM, bucket, key); err != nil {
				writeHTTPError(w, r, http.StatusBadGateway, "upload failed", err)
				return
			}
			if recErr := database.RecordFileUpload(r.Context(), oid); recErr != nil {
				slog.Debug("failed to record file upload metric", "request_id", core.GetRequestID(r.Context()), "oid", oid, "err", recErr)
			}
			w.WriteHeader(http.StatusOK)
			return
		}

		if err := uM.CompleteMultipartUpload(r.Context(), bucket, key, uploadID, parts); err != nil {
			writeHTTPError(w, r, http.StatusBadGateway, "failed to complete multipart upload", err)
			return
		}
		if recErr := database.RecordFileUpload(r.Context(), oid); recErr != nil {
			slog.Debug("failed to record file upload metric", "request_id", core.GetRequestID(r.Context()), "oid", oid, "err", recErr)
		}
		w.WriteHeader(http.StatusOK)
	}
}

func proxySinglePut(r *http.Request, uM urlmanager.UrlManager, bucket, key string) error {
	s3URL := fmt.Sprintf("s3://%s/%s", bucket, key)
	signedURL, err := uM.SignUploadURL(r.Context(), "", s3URL, urlmanager.SignOptions{})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPut, signedURL, bytes.NewReader(nil))
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("s3 put failed status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func uploadPartToSignedURL(ctx context.Context, signedURL string, content []byte) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, signedURL, bytes.NewReader(content))
	if err != nil {
		return "", err
	}
	req.ContentLength = int64(len(content))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return "", fmt.Errorf("multipart part put failed status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	etag := strings.TrimSpace(resp.Header.Get("ETag"))
	etag = strings.Trim(etag, "\"")
	if etag == "" {
		return "", fmt.Errorf("multipart part upload missing etag")
	}
	return etag, nil
}

func requestBaseURL(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if proto := strings.TrimSpace(r.Header.Get("X-Forwarded-Proto")); proto != "" {
		scheme = strings.ToLower(proto)
	}
	host := r.Host
	if forwardedHost := strings.TrimSpace(r.Header.Get("X-Forwarded-Host")); forwardedHost != "" {
		host = forwardedHost
	}
	return fmt.Sprintf("%s://%s", scheme, host)
}

func canonicalSHA256(checksums []drs.Checksum) (string, bool) {
	for _, c := range checksums {
		if strings.EqualFold(strings.TrimSpace(c.Type), "sha256") {
			oid := normalizeOID(c.Checksum)
			if oid != "" {
				return oid, true
			}
		}
	}
	return "", false
}

func uniqueAuthz(accessMethods []drs.AccessMethod) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0)
	for _, am := range accessMethods {
		for _, issuer := range am.Authorizations.BearerAuthIssuers {
			issuer = strings.TrimSpace(issuer)
			if issuer == "" {
				continue
			}
			if _, ok := seen[issuer]; ok {
				continue
			}
			seen[issuer] = struct{}{}
			out = append(out, issuer)
		}
	}
	return out
}

func candidateToInternalObject(c drs.DrsObjectCandidate, now time.Time) (core.InternalObject, error) {
	oid, ok := canonicalSHA256(c.Checksums)
	if !ok {
		return core.InternalObject{}, fmt.Errorf("candidate must include sha256 checksum")
	}
	obj := drs.DrsObject{
		Id:          oid,
		Name:        c.Name,
		Size:        c.Size,
		CreatedTime: now,
		UpdatedTime: now,
		Version:     "1",
		MimeType:    c.MimeType,
		Description: c.Description,
		Aliases:     append([]string(nil), c.Aliases...),
		SelfUri:     "drs://" + oid,
		Checksums:   []drs.Checksum{{Type: "sha256", Checksum: oid}},
	}
	if strings.TrimSpace(obj.Name) == "" {
		obj.Name = oid
	}
	authz := uniqueAuthz(c.AccessMethods)
	seenAccess := make(map[string]struct{})
	for _, am := range c.AccessMethods {
		url := strings.TrimSpace(am.AccessUrl.Url)
		if url == "" {
			continue
		}
		key := strings.TrimSpace(am.Type) + "|" + url
		if _, ok := seenAccess[key]; ok {
			continue
		}
		seenAccess[key] = struct{}{}
		accessID := am.AccessId
		if strings.TrimSpace(accessID) == "" {
			accessID = am.Type
		}
		obj.AccessMethods = append(obj.AccessMethods, drs.AccessMethod{
			Type:      am.Type,
			AccessId:  accessID,
			AccessUrl: drs.AccessMethodAccessUrl{Url: url},
			Region:    am.Region,
			Authorizations: drs.AccessMethodAuthorizations{
				BearerAuthIssuers: append([]string(nil), authz...),
			},
		})
	}
	return core.InternalObject{
		DrsObject:      obj,
		Authorizations: authz,
	}, nil
}

func normalizeOID(raw string) string {
	oid := strings.TrimSpace(strings.ToLower(raw))
	if len(oid) != 64 {
		return ""
	}
	for _, ch := range oid {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return ""
		}
	}
	return oid
}

func lfsCandidateToDRS(in lfsapi.DrsObjectCandidate) drs.DrsObjectCandidate {
	out := drs.DrsObjectCandidate{
		Name:        in.GetName(),
		Size:        in.GetSize(),
		MimeType:    in.GetMimeType(),
		Description: in.GetDescription(),
		Aliases:     append([]string(nil), in.GetAliases()...),
	}
	if checks := in.GetChecksums(); len(checks) > 0 {
		out.Checksums = make([]drs.Checksum, 0, len(checks))
		for _, c := range checks {
			out.Checksums = append(out.Checksums, drs.Checksum{
				Type:     c.GetType(),
				Checksum: c.GetChecksum(),
			})
		}
	}
	if methods := in.GetAccessMethods(); len(methods) > 0 {
		out.AccessMethods = make([]drs.AccessMethod, 0, len(methods))
		for _, am := range methods {
			drsMethod := drs.AccessMethod{
				Type:     am.GetType(),
				Region:   am.GetRegion(),
				AccessId: am.GetAccessId(),
			}
			if am.AccessUrl != nil {
				drsMethod.AccessUrl = drs.AccessMethodAccessUrl{
					Url: am.AccessUrl.GetUrl(),
				}
			}
			authz := am.GetAuthorizations()
			drsMethod.Authorizations = drs.AccessMethodAuthorizations{
				BearerAuthIssuers: append([]string(nil), authz.GetBearerAuthIssuers()...),
			}
			out.AccessMethods = append(out.AccessMethods, drsMethod)
		}
	}
	return out
}

func unauthorizedStatus(r *http.Request) int {
	if core.IsGen3Mode(r.Context()) && !core.HasAuthHeader(r.Context()) {
		return http.StatusUnauthorized
	}
	return http.StatusForbidden
}

func hasMethodAccess(r *http.Request, method string, resources []string) bool {
	if !core.IsGen3Mode(r.Context()) {
		return true
	}
	if len(resources) == 0 {
		return true
	}
	return core.HasMethodAccess(r.Context(), method, resources)
}

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, core.ErrNotFound) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "not found")
}

func isAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate") || strings.Contains(msg, "already exists") || strings.Contains(msg, "unique constraint")
}

func dbErrToBatchError(err error, r *http.Request) *lfsapi.ObjectError {
	switch {
	case err == nil:
		return nil
	case isNotFound(err):
		return &lfsapi.ObjectError{Code: int32(http.StatusNotFound), Message: "not found"}
	case errors.Is(err, core.ErrUnauthorized), strings.Contains(strings.ToLower(err.Error()), "unauthorized"):
		return &lfsapi.ObjectError{Code: int32(unauthorizedStatus(r)), Message: "unauthorized"}
	default:
		return &lfsapi.ObjectError{Code: int32(http.StatusInternalServerError), Message: err.Error()}
	}
}

func writeHTTPError(w http.ResponseWriter, r *http.Request, status int, msg string, err error) {
	requestID := core.GetRequestID(r.Context())
	if err != nil {
		slog.Error("lfs request failed", "request_id", requestID, "method", r.Method, "path", r.URL.Path, "status", status, "msg", msg, "err", err)
	} else {
		slog.Warn("lfs request rejected", "request_id", requestID, "method", r.Method, "path", r.URL.Path, "status", status, "msg", msg)
	}
	http.Error(w, msg, status)
}

func writeAuthError(w http.ResponseWriter, r *http.Request) {
	writeHTTPError(w, r, unauthorizedStatus(r), "Unauthorized", nil)
}

func writeDBError(w http.ResponseWriter, r *http.Request, err error) {
	if isNotFound(err) {
		writeHTTPError(w, r, http.StatusNotFound, "Object not found", err)
		return
	}
	if errors.Is(err, core.ErrUnauthorized) || strings.Contains(strings.ToLower(err.Error()), "unauthorized") {
		writeAuthError(w, r)
		return
	}
	writeHTTPError(w, r, http.StatusInternalServerError, err.Error(), err)
}

func writeLFSError(w http.ResponseWriter, r *http.Request, status int, message string, challenge bool) {
	requestID := core.GetRequestID(r.Context())
	if challenge {
		w.Header().Set("LFS-Authenticate", `Basic realm="Git LFS"`)
	}
	w.Header().Set("Content-Type", "application/vnd.git-lfs+json")
	w.WriteHeader(status)
	payload := lfsapi.LFSErrorResponse{
		Message: message,
	}
	if requestID != "" {
		payload.SetRequestId(requestID)
	}
	payload.SetDocumentationUrl("https://github.com/git-lfs/git-lfs/blob/main/docs/api")
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		slog.Error("lfs encode error response failed", "request_id", requestID, "method", r.Method, "path", r.URL.Path, "status", status, "err", err)
	}
}

func validateLFSRequestHeaders(w http.ResponseWriter, r *http.Request, requireAccept bool, requireContentType bool) bool {
	const mediaType = "application/vnd.git-lfs+json"
	if requireAccept {
		accept := strings.ToLower(strings.TrimSpace(r.Header.Get("Accept")))
		if accept == "" || (!strings.Contains(accept, mediaType) && !strings.Contains(accept, "*/*")) {
			writeLFSError(w, r, http.StatusNotAcceptable, "Accept header must include application/vnd.git-lfs+json", false)
			return false
		}
	}
	if requireContentType {
		contentType := strings.TrimSpace(r.Header.Get("Content-Type"))
		if contentType == "" {
			writeLFSError(w, r, http.StatusUnprocessableEntity, "Content-Type must be application/vnd.git-lfs+json", false)
			return false
		}
		parsed, _, err := mime.ParseMediaType(contentType)
		if err != nil || strings.ToLower(parsed) != mediaType {
			writeLFSError(w, r, http.StatusUnprocessableEntity, "Content-Type must be application/vnd.git-lfs+json", false)
			return false
		}
	}
	return true
}

func enforceRequestLimit(w http.ResponseWriter, r *http.Request, opts Options) bool {
	if opts.RequestLimitPerMinute <= 0 {
		return true
	}
	nowMinute := time.Now().UTC().Unix() / 60
	key := requestClientKey(r)
	limitMu.Lock()
	defer limitMu.Unlock()
	win := requestWindowMap[key]
	if win.Minute != nowMinute {
		win.Minute = nowMinute
		win.Count = 0
	}
	win.Count++
	requestWindowMap[key] = win
	if win.Count > opts.RequestLimitPerMinute {
		writeLFSError(w, r, http.StatusTooManyRequests, "rate limit exceeded", false)
		return false
	}
	return true
}

func enforceBandwidthLimit(w http.ResponseWriter, r *http.Request, opts Options, bytes int64) bool {
	if opts.BandwidthLimitBytesPerMinute <= 0 || bytes <= 0 {
		return true
	}
	nowMinute := time.Now().UTC().Unix() / 60
	key := requestClientKey(r)
	limitMu.Lock()
	defer limitMu.Unlock()
	win := bandwidthWindowMap[key]
	if win.Minute != nowMinute {
		win.Minute = nowMinute
		win.Bytes = 0
	}
	if win.Bytes+bytes > opts.BandwidthLimitBytesPerMinute {
		writeLFSError(w, r, 509, "bandwidth limit exceeded", false)
		return false
	}
	win.Bytes += bytes
	bandwidthWindowMap[key] = win
	return true
}

func requestClientKey(r *http.Request) string {
	if auth := strings.TrimSpace(r.Header.Get("Authorization")); auth != "" {
		if len(auth) > 64 {
			auth = auth[:64]
		}
		return "auth:" + auth
	}
	return "addr:" + strings.TrimSpace(r.RemoteAddr)
}

func resetLFSLimitersForTest() {
	limitMu.Lock()
	defer limitMu.Unlock()
	requestWindowMap = map[string]windowCounter{}
	bandwidthWindowMap = map[string]windowBytes{}
}
