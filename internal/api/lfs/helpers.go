package lfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/calypr/drs-server/apigen/drs"
	"github.com/calypr/drs-server/apigen/lfsapi"
	"github.com/calypr/drs-server/db/core"
)

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

func resolveObjectForOID(ctx context.Context, database core.DatabaseInterface, oid string) (*core.InternalObject, error) {
	byChecksum, err := database.GetObjectsByChecksum(ctx, oid)
	if err != nil {
		return nil, err
	}
	if len(byChecksum) == 0 {
		if obj, getErr := database.GetObject(ctx, oid); getErr == nil {
			return obj, nil
		} else if !isNotFound(getErr) {
			return nil, getErr
		}
		return nil, fmt.Errorf("%w: object not found", core.ErrNotFound)
	}
	obj := byChecksum[0]
	return &obj, nil
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
	authz := uniqueAuthz(c.AccessMethods)
	obj := drs.DrsObject{
		Id:          core.MintObjectIDFromChecksum(oid, authz),
		Name:        c.Name,
		Size:        c.Size,
		CreatedTime: now,
		UpdatedTime: now,
		Version:     "1",
		MimeType:    c.MimeType,
		Description: c.Description,
		Aliases:     append([]string(nil), c.Aliases...),
		SelfUri:     "",
		Checksums:   []drs.Checksum{{Type: "sha256", Checksum: oid}},
	}
	obj.SelfUri = "drs://" + obj.Id
	if strings.TrimSpace(obj.Name) == "" {
		obj.Name = oid
	}
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
