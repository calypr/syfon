package lfs

import (
	"context"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/lfsapi"
	"github.com/calypr/syfon/db/core"
	"github.com/gofiber/fiber/v3"
)

func validateLFSRequestHeaders(c fiber.Ctx, requireAccept bool, requireContentType bool) bool {
	const mediaType = "application/vnd.git-lfs+json"
	if requireAccept {
		accept := strings.ToLower(strings.TrimSpace(c.Get("Accept")))
		if accept == "" || (!strings.Contains(accept, mediaType) && !strings.Contains(accept, "*/*")) {
			writeLFSError(c, http.StatusNotAcceptable, "Accept header must include application/vnd.git-lfs+json", false)
			return false
		}
	}
	if requireContentType {
		contentType := strings.TrimSpace(c.Get("Content-Type"))
		if contentType == "" {
			writeLFSError(c, http.StatusUnprocessableEntity, "Content-Type must be application/vnd.git-lfs+json", false)
			return false
		}
		parsed, _, err := mime.ParseMediaType(contentType)
		if err != nil || strings.ToLower(parsed) != mediaType {
			writeLFSError(c, http.StatusUnprocessableEntity, "Content-Type must be application/vnd.git-lfs+json", false)
			return false
		}
	}
	return true
}

func writeLFSError(c fiber.Ctx, status int, message string, challenge bool) error {
	if challenge {
		c.Set("LFS-Authenticate", `Basic realm="Git LFS"`)
	}
	c.Set("Content-Type", "application/vnd.git-lfs+json")
	payload := lfsapi.LFSErrorResponse{
		Message: message,
	}
	if reqID := core.GetRequestID(c.Context()); reqID != "" {
		payload.RequestId = &reqID
	}
	docURL := "https://github.com/git-lfs/git-lfs/blob/main/docs/api"
	payload.DocumentationUrl = &docURL

	return c.Status(status).JSON(payload)
}

func enforceRequestLimit(c fiber.Ctx, opts Options) bool {
	if opts.RequestLimitPerMinute <= 0 {
		return true
	}
	nowMinute := time.Now().UTC().Unix() / 60
	key := requestClientKey(c)
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
		writeLFSError(c, http.StatusTooManyRequests, "rate limit exceeded", false)
		return false
	}
	return true
}

func enforceBandwidthLimit(c fiber.Ctx, opts Options, bytes int64) bool {
	if opts.BandwidthLimitBytesPerMinute <= 0 || bytes <= 0 {
		return true
	}
	nowMinute := time.Now().UTC().Unix() / 60
	key := requestClientKey(c)
	limitMu.Lock()
	defer limitMu.Unlock()
	win := bandwidthWindowMap[key]
	if win.Minute != nowMinute {
		win.Minute = nowMinute
		win.Bytes = 0
	}
	if win.Bytes+bytes > opts.BandwidthLimitBytesPerMinute {
		writeLFSError(c, 509, "bandwidth limit exceeded", false)
		return false
	}
	win.Bytes += bytes
	bandwidthWindowMap[key] = win
	return true
}

func requestClientKey(c fiber.Ctx) string {
	if auth := strings.TrimSpace(c.Get("Authorization")); auth != "" {
		if len(auth) > 64 {
			auth = auth[:64]
		}
		return "auth:" + auth
	}
	return "addr:" + c.IP()
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

func requestBaseURL(c fiber.Ctx) string {
	return c.BaseURL()
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
	if len(accessMethods) == 0 {
		return nil
	}
	seen := make(map[string]struct{})
	out := make([]string, 0)
	for _, am := range accessMethods {
		if am.Authorizations != nil && am.Authorizations.BearerAuthIssuers != nil {
			for _, issuer := range *am.Authorizations.BearerAuthIssuers {
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
	}
	return out
}

func candidateToInternalObject(c drs.DrsObjectCandidate, now time.Time) (core.InternalObject, error) {
	oid, ok := canonicalSHA256(c.Checksums)
	if !ok {
		return core.InternalObject{}, fmt.Errorf("candidate must include sha256 checksum")
	}
	var ams []drs.AccessMethod
	if c.AccessMethods != nil {
		ams = *c.AccessMethods
	}
	authz := uniqueAuthz(ams)
	obj := drs.DrsObject{
		Id:          core.MintObjectIDFromChecksum(oid, authz),
		Size:        c.Size,
		CreatedTime: now,
		UpdatedTime: &now,
		Version:     core.Ptr("1"),
		MimeType:    c.MimeType,
		Description: c.Description,
		Aliases:     core.Ptr(append([]string(nil), derefStringSlice(c.Aliases)...)),
		SelfUri:     "",
		Checksums:   []drs.Checksum{{Type: "sha256", Checksum: oid}},
	}
	if c.Name != nil {
		obj.Name = c.Name
	}
	obj.SelfUri = "drs://" + obj.Id
	if obj.Name == nil || strings.TrimSpace(*obj.Name) == "" {
		obj.Name = &oid
	}
	seenAccess := make(map[string]struct{})
	if c.AccessMethods != nil {
		for _, am := range *c.AccessMethods {
			url := ""
			if am.AccessUrl != nil {
				url = strings.TrimSpace(am.AccessUrl.Url)
			}
			if url == "" {
				continue
			}
			key := string(am.Type) + "|" + url
			if _, ok := seenAccess[key]; ok {
				continue
			}
			seenAccess[key] = struct{}{}
			accessID := am.AccessId
			if accessID == nil || strings.TrimSpace(*accessID) == "" {
				accessID = core.Ptr(string(am.Type))
			}
			if obj.AccessMethods == nil {
				obj.AccessMethods = &[]drs.AccessMethod{}
			}

			*obj.AccessMethods = append(*obj.AccessMethods, drs.AccessMethod{
				Type:     drs.AccessMethodType(am.Type),
				AccessId: accessID,
				AccessUrl: &struct {
					Headers *[]string `json:"headers,omitempty"`
					Url     string    `json:"url"`
				}{Url: url},
				Authorizations: &struct {
					BearerAuthIssuers   *[]string                                        `json:"bearer_auth_issuers,omitempty"`
					DrsObjectId         *string                                          `json:"drs_object_id,omitempty"`
					PassportAuthIssuers *[]string                                        `json:"passport_auth_issuers,omitempty"`
					SupportedTypes      *[]drs.AccessMethodAuthorizationsSupportedTypes `json:"supported_types,omitempty"`
				}{
					BearerAuthIssuers: &authz,
				},
			})
		}
	}
	return core.InternalObject{
		DrsObject:      obj,
		Authorizations: authz,
	}, nil
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func derefStringSlice(s *[]string) []string {
	if s == nil {
		return nil
	}
	return *s
}

func lfsCandidateToDRS(in lfsapi.DrsObjectCandidate) drs.DrsObjectCandidate {
	var size int64
	if in.Size != nil {
		size = *in.Size
	}
	out := drs.DrsObjectCandidate{
		Name:        in.Name,
		Size:        size,
		MimeType:    in.MimeType,
		Description: in.Description,
		Aliases:     in.Aliases,
	}
	if in.Checksums != nil {
		out.Checksums = make([]drs.Checksum, 0, len(*in.Checksums))
		for _, c := range *in.Checksums {
			out.Checksums = append(out.Checksums, drs.Checksum{
				Type:     c.Type,
				Checksum: c.Checksum,
			})
		}
	}
	if in.AccessMethods != nil {
		ams := make([]drs.AccessMethod, 0, len(*in.AccessMethods))
		for _, am := range *in.AccessMethods {
			drsMethod := drs.AccessMethod{
				Type:     drs.AccessMethodType(derefString(am.Type)),
				Region:   am.Region,
				AccessId: am.AccessId,
			}
			if am.AccessUrl != nil {
				drsMethod.AccessUrl = &struct {
					Headers *[]string `json:"headers,omitempty"`
					Url     string    `json:"url"`
				}{}
				if am.AccessUrl.Url != nil {
					drsMethod.AccessUrl.Url = *am.AccessUrl.Url
				}
			}
			if am.Authorizations != nil {
				drsMethod.Authorizations = &struct {
					BearerAuthIssuers   *[]string                                        `json:"bearer_auth_issuers,omitempty"`
					DrsObjectId         *string                                          `json:"drs_object_id,omitempty"`
					PassportAuthIssuers *[]string                                        `json:"passport_auth_issuers,omitempty"`
					SupportedTypes      *[]drs.AccessMethodAuthorizationsSupportedTypes `json:"supported_types,omitempty"`
				}{
					BearerAuthIssuers: am.Authorizations.BearerAuthIssuers,
				}
			}
			ams = append(ams, drsMethod)
		}
		out.AccessMethods = &ams
	}
	return out
}

func unauthorizedStatus(ctx context.Context) int {
	if core.IsGen3Mode(ctx) && !core.HasAuthHeader(ctx) {
		return http.StatusUnauthorized
	}
	return http.StatusForbidden
}

func hasMethodAccess(ctx context.Context, method string, resources []string) bool {
	if !core.IsGen3Mode(ctx) {
		return true
	}
	if len(resources) == 0 {
		return true
	}
	return core.HasMethodAccess(ctx, method, resources)
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

func dbErrToBatchError(err error, ctx context.Context) *lfsapi.ObjectError {
	switch {
	case err == nil:
		return nil
	case isNotFound(err):
		return &lfsapi.ObjectError{Code: int32(http.StatusNotFound), Message: "not found"}
	case errors.Is(err, core.ErrUnauthorized), strings.Contains(strings.ToLower(err.Error()), "unauthorized"):
		return &lfsapi.ObjectError{Code: int32(unauthorizedStatus(ctx)), Message: "unauthorized"}
	default:
		return &lfsapi.ObjectError{Code: int32(http.StatusInternalServerError), Message: err.Error()}
	}
}
