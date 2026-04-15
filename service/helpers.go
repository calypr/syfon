package service

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/db/core"
)

func errorResponseForDBError(ctx context.Context, op string, err error) drs.ImplResponse {
	requestID := core.GetRequestID(ctx)
	switch {
	case errors.Is(err, core.ErrUnauthorized):
		code := http.StatusForbidden
		if core.IsGen3Mode(ctx) && !core.HasAuthHeader(ctx) {
			code = http.StatusUnauthorized
		}
		slog.Warn("service db unauthorized", "op", op, "request_id", requestID, "status", code, "err", err)
		return drs.ImplResponse{Code: code, Body: drsError("unauthorized", code)}
	case errors.Is(err, core.ErrNotFound):
		slog.Info("service db not found", "op", op, "request_id", requestID, "status", http.StatusNotFound, "err", err)
		return drs.ImplResponse{Code: http.StatusNotFound, Body: drsError("not found", http.StatusNotFound)}
	default:
		slog.Error("service db failure", "op", op, "request_id", requestID, "status", http.StatusInternalServerError, "err", err)
		return drs.ImplResponse{Code: http.StatusInternalServerError, Body: drsError(err.Error(), http.StatusInternalServerError)}
	}
}

func drsError(msg string, status int) drs.Error {
	return drs.Error{Msg: core.Ptr(msg), StatusCode: core.Ptr(status)}
}

func unauthorizedStatus(ctx context.Context) int {
	if core.IsGen3Mode(ctx) && !core.HasAuthHeader(ctx) {
		return http.StatusUnauthorized
	}
	return http.StatusForbidden
}

func forbiddenResponse(ctx context.Context, msg string) drs.ImplResponse {
	code := unauthorizedStatus(ctx)
	slog.Warn("service forbidden", "request_id", core.GetRequestID(ctx), "status", code, "reason", msg)
	return drs.ImplResponse{
		Code: code,
		Body: drsError(msg, code),
	}
}

func tooLargeResponse(msg string) drs.ImplResponse {
	return drs.ImplResponse{
		Code: http.StatusRequestEntityTooLarge,
		Body: drsError(msg, http.StatusRequestEntityTooLarge),
	}
}

func normalizeChecksum(cs string) string {
	if parts := strings.SplitN(cs, ":", 2); len(parts) == 2 {
		return parts[1]
	}
	return cs
}

func normalizeChecksumType(checksumType string) string {
	normalized := strings.ToLower(strings.TrimSpace(checksumType))
	normalized = strings.ReplaceAll(normalized, "-", "")
	return normalized
}

func parseChecksumQuery(checksum string) (checksumType string, checksumValue string) {
	clean := strings.Trim(strings.TrimSpace(checksum), `"'`)
	checksumType = ""
	checksumValue = normalizeChecksum(clean)
	if parts := strings.SplitN(clean, ":", 2); len(parts) == 2 {
		checksumType = normalizeChecksumType(parts[0])
		checksumValue = strings.Trim(strings.TrimSpace(parts[1]), `"'`)
	}
	return checksumType, checksumValue
}

func mergeAdditionalChecksums(existing []drs.Checksum, additions []drs.Checksum) []drs.Checksum {
	out := make([]drs.Checksum, 0, len(existing)+len(additions))
	seenTypes := make(map[string]struct{}, len(existing)+len(additions))

	for _, cs := range existing {
		if t := normalizeChecksumType(cs.Type); t != "" {
			seenTypes[t] = struct{}{}
		}
		out = append(out, cs)
	}

	for _, cs := range additions {
		t := normalizeChecksumType(cs.Type)
		v := strings.TrimSpace(normalizeChecksum(cs.Checksum))
		if t == "" || v == "" {
			continue
		}
		if _, exists := seenTypes[t]; exists {
			// Do not alter existing checksum types.
			continue
		}
		out = append(out, drs.Checksum{Type: strings.TrimSpace(cs.Type), Checksum: v})
		seenTypes[t] = struct{}{}
	}
	return out
}

func canonicalSHA256(checksums []drs.Checksum) (string, bool) {
	for _, cs := range checksums {
		checksumType := strings.ToLower(strings.TrimSpace(cs.Type))
		if checksumType == "sha256" || checksumType == "sha-256" {
			normalized := normalizeChecksum(strings.TrimSpace(cs.Checksum))
			if normalized != "" {
				return normalized, true
			}
		}
	}
	return "", false
}

func authorizationsForObject(obj *core.InternalObject) drs.Authorizations {
	authz := uniqueStrings(obj.Authorizations)
	if len(authz) == 0 {
		if obj.AccessMethods != nil {
			for _, am := range *obj.AccessMethods {
				if am.Authorizations != nil && am.Authorizations.BearerAuthIssuers != nil {
					authz = append(authz, (*am.Authorizations.BearerAuthIssuers)...)
				}
			}
		}
		authz = uniqueStrings(authz)
	}
	var issuers *[]string
	if len(authz) > 0 {
		issuers = &authz
	}
	supported := []drs.AuthorizationsSupportedTypes{drs.AuthorizationsSupportedTypesBearerAuth}
	return drs.Authorizations{
		BearerAuthIssuers: issuers,
		SupportedTypes:    &supported,
	}
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, v := range values {
		if v == "" {
			continue
		}
		if _, exists := seen[v]; exists {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func uniqueStringsCaseInsensitive(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, v := range values {
		normalized := strings.ToLower(strings.TrimSpace(v))
		if normalized == "" {
			continue
		}
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, v)
	}
	return out
}
