package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// logOpenAPIResponseIssue logs an OpenAPI response validation failure.
// Intended for audit mode (and also for enforce-mode diagnostics).
func logOpenAPIResponseIssue(c *gin.Context, cfg ResponseValidatorConfig, detail string, status int, hdr http.Header, body []byte) {
	// ---- request metadata ----
	method := c.Request.Method
	path := c.Request.URL.Path
	query := c.Request.URL.RawQuery
	contentType := hdr.Get("Content-Type")

	// Pull common request IDs if present
	reqID := firstNonEmpty(
		c.GetHeader("X-Request-Id"),
		c.GetHeader("X-Request-ID"),
		c.GetHeader("X-Correlation-Id"),
		c.GetHeader("X-Correlation-ID"),
		c.GetString("request_id"), // if you set it earlier in middleware
	)

	// Latency if gin has one stored; otherwise compute from start time if you keep it in context.
	latency := time.Duration(0)
	if v, ok := c.Get("request_start"); ok {
		if t0, ok := v.(time.Time); ok {
			latency = time.Since(t0)
		}
	}

	// ---- header redaction ----
	redacted := redactHeaders(hdr, cfg.RedactHeaders)

	// ---- body logging controls ----
	const maxBodyLog = 4096 // keep logs bounded; tune as needed
	snippet, truncated := safeBodySnippet(body, maxBodyLog)

	// In some cases it's helpful to log a body hash for correlation without logging full body
	bodyHash := sha256Hex(body)

	// ---- emit ----
	// Keep it single-line and machine-friendly.
	// NOTE: Avoid dumping *all* headers by default; it can be very noisy.
	if cfg.LogHeaders {
		log.Printf(
			"openapi_response_invalid mode=%s method=%s path=%s query=%s status=%d content_type=%q req_id=%q latency_ms=%d detail=%q body_sha256=%s body_truncated=%t body=%q headers=%s",
			cfg.Mode, method, path, query, status, contentType, reqID, latency.Milliseconds(),
			detail, bodyHash, truncated, snippet, headerKVs(redacted),
		)
	} else {
		log.Printf(
			"openapi_response_invalid mode=%s method=%s path=%s query=%s status=%d content_type=%q req_id=%q latency_ms=%d detail=%q body_sha256=%s body_truncated=%t body=%q",
			cfg.Mode, method, path, query, status, contentType, reqID, latency.Milliseconds(),
			detail, bodyHash, truncated, snippet,
		)
	}
}

func redactHeaders(h http.Header, redactKeys []string) http.Header {
	out := cloneHeader(h)

	// Build a canonical set of keys to redact (case-insensitive).
	redactSet := map[string]struct{}{}
	for _, k := range redactKeys {
		k = strings.TrimSpace(k)
		if k == "" {
			continue
		}
		redactSet[strings.ToLower(k)] = struct{}{}
		redactSet[strings.ToLower(http.CanonicalHeaderKey(k))] = struct{}{}
	}

	// Always redact some defaults even if config forgets them.
	for _, k := range []string{"Authorization", "X-Api-Key", "X-Amz-Security-Token"} {
		redactSet[strings.ToLower(k)] = struct{}{}
		redactSet[strings.ToLower(http.CanonicalHeaderKey(k))] = struct{}{}
	}

	for k := range out {
		if _, ok := redactSet[strings.ToLower(k)]; ok {
			out.Set(k, "[REDACTED]")
		}
	}

	return out
}

// safeBodySnippet returns a loggable snippet of body (escaped) and whether it was truncated.
// If the body is mostly binary, it emits a short hex prefix instead of raw bytes.
func safeBodySnippet(body []byte, max int) (string, bool) {
	if max <= 0 {
		return "", len(body) > 0
	}
	truncated := false
	b := body
	if len(b) > max {
		b = b[:max]
		truncated = true
	}

	// If it looks binary, log as hex prefix.
	if looksBinary(b) {
		// hex expands 2x; keep it bounded
		hexBytes := b
		if len(hexBytes) > 512 {
			hexBytes = hexBytes[:512]
			truncated = true
		}
		return "hex:" + hex.EncodeToString(hexBytes), truncated
	}

	// Otherwise, return as a safe quoted string; non-printables are escaped by %q at log.Printf call site.
	return string(b), truncated
}

func looksBinary(b []byte) bool {
	// Heuristic: if there are many NULs or too many non-printables, treat as binary.
	if len(b) == 0 {
		return false
	}
	nul := bytes.Count(b, []byte{0})
	if nul > 0 {
		return true
	}
	nonPrintable := 0
	for _, c := range b {
		if c == '\n' || c == '\r' || c == '\t' {
			continue
		}
		if c < 0x20 || c == 0x7f {
			nonPrintable++
		}
	}
	return nonPrintable > len(b)/10
}

func headerKVs(h http.Header) string {
	// Produce a compact string like: k1=v1;k2=v2
	// Avoid huge headers; caller already controls cfg.LogHeaders.
	var parts []string
	for k, vs := range h {
		// collapse multi-values
		v := strings.Join(vs, ",")
		// hard cap per-header
		if len(v) > 256 {
			v = v[:256] + "…"
		}
		parts = append(parts, k+"="+v)
	}
	// Stable ordering is nice but not required; add sort.Strings(parts) if you prefer.
	return strings.Join(parts, ";")
}

func sha256Hex(b []byte) string {
	sum := sha256Sum(b) // wrapper to avoid extra imports in other files if desired
	return sum
}

// Minimal wrapper so you can keep imports tidy if you want.
// If you don't care, just inline crypto/sha256 and hex in sha256Hex.
func sha256Sum(b []byte) string {
	// local inline implementation to keep this file self-contained
	// (uses small helper to avoid extra dependency churn)
	// NOTE: If you already imported crypto/sha256 + hex above, you can simplify.
	h := sha256.New()
	_, _ = h.Write(b)
	return hex.EncodeToString(h.Sum(nil))
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
