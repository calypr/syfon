package common

import (
	"errors"
	"net/url"
	"regexp"
	"strings"

	"github.com/calypr/syfon/apigen/server/drs"
	sycommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/models"
)

var ErrNoValidSHA256 = errors.New("no valid sha256 values provided")

var sha256Like = regexp.MustCompile(`^[A-Fa-f0-9]{64}$`)

// LooksLikeSHA256 checks if a string matches the format of a SHA256 hash.
func LooksLikeSHA256(v string) bool {
	return sha256Like.MatchString(strings.TrimSpace(v))
}

// NormalizeChecksum removes any "sha256:" prefixes if present.
func NormalizeChecksum(cs string) string {
	if parts := strings.SplitN(cs, ":", 2); len(parts) == 2 {
		return parts[1]
	}
	return cs
}

// NormalizeChecksumType cleans up a checksum type string (lowercase, remove hyphens).
func NormalizeChecksumType(checksumType string) string {
	normalized := strings.ToLower(strings.TrimSpace(checksumType))
	normalized = strings.ReplaceAll(normalized, "-", "")
	return normalized
}

// ParseHashQuery parses a checksum string that might be in "type:value" format.
func ParseHashQuery(rawHash string, rawType string) (string, string) {
	hashType := NormalizeChecksumType(rawType)
	hashValue := strings.Trim(strings.TrimSpace(NormalizeChecksum(rawHash)), `"'`)
	if hashType == "" {
		if parts := strings.SplitN(strings.Trim(strings.TrimSpace(rawHash), `"'`), ":", 2); len(parts) == 2 {
			hashType = NormalizeChecksumType(parts[0])
		}
	}
	return hashType, hashValue
}

func ObjectHasChecksumTypeAndValue(obj models.InternalObject, hashType string, hashValue string) bool {
	if hashType == "" {
		return true
	}
	targetType := NormalizeChecksumType(hashType)
	targetValue := strings.Trim(strings.TrimSpace(NormalizeChecksum(hashValue)), `"'`)
	if targetType == "" || targetValue == "" {
		return false
	}
	for _, checksum := range obj.Checksums {
		if NormalizeChecksumType(checksum.Type) == targetType && strings.Trim(strings.TrimSpace(NormalizeChecksum(checksum.Checksum)), `"'`) == targetValue {
			return true
		}
	}
	return false
}

// MergeAdditionalChecksums merges new checksums into an existing set, avoiding duplicate types.
func MergeAdditionalChecksums(existing []drs.Checksum, additions []drs.Checksum) []drs.Checksum {
	out := make([]drs.Checksum, 0, len(existing)+len(additions))
	seenTypes := make(map[string]struct{}, len(existing)+len(additions))

	for _, cs := range existing {
		if t := NormalizeChecksumType(cs.Type); t != "" {
			seenTypes[t] = struct{}{}
		}
		out = append(out, cs)
	}

	for _, cs := range additions {
		t := NormalizeChecksumType(cs.Type)
		v := strings.TrimSpace(NormalizeChecksum(cs.Checksum))
		if t == "" || v == "" {
			continue
		}
		if _, exists := seenTypes[t]; exists {
			continue
		}
		out = append(out, drs.Checksum{Type: strings.TrimSpace(cs.Type), Checksum: v})
		seenTypes[t] = struct{}{}
	}
	return out
}

// CanonicalSHA256 pulls the sha256 value from a list of checksums if it exists.
func CanonicalSHA256(checksums []drs.Checksum) (string, bool) {
	for _, cs := range checksums {
		checksumType := strings.ToLower(strings.TrimSpace(cs.Type))
		if checksumType == "sha256" || checksumType == "sha-256" {
			normalized := sycommon.NormalizeOid(cs.Checksum)
			if normalized != "" {
				return normalized, true
			}
		}
	}
	return "", false
}

// NormalizeSHA256 cleans and dedups a list of potential SHA256 hashes.
func NormalizeSHA256(values []string) []string {
	targets := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, raw := range values {
		sha := strings.ToLower(strings.TrimSpace(NormalizeChecksum(strings.TrimSpace(raw))))
		if sha == "" {
			continue
		}
		if _, ok := seen[sha]; ok {
			continue
		}
		seen[sha] = struct{}{}
		targets = append(targets, sha)
	}
	return targets
}

// ParseS3URL extracts bucket/key pairs from an s3:// URL.
func ParseS3URL(raw string) (bucket string, key string, ok bool) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", "", false
	}
	if !strings.EqualFold(u.Scheme, "s3") {
		return "", "", false
	}
	bucket = strings.TrimSpace(u.Host)
	key = strings.TrimSpace(strings.TrimPrefix(u.Path, "/"))
	if bucket == "" || key == "" {
		return "", "", false
	}
	return bucket, key, true
}
