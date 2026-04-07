package drs

import (
	"strings"

	"github.com/calypr/syfon/client/pkg/hash"
	"github.com/google/uuid"
)

type IdentifierKind string

const (
	IdentifierUUID     IdentifierKind = "uuid"
	IdentifierChecksum IdentifierKind = "checksum"
	IdentifierUnknown  IdentifierKind = "unknown"
	// Backward-compatible alias.
	IdentifierSHA256 = IdentifierChecksum
)

// ObjectIdentifier carries parsed identifier variants while preserving raw input.
// UUID and SHA256 are mutually exclusive in normalized form.
type ObjectIdentifier struct {
	Raw  string
	UUID uuid.UUID
	Hash *hash.Checksum
}

func (o ObjectIdentifier) Kind() IdentifierKind {
	if o.UUID != uuid.Nil {
		return IdentifierUUID
	}
	if o.Hash != nil {
		return IdentifierChecksum
	}
	return IdentifierUnknown
}

func ParseObjectIdentifier(id string) ObjectIdentifier {
	raw := strings.TrimSpace(id)
	out := ObjectIdentifier{Raw: raw}
	if raw == "" {
		return out
	}

	if parsed, err := uuid.Parse(raw); err == nil {
		out.UUID = parsed
		return out
	}

	if parsedHash, ok := parseValidatedHashIdentifier(raw); ok {
		out.Hash = parsedHash
	}
	return out
}

func parseValidatedHashIdentifier(v string) (*hash.Checksum, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return nil, false
	}

	if strings.Contains(v, ":") {
		parts := strings.SplitN(v, ":", 2)
		if len(parts) != 2 {
			return nil, false
		}
		ck := &hash.Checksum{
			Type:     hash.NormalizeChecksumType(parts[0]),
			Checksum: strings.TrimSpace(parts[1]),
		}
		if err := hash.ValidateChecksum(*ck); err != nil {
			return nil, false
		}
		return ck, true
	}

	// Backward-compatible shorthand: bare 64-hex means sha256.
	ck := &hash.Checksum{
		Type:     hash.ChecksumTypeSHA256,
		Checksum: strings.ToLower(strings.TrimSpace(v)),
	}
	if err := hash.ValidateChecksum(*ck); err != nil {
		return nil, false
	}
	return ck, true
}
