package hash

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	drsapi "github.com/calypr/syfon/apigen/drs"
)

// ChecksumType represents the digest method used to create the checksum
type ChecksumType string

// IANA Named Information Hash Algorithm Registry values and other common types
const (
	ChecksumTypeSHA1     ChecksumType = "sha1"
	ChecksumTypeSHA256   ChecksumType = "sha256"
	ChecksumTypeSHA512   ChecksumType = "sha512"
	ChecksumTypeMD5      ChecksumType = "md5"
	ChecksumTypeETag     ChecksumType = "etag"
	ChecksumTypeCRC32C   ChecksumType = "crc32c"
	ChecksumTypeTrunc512 ChecksumType = "trunc512"
)

// IsValid checks if the checksum type is a known/recommended value
func (ct ChecksumType) IsValid() bool {
	switch ct {
	case ChecksumTypeSHA256, ChecksumTypeSHA512, ChecksumTypeSHA1, ChecksumTypeMD5,
		ChecksumTypeETag, ChecksumTypeCRC32C, ChecksumTypeTrunc512:
		return true
	default:
		return false
	}
}

// String returns the string representation of the checksum type
func (ct ChecksumType) String() string {
	return string(ct)
}

var SupportedChecksums = map[string]bool{
	string(ChecksumTypeSHA1):     true,
	string(ChecksumTypeSHA256):   true,
	string(ChecksumTypeSHA512):   true,
	string(ChecksumTypeMD5):      true,
	string(ChecksumTypeETag):     true,
	string(ChecksumTypeCRC32C):   true,
	string(ChecksumTypeTrunc512): true,
}

type Checksum drsapi.Checksum

type HashInfo struct {
	MD5    string `json:"md5,omitempty"`
	SHA    string `json:"sha,omitempty"`
	SHA256 string `json:"sha256,omitempty"`
	SHA512 string `json:"sha512,omitempty"`
	CRC    string `json:"crc,omitempty"`
	ETag   string `json:"etag,omitempty"`
}

// UnmarshalJSON accepts both the DRS map-based schema (Indexd) and the array-of-checksums schema (GA4GH).
func (h *HashInfo) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*h = HashInfo{}
		return nil
	}

	var mapPayload map[string]string
	if err := json.Unmarshal(data, &mapPayload); err == nil {
		*h = ConvertStringMapToHashInfo(mapPayload)
		return nil
	}

	var checksumPayload []Checksum
	if err := json.Unmarshal(data, &checksumPayload); err == nil {
		*h = ConvertChecksumsToHashInfo(checksumPayload)
		return nil
	}

	return fmt.Errorf("unsupported HashInfo payload: %s", string(data))
}

func ConvertStringMapToHashInfo(inputHashes map[string]string) HashInfo {
	hashInfo := HashInfo{}

	for key, value := range inputHashes {
		if !SupportedChecksums[key] {
			continue // Disregard unsupported types
		}
		switch key {
		case string(ChecksumTypeMD5):
			hashInfo.MD5 = value
		case string(ChecksumTypeSHA1):
			hashInfo.SHA = value
		case string(ChecksumTypeSHA256):
			hashInfo.SHA256 = value
		case string(ChecksumTypeSHA512):
			hashInfo.SHA512 = value
		case string(ChecksumTypeCRC32C):
			hashInfo.CRC = value
		case string(ChecksumTypeETag):
			hashInfo.ETag = value
		}
	}

	return hashInfo
}

func ConvertHashInfoToMap(hashes HashInfo) map[string]string {
	result := make(map[string]string)
	if hashes.MD5 != "" {
		result["md5"] = hashes.MD5
	}
	if hashes.SHA != "" {
		result["sha"] = hashes.SHA
	}
	if hashes.SHA256 != "" {
		result["sha256"] = hashes.SHA256
	}
	if hashes.SHA512 != "" {
		result["sha512"] = hashes.SHA512
	}
	if hashes.CRC != "" {
		result["crc"] = hashes.CRC
	}
	if hashes.ETag != "" {
		result["etag"] = hashes.ETag
	}
	return result
}

func ConvertChecksumsToMap(checksums []Checksum) map[string]string {
	result := make(map[string]string, len(checksums))
	for _, c := range checksums {
		result[c.Type] = c.Checksum
	}
	return result
}

func ConvertChecksumsToHashInfo(checksums []Checksum) HashInfo {
	checksumMap := ConvertChecksumsToMap(checksums)
	return ConvertStringMapToHashInfo(checksumMap)
}

func ConvertDrsChecksumsToMap(checksums []drsapi.Checksum) map[string]string {
	result := make(map[string]string, len(checksums))
	for _, c := range checksums {
		result[c.Type] = c.Checksum
	}
	return result
}

func ConvertDrsChecksumsToHashInfo(checksums []drsapi.Checksum) HashInfo {
	checksumMap := ConvertDrsChecksumsToMap(checksums)
	return ConvertStringMapToHashInfo(checksumMap)
}

func ConvertMapToDrsChecksums(hashes map[string]string) []drsapi.Checksum {
	result := make([]drsapi.Checksum, 0, len(hashes))
	for t, c := range hashes {
		result = append(result, drsapi.Checksum{
			Type:     t,
			Checksum: c,
		})
	}
	return result
}

func NormalizeChecksumType(raw string) ChecksumType {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "sha256":
		return ChecksumTypeSHA256
	case "sha512":
		return ChecksumTypeSHA512
	case "sha1", "sha":
		return ChecksumTypeSHA1
	case "md5":
		return ChecksumTypeMD5
	case "etag":
		return ChecksumTypeETag
	case "crc32c":
		return ChecksumTypeCRC32C
	case "trunc512":
		return ChecksumTypeTrunc512
	default:
		return ChecksumType(raw)
	}
}

func ValidateChecksum(c Checksum) error {
	checksum := strings.TrimSpace(c.Checksum)
	if checksum == "" {
		return fmt.Errorf("checksum value is required")
	}
	ct := NormalizeChecksumType(c.Type)
	if !ct.IsValid() {
		return fmt.Errorf("unsupported checksum type %q", c.Type)
	}
	switch ct {
	case ChecksumTypeSHA256:
		if !isHexDigest(checksum, 64) {
			return fmt.Errorf("invalid sha256 checksum")
		}
	case ChecksumTypeSHA512:
		if !isHexDigest(checksum, 128) {
			return fmt.Errorf("invalid sha512 checksum")
		}
	case ChecksumTypeSHA1:
		if !isHexDigest(checksum, 40) {
			return fmt.Errorf("invalid sha1 checksum")
		}
	case ChecksumTypeMD5:
		if !isHexDigest(checksum, 32) {
			return fmt.Errorf("invalid md5 checksum")
		}
	case ChecksumTypeCRC32C, ChecksumTypeETag, ChecksumTypeTrunc512:
		// Provider implementations vary representation (hex/base64/quoted ETag).
		// Non-empty check above is the strict lower bound.
	}
	return nil
}

func isHexDigest(v string, wantLen int) bool {
	if len(v) != wantLen {
		return false
	}
	_, err := hex.DecodeString(v)
	return err == nil
}
