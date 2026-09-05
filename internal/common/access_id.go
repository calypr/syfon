package common

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// AccessMethodID is the stable public selector for a stored access method.
// The URL is part of the identity so two replicas of the same type cannot
// accidentally resolve to the same selector.
func AccessMethodID(accessType, accessURL string) string {
	accessType = strings.ToLower(strings.TrimSpace(accessType))
	accessURL = strings.TrimSpace(accessURL)
	digest := sha256.Sum256([]byte(accessType + "\x00" + accessURL))
	return accessType + "-" + hex.EncodeToString(digest[:12])
}
