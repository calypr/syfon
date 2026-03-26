package provider

import "strings"

const (
	S3    = "s3"
	GCS   = "gcs"
	Azure = "azure"
	File  = "file"
)

// Normalize returns a standard provider name from a raw string.
func Normalize(p string, fallback string) string {
	p = strings.ToLower(strings.TrimSpace(p))
	switch p {
	case S3, GCS, Azure, File:
		return p
	case "gs":
		return GCS
	case "azblob":
		return Azure
	default:
		if fallback != "" {
			return Normalize(fallback, "")
		}
		return S3
	}
}

// FromScheme maps a URL scheme to a provider name.
func FromScheme(scheme string) string {
	switch strings.ToLower(strings.TrimSpace(scheme)) {
	case "s3":
		return S3
	case "gs":
		return GCS
	case "azblob":
		return Azure
	case "file":
		return File
	default:
		return ""
	}
}

// ToScheme maps a provider name to its primary URL scheme.
func ToScheme(p string) string {
	switch Normalize(p, "") {
	case S3:
		return "s3"
	case GCS:
		return "gs"
	case Azure:
		return "azblob"
	case File:
		return "file"
	default:
		return "s3"
	}
}
