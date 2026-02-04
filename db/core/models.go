package core

import (
	"time"
)

// DrsObjectRecord represents the internal database record for a DRS Object
type DrsObjectRecord struct {
	ID          string    `db:"id"`
	Description string    `db:"description"`
	CreatedTime time.Time `db:"created_time"`
	UpdatedTime time.Time `db:"updated_time"`
	Size        int64     `db:"size"`
	Version     string    `db:"version"`
	Name        string    `db:"name"`
	MimeType    string    `db:"mime_type"`
}

// DrsObjectAccessMethod represents the internal database record for a DRS Access Method (URL)
type DrsObjectAccessMethod struct {
	ObjectID string `db:"object_id"`
	URL      string `db:"url"`
	Type     string `db:"type"` // e.g., "s3"
}

// DrsObjectAuthz represents the internal database record for DRS RBAC
type DrsObjectAuthz struct {
	ObjectID string `db:"object_id"`
	Resource string `db:"resource"`
}

// DrsObjectChecksum represents the internal database record for DRS Checksums
type DrsObjectChecksum struct {
	ObjectID string `db:"object_id"`
	Type     string `db:"type"`
	Checksum string `db:"checksum"`
}

// S3Credential represents the 's3_credential' table
type S3Credential struct {
	Bucket    string `db:"bucket"`
	Region    string `db:"region"`
	AccessKey string `db:"access_key"`
	SecretKey string `db:"secret_key"`
	Endpoint  string `db:"endpoint"`
}
