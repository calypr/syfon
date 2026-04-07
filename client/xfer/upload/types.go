package upload

import "github.com/calypr/syfon/client/pkg/common"

type UploadConfig struct {
	BucketName        string
	NumParallel       int
	ForceMultipart    bool
	IncludeSubDirName bool
	HasMetadata       bool
	ShowProgress      bool
}

// FileInfo is a helper struct for including subdirname as filename
type FileInfo struct {
	FilePath     string
	Filename     string
	FileMetadata common.FileMetadata
	ObjectId     string
}

// RenamedOrSkippedFileInfo is a helper struct for recording renamed or skipped files
type RenamedOrSkippedFileInfo struct {
	GUID        string
	OldFilename string
	NewFilename string
}
