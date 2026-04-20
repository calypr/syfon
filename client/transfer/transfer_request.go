package transfer

import "github.com/calypr/syfon/client/common"

// TransferRequest represents a request to move a single file.
type TransferRequest struct {
	SourcePath     string
	ObjectKey      string
	GUID           string
	Bucket         string
	Metadata       common.FileMetadata
	ForceMultipart bool
}
