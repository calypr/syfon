package common

import (
	"io"
	"net/http"
)

type AccessTokenStruct struct {
	AccessToken string `json:"access_token"`
}

type FileUploadRequestObject struct {
	SourcePath   string
	ObjectKey    string
	FileMetadata FileMetadata
	GUID         string
	PresignedURL string
	Bucket       string `json:"bucket,omitempty"`
}

type FileDownloadResponseObject struct {
	DownloadPath string
	Filename     string
	GUID         string
	PresignedURL string
	Range        int64
	RangeStart   *int64
	RangeEnd     *int64
	Overwrite    bool
	Skip         bool
	Response     *http.Response
	Writer       io.Writer
}

type FileMetadata struct {
	Authz    []string       `json:"authz"`
	Aliases  []string       `json:"aliases"`
	Metadata map[string]any `json:"metadata"`
}

type RetryObject struct {
	SourcePath   string
	ObjectKey    string
	FileMetadata FileMetadata
	GUID         string
	RetryCount   int
	Multipart    bool
	Bucket       string
}

type MultipartUploadInit struct {
	GUID     string
	UploadID string
}

type MultipartUploadPart struct {
	PartNumber int32
	ETag       string
}

type ManifestObject struct {
	GUID      string `json:"object_id"`
	SubjectID string `json:"subject_id"`
	Title     string `json:"title"`
	Size      int64  `json:"size"`
}

type ShepherdInitRequestObject struct {
	Filename string         `json:"file_name"`
	Authz    ShepherdAuthz  `json:"authz"`
	Aliases  []string       `json:"aliases"`
	Metadata map[string]any `json:"metadata"`
}

type ShepherdAuthz struct {
	Version       string   `json:"version"`
	ResourcePaths []string `json:"resource_paths"`
}

type PresignedURLResponse struct {
	GUID string `json:"guid"`
	URL  string `json:"upload_url"`
}

type UploadURLResolveRequest struct {
	GUID     string
	Filename string
	Metadata FileMetadata
	Bucket   string
}

type UploadURLResolveResponse struct {
	GUID     string
	Filename string
	Bucket   string
	URL      string
	Status   int
	Error    string
}
