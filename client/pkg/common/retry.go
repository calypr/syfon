package common

type RetryObject struct {
	SourcePath   string
	ObjectKey    string
	FileMetadata FileMetadata
	GUID         string
	RetryCount   int
	Multipart    bool
	Bucket       string
}
