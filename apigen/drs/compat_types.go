package drs

type UnresolvedInner struct {
	ErrorCode *int      `json:"error_code,omitempty"`
	ObjectIds *[]string `json:"object_ids,omitempty"`
}

// Compatibility types for strict-server response objects that oapi-codegen
// fails to fully define when nested in anonymous structs.
type N200ServiceInfoJSONResponseDrsSupportedUploadMethods string
type N200ServiceInfoJSONResponseTypeArtifact string
