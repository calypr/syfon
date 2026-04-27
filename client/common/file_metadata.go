package common

type FileMetadata struct {
	Authorizations map[string][]string `json:"authorizations,omitempty"`
	Aliases        []string            `json:"aliases"`
	Metadata       map[string]any      `json:"metadata"`
}
