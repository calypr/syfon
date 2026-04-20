package common

type FileMetadata struct {
	Authz    []string       `json:"authz"`
	Aliases  []string       `json:"aliases"`
	Metadata map[string]any `json:"metadata"`
}
