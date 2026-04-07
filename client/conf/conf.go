package conf

// Credential stores auth details used by client request/fence helpers.
type Credential struct {
	Profile            string
	KeyID              string
	APIKey             string
	AccessToken        string
	APIEndpoint        string
	UseShepherd        string
	MinShepherdVersion string
	Bucket             string
	ProjectID          string
}

// ManagerInterface is the minimal surface required by auth transport.
type ManagerInterface interface {
	Save(*Credential) error
}
