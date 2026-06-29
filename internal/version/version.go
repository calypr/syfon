package version

import "fmt"

// Build and version details
var (
	GitCommit   = ""
	GitBranch   = ""
	GitUpstream = ""
	BuildDate   = ""
	Version     = "dev"
)

var tpl = `git commit:   %s
git branch:   %s
git upstream: %s
build date:   %s
version:      %s
`

// String formats a string with version details.
func String() string {
	return fmt.Sprintf("Syfon — a Modern DRS Server ⚡️\n"+tpl, GitCommit, GitBranch, GitUpstream, BuildDate, Version)
}
