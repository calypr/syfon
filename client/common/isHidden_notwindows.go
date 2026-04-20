//go:build !windows
// +build !windows

package common

import "path/filepath"

func IsHidden(filePath string) (bool, error) {
	filename := filepath.Base(filePath)
	if filename[0:1] == "." || filename[0:1] == "~" {
		return true, nil
	}
	return false, nil
}
