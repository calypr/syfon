package download

import (
	"context"
	"strings"

	"github.com/calypr/syfon/client/transfer"
)

func GetFileInfo(
	ctx context.Context,
	dc Resolver,
	logger transfer.TransferLogger,
	guid, protocol, downloadPath, filenameFormat string,
	rename bool,
	renamedFiles *[]RenamedOrSkippedFileInfo,
) (*IndexdResponse, error) {
	drsObj, err := dc.Resolve(ctx, guid)
	if err != nil {
		logger.Warn("Failed to get file details", "guid", guid, "error", err)
		// Fallback: use GUID as filename if failed?
		*renamedFiles = append(*renamedFiles, RenamedOrSkippedFileInfo{GUID: guid, OldFilename: guid, NewFilename: guid})
		return &IndexdResponse{Name: guid, Size: 0}, nil
	}

	name := strings.TrimSpace(drsObj.Name)
	if name == "" {
		name = guid
	}
	finalName := applyFilenameFormat(name, guid, downloadPath, filenameFormat, rename, renamedFiles)
	return &IndexdResponse{Name: finalName, Size: drsObj.Size}, nil
}

func applyFilenameFormat(baseName, guid, downloadPath, format string, rename bool, renamedFiles *[]RenamedOrSkippedFileInfo) string {
	switch format {
	case "guid":
		return guid
	case "combined":
		return guid + "_" + baseName
	case "original":
		if !rename {
			return baseName
		}
		newName := processOriginalFilename(downloadPath, baseName)
		if newName != baseName {
			*renamedFiles = append(*renamedFiles, RenamedOrSkippedFileInfo{
				GUID:        guid,
				OldFilename: baseName,
				NewFilename: newName,
			})
		}
		return newName
	default:
		return baseName
	}
}
