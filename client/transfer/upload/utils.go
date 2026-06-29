package upload

import (
	"fmt"

	"github.com/calypr/syfon/client/common"
)

// FormatSize helps to parse a int64 size into string
func FormatSize(size int64) string {
	var unitSize int64
	switch {
	case size >= common.TB:
		unitSize = common.TB
	case size >= common.GB:
		unitSize = common.GB
	case size >= common.MB:
		unitSize = common.MB
	case size >= common.KB:
		unitSize = common.KB
	default:
		unitSize = common.B
	}

	var unitMap = map[int64]string{
		common.B:  "B",
		common.KB: "KB",
		common.MB: "MB",
		common.GB: "GB",
		common.TB: "TB",
	}

	return fmt.Sprintf("%.1f"+unitMap[unitSize], float64(size)/float64(unitSize))
}
