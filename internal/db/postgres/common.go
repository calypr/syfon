package postgres

import (
	"strings"
	"time"

	"github.com/calypr/syfon/internal/db/core"
)

func defaultProvider(provider string) string {
	if strings.TrimSpace(provider) == "" {
		return "s3"
	}
	return provider
}

func uniqueObjectsByID(objs []core.InternalObject) []core.InternalObject {
	seen := make(map[string]struct{}, len(objs))
	out := make([]core.InternalObject, 0, len(objs))
	for _, o := range objs {
		if _, ok := seen[o.Id]; ok {
			continue
		}
		seen[o.Id] = struct{}{}
		out = append(out, o)
	}
	return out
}

func latestUsageTime(ts ...*time.Time) *time.Time {
	var latest *time.Time
	for _, t := range ts {
		if t == nil {
			continue
		}
		if latest == nil || t.After(*latest) {
			copyT := *t
			latest = &copyT
		}
	}
	return latest
}
