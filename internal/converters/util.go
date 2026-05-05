package converters

import (
	"sort"

	"github.com/calypr/syfon/apigen/server/drs"
	syfoncommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/models"
)

func UniqueAuthz(values any) map[string][]string {
	switch v := values.(type) {
	case nil:
		return nil
	case []drs.AccessMethod:
		out := make(map[string][]string)
		for _, method := range v {
			if method.Authorizations == nil {
				continue
			}
			if method.Authorizations.BearerAuthIssuers == nil {
				continue
			}
			for org, projects := range syfoncommon.AuthzListToMap(*method.Authorizations.BearerAuthIssuers) {
				if len(projects) == 0 {
					if _, ok := out[org]; !ok {
						out[org] = []string{}
					}
					continue
				}
				seen := make(map[string]struct{}, len(out[org]))
				for _, p := range out[org] {
					seen[p] = struct{}{}
				}
				for _, p := range projects {
					if _, ok := seen[p]; !ok {
						out[org] = append(out[org], p)
						seen[p] = struct{}{}
					}
				}
			}
		}
		if len(out) == 0 {
			return nil
		}
		return out
	case *[]drs.AccessMethod:
		if v == nil {
			return nil
		}
		return UniqueAuthz(*v)
	default:
		return nil
	}
}

func SortInternalObjects(objects []models.InternalObject) {
	sort.Slice(objects, func(i, j int) bool {
		return objects[i].Id < objects[j].Id
	})
}
