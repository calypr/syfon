package converters

import (
	"sort"

	"github.com/calypr/syfon/internal/models"
)

func SortInternalObjects(objects []models.InternalObject) {
	sort.Slice(objects, func(i, j int) bool {
		return objects[i].Id < objects[j].Id
	})
}
