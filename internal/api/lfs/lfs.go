package lfs

import (
	"github.com/calypr/syfon/internal/db"
	"github.com/calypr/syfon/internal/service"
	"github.com/calypr/syfon/internal/urlmanager"
	"github.com/gofiber/fiber/v3"
)

type Options = service.Options

func DefaultOptions() Options {
	return service.DefaultOptions()
}

func RegisterLFSRoutes(router fiber.Router, database db.LFSStore, uM urlmanager.UrlManager, opts ...Options) {
	effective := DefaultOptions()
	if len(opts) > 0 {
		effective = opts[0]
	}
	service.RegisterLFSRoutes(router, database, uM, effective)
}

func resetLFSLimitersForTest() {
	service.ResetLFSLimitersForTest()
}
