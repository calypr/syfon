package drs

import (
	"github.com/calypr/syfon/client/conf"
	internaldrs "github.com/calypr/syfon/client/internal/drs"
	"github.com/calypr/syfon/client/pkg/logs"
	"github.com/calypr/syfon/client/pkg/request"
)

type Client = internaldrs.Client
type DRSObject = internaldrs.DRSObject
type DRSObjectResult = internaldrs.DRSObjectResult
type Checksum = internaldrs.Checksum

var NormalizeOid = internaldrs.NormalizeOid
var ProjectToResource = internaldrs.ProjectToResource
var DrsUUID = internaldrs.DrsUUID
var BuildDrsObj = internaldrs.BuildDrsObj
var BuildDrsObjWithPrefix = internaldrs.BuildDrsObjWithPrefix
var FindMatchingRecord = internaldrs.FindMatchingRecord
var ConvertToCandidate = internaldrs.ConvertToCandidate

func NewDrsClient(req request.RequestInterface, cred *conf.Credential, logger *logs.Gen3Logger) Client {
	return internaldrs.NewDrsClient(req, cred, logger)
}

func NewLocalDrsClient(req request.RequestInterface, baseURL string, logger *logs.Gen3Logger) Client {
	return internaldrs.NewLocalDrsClient(req, baseURL, logger)
}
