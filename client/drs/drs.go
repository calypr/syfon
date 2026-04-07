package drs

import (
	"github.com/calypr/syfon/client/conf"
	internaldrs "github.com/calypr/syfon/client/internal/drs"
	"github.com/calypr/syfon/client/pkg/logs"
	"github.com/calypr/syfon/client/pkg/request"
)

type Client = internaldrs.Client
type DRSObject = internaldrs.DRSObject
type DRSObjectCandidate = internaldrs.DRSObjectCandidate
type DRSObjectResult = internaldrs.DRSObjectResult
type Checksum = internaldrs.Checksum
type AccessURL = internaldrs.AccessURL
type AccessMethod = internaldrs.AccessMethod
type Authorizations = internaldrs.Authorizations
type ObjectBuilder = internaldrs.ObjectBuilder
type AddURLOption = internaldrs.AddURLOption

var NormalizeOid = internaldrs.NormalizeOid
var ProjectToResource = internaldrs.ProjectToResource
var DrsUUID = internaldrs.DrsUUID
var NAMESPACE = internaldrs.NAMESPACE
var StoragePrefix = internaldrs.StoragePrefix
var BuildDrsObj = internaldrs.BuildDrsObj
var BuildDrsObjWithPrefix = internaldrs.BuildDrsObjWithPrefix
var NewObjectBuilder = internaldrs.NewObjectBuilder
var FindMatchingRecord = internaldrs.FindMatchingRecord
var ConvertToCandidate = internaldrs.ConvertToCandidate
var ResolveObject = internaldrs.ResolveObject
var ResolveDownloadURL = internaldrs.ResolveDownloadURL
var WithPrefetchedBySHA = internaldrs.WithPrefetchedBySHA
var PrefetchedBySHA = internaldrs.PrefetchedBySHA

func NewDrsClient(req request.RequestInterface, cred *conf.Credential, logger *logs.Gen3Logger) Client {
	return internaldrs.NewDrsClient(req, cred, logger)
}

func NewLocalDrsClient(req request.RequestInterface, baseURL string, logger *logs.Gen3Logger) Client {
	return internaldrs.NewLocalDrsClient(req, baseURL, logger)
}
