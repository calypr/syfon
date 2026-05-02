package models

import (
	"encoding/json"
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/common"
)

// InternalObject is the primary DRS domain model. It wraps the GA4GH DrsObject
// and adds Syfon-specific authorization metadata.
type InternalObject struct {
	drs.DrsObject
	Authorizations map[string][]string    `json:"-"`
	Properties     map[string]interface{} `json:"-"`
}

// DrsObjectWithAuthz is an alias for InternalObject retained for older Go call sites.
type DrsObjectWithAuthz = InternalObject

func (o *InternalObject) UnmarshalJSON(data []byte) error {
	*o = InternalObject{}

	// Capture the original payload so we can preserve unknown fields on marshal.
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw == nil {
		raw = map[string]interface{}{}
	}
	o.Properties = raw

	type wireObject struct {
		drs.DrsObject
		Did      string            `json:"did,omitempty"`
		Hashes   map[string]string `json:"hashes,omitempty"`
		FileName *string           `json:"file_name,omitempty"`
	}

	var wire wireObject
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}

	if wire.Id != "" {
		o.Id = wire.Id
	} else if wire.Did != "" {
		o.Id = wire.Did
	}
	o.Checksums = append([]drs.Checksum(nil), wire.Checksums...)
	if len(o.Checksums) == 0 && len(wire.Hashes) > 0 {
		o.Checksums = make([]drs.Checksum, 0, len(wire.Hashes))
		for typ, checksum := range wire.Hashes {
			if checksum == "" {
				continue
			}
			o.Checksums = append(o.Checksums, drs.Checksum{Type: typ, Checksum: checksum})
		}
	}
	o.AccessMethods = wire.AccessMethods
	o.ControlledAccess = wire.ControlledAccess
	o.CreatedTime = wire.CreatedTime
	o.UpdatedTime = wire.UpdatedTime
	o.Size = wire.Size
	o.Name = wire.Name
	if o.Name == nil {
		o.Name = wire.FileName
	}
	o.Description = wire.Description
	o.MimeType = wire.MimeType
	o.SelfUri = wire.SelfUri
	o.Version = wire.Version

	if wire.ControlledAccess != nil {
		o.Authorizations = common.ControlledAccessToAuthzMap(*wire.ControlledAccess)
	}

	return nil
}

func (o InternalObject) MarshalJSON() ([]byte, error) {
	out := make(map[string]interface{})

	// Preserve original request fields, then layer canonical aliases on top.
	for k, v := range o.Properties {
		if isRetiredInternalAuthField(k) {
			continue
		}
		out[k] = v
	}

	if o.Id != "" {
		out["id"] = o.Id
	}
	if len(o.Checksums) > 0 {
		out["checksums"] = o.Checksums
	}
	if o.AccessMethods != nil {
		out["access_methods"] = o.AccessMethods
	}
	if o.ControlledAccess != nil {
		out["controlled_access"] = o.ControlledAccess
	}
	if !o.CreatedTime.IsZero() {
		out["created_time"] = o.CreatedTime.Format(time.RFC3339)
	}
	if o.UpdatedTime != nil {
		out["updated_time"] = o.UpdatedTime.Format(time.RFC3339)
	}
	if o.Name != nil {
		out["name"] = *o.Name
	}
	if o.Description != nil {
		out["description"] = *o.Description
	}
	if o.Size > 0 {
		out["size"] = o.Size
	}
	// Ensure Gen3 compatibility fields are also present.
	out["did"] = o.Id
	if o.Name != nil {
		out["file_name"] = *o.Name
	}

	if len(o.Checksums) > 0 {
		hashes := make(map[string]string, len(o.Checksums))
		for _, c := range o.Checksums {
			if c.Type == "" || c.Checksum == "" {
				continue
			}
			hashes[c.Type] = c.Checksum
		}
		if len(hashes) > 0 {
			out["hashes"] = hashes
		}
	}

	return json.Marshal(out)
}

func isRetiredInternalAuthField(key string) bool {
	switch key {
	case "auth", "authz", "authorizations", "urls":
		return true
	default:
		return false
	}
}

func (o InternalObject) External() drs.DrsObject {
	return o.DrsObject
}
