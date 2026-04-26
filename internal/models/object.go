package models

import (
	"encoding/json"
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
)

// InternalObject is the primary DRS domain model. It wraps the GA4GH DrsObject
// and adds Syfon-specific authorization metadata.
type InternalObject struct {
	drs.DrsObject
	Authorizations map[string][]string    `json:"authorizations,omitempty"`
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
		Authorizations map[string][]string `json:"authorizations,omitempty"`
		Did            string              `json:"did,omitempty"`
		Hashes         map[string]string   `json:"hashes,omitempty"`
		Urls           []string            `json:"urls,omitempty"`
		FileName       *string             `json:"file_name,omitempty"`
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
	if o.AccessMethods == nil && len(wire.Urls) > 0 {
		methods := make([]drs.AccessMethod, 0, len(wire.Urls))
		for _, rawURL := range wire.Urls {
			if rawURL == "" {
				continue
			}
			methods = append(methods, drs.AccessMethod{
				Type: "https",
				AccessUrl: &struct {
					Headers *[]string `json:"headers,omitempty"`
					Url     string    `json:"url"`
				}{Url: rawURL},
			})
		}
		if len(methods) > 0 {
			o.AccessMethods = &methods
		}
	}
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

	if len(wire.Authorizations) > 0 {
		o.Authorizations = wire.Authorizations
	}

	return nil
}

func (o InternalObject) MarshalJSON() ([]byte, error) {
	out := make(map[string]interface{})

	// Preserve original request fields, then layer canonical aliases on top.
	for k, v := range o.Properties {
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
	if len(o.Authorizations) > 0 {
		out["authorizations"] = o.Authorizations
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

	if o.AccessMethods != nil && len(*o.AccessMethods) > 0 {
		urls := make([]string, 0, len(*o.AccessMethods))
		for _, method := range *o.AccessMethods {
			if method.AccessUrl == nil {
				continue
			}
			if url := method.AccessUrl.Url; url != "" {
				urls = append(urls, url)
			}
		}
		if len(urls) > 0 {
			out["urls"] = urls
		}
	}

	return json.Marshal(out)
}

func (o InternalObject) External() drs.DrsObject {
	return o.DrsObject
}
