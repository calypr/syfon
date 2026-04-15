package drs

import (
	"fmt"
	"net/url"

	drsapi "github.com/calypr/syfon/apigen/drs"
)

func DRSAccessMethodsFromInternalURLs(urls []string, authz []string) ([]AccessMethod, error) {
	if len(urls) == 0 && len(authz) > 0 {
		// Create a placeholder access method to preserve authorizations
		return []AccessMethod{
			{
				Type: "s3", // Default type for the placeholder
				Authorizations: &struct {
					BearerAuthIssuers   *[]string                                            "json:\"bearer_auth_issuers,omitempty\""
					DrsObjectId         *string                                              "json:\"drs_object_id,omitempty\""
					PassportAuthIssuers *[]string                                            "json:\"passport_auth_issuers,omitempty\""
					SupportedTypes      *[]drsapi.AccessMethodAuthorizationsSupportedTypes "json:\"supported_types,omitempty\""
				}{
					BearerAuthIssuers: &[]string{authz[0]},
				},
			},
		}, nil
	}

	accessMethods := make([]AccessMethod, 0, len(urls))
	for _, urlString := range urls {
		method := AccessMethod{
			AccessUrl: &struct {
				Headers *[]string "json:\"headers,omitempty\""
				Url     string    "json:\"url\""
			}{
				Url: urlString,
			},
		}

		parsed, err := url.Parse(urlString)
		if err != nil {
			return nil, fmt.Errorf("failed to parse url %q: %v", urlString, err)
		}
		if parsed.Scheme == "" {
			method.Type = "https"
		} else {
			method.Type = drsapi.AccessMethodType(parsed.Scheme)
		}

		if len(authz) > 0 {
			method.Authorizations = &struct {
				BearerAuthIssuers   *[]string                                            "json:\"bearer_auth_issuers,omitempty\""
				DrsObjectId         *string                                              "json:\"drs_object_id,omitempty\""
				PassportAuthIssuers *[]string                                            "json:\"passport_auth_issuers,omitempty\""
				SupportedTypes      *[]drsapi.AccessMethodAuthorizationsSupportedTypes "json:\"supported_types,omitempty\""
			}{
				BearerAuthIssuers: &[]string{authz[0]},
			}
		}
		accessMethods = append(accessMethods, method)
	}
	return accessMethods, nil
}

// InternalAuthzFromDrsAccessMethods extracts authz values from DRS access methods.
func InternalAuthzFromDrsAccessMethods(accessMethods []AccessMethod) []string {
	authz := make([]string, 0, len(accessMethods))
	for _, drsURL := range accessMethods {
		if drsURL.Authorizations != nil && drsURL.Authorizations.BearerAuthIssuers != nil && len(*drsURL.Authorizations.BearerAuthIssuers) > 0 {
			authz = append(authz, (*drsURL.Authorizations.BearerAuthIssuers)[0])
		}
	}
	return authz
}

func InternalURLFromDrsAccessURLs(accessMethods []AccessMethod) []string {
	urls := make([]string, 0, len(accessMethods))
	for _, drsURL := range accessMethods {
		if drsURL.AccessUrl == nil || drsURL.AccessUrl.Url == "" {
			continue
		}
		urls = append(urls, drsURL.AccessUrl.Url)
	}
	return urls
}
