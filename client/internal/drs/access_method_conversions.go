package drs

import (
	"fmt"
	"net/url"
)

func DRSAccessMethodsFromInternalURLs(urls []string, authz []string) ([]AccessMethod, error) {
	accessMethods := make([]AccessMethod, 0, len(urls))
	for _, urlString := range urls {
		method := AccessMethod{
			AccessUrl: AccessURL{Url: urlString},
		}

		parsed, err := url.Parse(urlString)
		if err != nil {
			return nil, fmt.Errorf("failed to parse url %q: %v", urlString, err)
		}
		if parsed.Scheme == "" {
			method.Type = "https"
		} else {
			method.Type = parsed.Scheme
		}

		if len(authz) > 0 {
			method.Authorizations = Authorizations{BearerAuthIssuers: []string{authz[0]}}
		}
		accessMethods = append(accessMethods, method)
	}
	return accessMethods, nil
}

// InternalAuthzFromDrsAccessMethods extracts authz values from DRS access methods.
func InternalAuthzFromDrsAccessMethods(accessMethods []AccessMethod) []string {
	authz := make([]string, 0, len(accessMethods))
	for _, drsURL := range accessMethods {
		if len(drsURL.Authorizations.BearerAuthIssuers) > 0 {
			authz = append(authz, drsURL.Authorizations.BearerAuthIssuers[0])
		}
	}
	return authz
}

func InternalURLFromDrsAccessURLs(accessMethods []AccessMethod) []string {
	urls := make([]string, 0, len(accessMethods))
	for _, drsURL := range accessMethods {
		if drsURL.AccessUrl.Url == "" {
			continue
		}
		urls = append(urls, drsURL.AccessUrl.Url)
	}
	return urls
}
