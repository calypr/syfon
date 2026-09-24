package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	hplugin "github.com/hashicorp/go-plugin"
)

// Handshake is the plugin handshake configuration for hashicorp/go-plugin
var Handshake = hplugin.HandshakeConfig{
	ProtocolVersion:  2,
	MagicCookieKey:   "SYFON_AUTHZ_PLUGIN",
	MagicCookieValue: "syfon_authz_plugin_v2",
}

// AuthorizationInput is the request sent to the plugin for an authz decision.
type AuthorizationInput struct {
	RequestID string
	Subject   string
	Action    string
	Resource  string
	Claims    map[string]interface{}
	Metadata  map[string]interface{}
}

// AuthorizationOutput is the plugin's response.
type AuthorizationOutput struct {
	Allow       bool
	Reason      string
	Obligations map[string]interface{}
}

// AuthorizationPlugin is the interface plugins must implement.
type AuthorizationPlugin interface {
	Authorize(ctx context.Context, in *AuthorizationInput) (*AuthorizationOutput, error)
}

// AuthenticationInput is the request sent to the plugin for authentication.
type AuthenticationInput struct {
	RequestID  string
	AuthHeader string
	Metadata   map[string]interface{}
}

// AuthenticationOutput is the plugin's response.
type AuthenticationOutput struct {
	Authenticated bool
	Subject       string
	Claims        map[string]interface{}
	Reason        string
}

// AuthenticationPlugin is the interface plugins must implement.
type AuthenticationPlugin interface {
	Authenticate(ctx context.Context, in *AuthenticationInput) (*AuthenticationOutput, error)
}

// AuthenticationRPCInput is the NetRPC-safe representation of AuthenticationInput.
// JSON maps use byte fields because gob cannot encode arbitrary interface values.
type AuthenticationRPCInput struct {
	RequestID  string
	AuthHeader string
	Metadata   []byte
}

// AuthenticationRPCOutput is the NetRPC-safe representation of AuthenticationOutput.
type AuthenticationRPCOutput struct {
	Authenticated bool
	Subject       string
	Claims        []byte
	Reason        string
}

// AuthorizationRPCInput is the NetRPC-safe representation of AuthorizationInput.
type AuthorizationRPCInput struct {
	RequestID string
	Subject   string
	Action    string
	Resource  string
	Claims    []byte
	Metadata  []byte
}

// AuthorizationRPCOutput is the NetRPC-safe representation of AuthorizationOutput.
type AuthorizationRPCOutput struct {
	Allow       bool
	Reason      string
	Obligations []byte
}

func (in *AuthenticationInput) ToRPC() (*AuthenticationRPCInput, error) {
	if in == nil {
		return nil, errors.New("authentication input is required")
	}
	metadata, err := encodeJSONMap(in.Metadata)
	if err != nil {
		return nil, fmt.Errorf("encode authentication metadata: %w", err)
	}
	return &AuthenticationRPCInput{RequestID: in.RequestID, AuthHeader: in.AuthHeader, Metadata: metadata}, nil
}

func (in *AuthenticationRPCInput) ToPlugin() (*AuthenticationInput, error) {
	if in == nil {
		return nil, errors.New("authentication RPC input is required")
	}
	metadata, err := decodeJSONMap(in.Metadata)
	if err != nil {
		return nil, fmt.Errorf("decode authentication metadata: %w", err)
	}
	return &AuthenticationInput{RequestID: in.RequestID, AuthHeader: in.AuthHeader, Metadata: metadata}, nil
}

func (out *AuthenticationOutput) ToRPC() (*AuthenticationRPCOutput, error) {
	if out == nil {
		return nil, errors.New("authentication output is required")
	}
	claims, err := encodeJSONMap(out.Claims)
	if err != nil {
		return nil, fmt.Errorf("encode authentication claims: %w", err)
	}
	return &AuthenticationRPCOutput{Authenticated: out.Authenticated, Subject: out.Subject, Claims: claims, Reason: out.Reason}, nil
}

func (out *AuthenticationRPCOutput) ToPlugin() (*AuthenticationOutput, error) {
	if out == nil {
		return nil, errors.New("authentication RPC output is required")
	}
	claims, err := decodeJSONMap(out.Claims)
	if err != nil {
		return nil, fmt.Errorf("decode authentication claims: %w", err)
	}
	return &AuthenticationOutput{Authenticated: out.Authenticated, Subject: out.Subject, Claims: claims, Reason: out.Reason}, nil
}

func (in *AuthorizationInput) ToRPC() (*AuthorizationRPCInput, error) {
	if in == nil {
		return nil, errors.New("authorization input is required")
	}
	claims, err := encodeJSONMap(in.Claims)
	if err != nil {
		return nil, fmt.Errorf("encode authorization claims: %w", err)
	}
	metadata, err := encodeJSONMap(in.Metadata)
	if err != nil {
		return nil, fmt.Errorf("encode authorization metadata: %w", err)
	}
	return &AuthorizationRPCInput{
		RequestID: in.RequestID,
		Subject:   in.Subject,
		Action:    in.Action,
		Resource:  in.Resource,
		Claims:    claims,
		Metadata:  metadata,
	}, nil
}

func (in *AuthorizationRPCInput) ToPlugin() (*AuthorizationInput, error) {
	if in == nil {
		return nil, errors.New("authorization RPC input is required")
	}
	claims, err := decodeJSONMap(in.Claims)
	if err != nil {
		return nil, fmt.Errorf("decode authorization claims: %w", err)
	}
	metadata, err := decodeJSONMap(in.Metadata)
	if err != nil {
		return nil, fmt.Errorf("decode authorization metadata: %w", err)
	}
	return &AuthorizationInput{
		RequestID: in.RequestID,
		Subject:   in.Subject,
		Action:    in.Action,
		Resource:  in.Resource,
		Claims:    claims,
		Metadata:  metadata,
	}, nil
}

func (out *AuthorizationOutput) ToRPC() (*AuthorizationRPCOutput, error) {
	if out == nil {
		return nil, errors.New("authorization output is required")
	}
	obligations, err := encodeJSONMap(out.Obligations)
	if err != nil {
		return nil, fmt.Errorf("encode authorization obligations: %w", err)
	}
	return &AuthorizationRPCOutput{Allow: out.Allow, Reason: out.Reason, Obligations: obligations}, nil
}

func (out *AuthorizationRPCOutput) ToPlugin() (*AuthorizationOutput, error) {
	if out == nil {
		return nil, errors.New("authorization RPC output is required")
	}
	obligations, err := decodeJSONMap(out.Obligations)
	if err != nil {
		return nil, fmt.Errorf("decode authorization obligations: %w", err)
	}
	return &AuthorizationOutput{Allow: out.Allow, Reason: out.Reason, Obligations: obligations}, nil
}

// AuthenticationRPCServer adapts a public authentication plugin to NetRPC.
type AuthenticationRPCServer struct {
	implementation AuthenticationPlugin
}

// NewAuthenticationRPCServer returns the NetRPC service to expose as "Plugin".
func NewAuthenticationRPCServer(implementation AuthenticationPlugin) *AuthenticationRPCServer {
	return &AuthenticationRPCServer{implementation: implementation}
}

func (s *AuthenticationRPCServer) Authenticate(in *AuthenticationRPCInput, out *AuthenticationRPCOutput) error {
	if s == nil || s.implementation == nil {
		return errors.New("authentication plugin is not configured")
	}
	if out == nil {
		return errors.New("authentication RPC output is required")
	}
	input, err := in.ToPlugin()
	if err != nil {
		return err
	}
	result, err := s.implementation.Authenticate(context.Background(), input)
	if err != nil {
		return err
	}
	wireOutput, err := result.ToRPC()
	if err != nil {
		return err
	}
	*out = *wireOutput
	return nil
}

// AuthorizationRPCServer adapts a public authorization plugin to NetRPC.
type AuthorizationRPCServer struct {
	implementation AuthorizationPlugin
}

// NewAuthorizationRPCServer returns the NetRPC service to expose as "Plugin".
func NewAuthorizationRPCServer(implementation AuthorizationPlugin) *AuthorizationRPCServer {
	return &AuthorizationRPCServer{implementation: implementation}
}

func (s *AuthorizationRPCServer) Authorize(in *AuthorizationRPCInput, out *AuthorizationRPCOutput) error {
	if s == nil || s.implementation == nil {
		return errors.New("authorization plugin is not configured")
	}
	if out == nil {
		return errors.New("authorization RPC output is required")
	}
	input, err := in.ToPlugin()
	if err != nil {
		return err
	}
	result, err := s.implementation.Authorize(context.Background(), input)
	if err != nil {
		return err
	}
	wireOutput, err := result.ToRPC()
	if err != nil {
		return err
	}
	*out = *wireOutput
	return nil
}

func encodeJSONMap(values map[string]interface{}) ([]byte, error) {
	if values == nil {
		return nil, nil
	}
	return json.Marshal(values)
}

func decodeJSONMap(encoded []byte) (map[string]interface{}, error) {
	if len(encoded) == 0 {
		return nil, nil
	}
	var values map[string]interface{}
	if err := json.Unmarshal(encoded, &values); err != nil {
		return nil, err
	}
	if values == nil {
		return nil, errors.New("encoded value must be a JSON object")
	}
	return values, nil
}
