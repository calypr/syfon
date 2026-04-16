package service

// ImplResponse is the legacy service-layer transport for HTTP code + body.
// The strict server adapter converts this into generated response objects.
type ImplResponse struct {
	Code int
	Body any
}
