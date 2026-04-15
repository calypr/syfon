package core

import "time"

// Ptr returns a pointer to the value passed in.
func Ptr[T any](v T) *T {
	return &v
}

// Val returns the value of the pointer if not nil, otherwise the default value.
func Val[T any](p *T, def T) T {
	if p == nil {
		return def
	}
	return *p
}

// TimeVal returns the time value if not nil, otherwise zero time.
func TimeVal(p *time.Time) time.Time {
	if p == nil {
		return time.Time{}
	}
	return *p
}

// StringVal returns the string value if not nil, otherwise empty string.
func StringVal(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// IntVal returns the int value if not nil, otherwise 0.
func IntVal(p *int) int {
	if p == nil {
		return 0
	}
	return *p
}

// FloatVal returns the float64 value if not nil, otherwise 0.0.
func FloatVal(p *float64) float64 {
	if p == nil {
		return 0.0
	}
	return *p
}
