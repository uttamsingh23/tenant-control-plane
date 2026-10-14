package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const maxBodyBytes = 1 << 20

// timestampLayout is ISO 8601 in UTC with millisecond precision and a Z suffix.
const timestampLayout = "2006-01-02T15:04:05.000Z"

func formatTime(t time.Time) string { return t.UTC().Format(timestampLayout) }

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	// The status line is already sent; an encode error here means the
	// client went away and there is nothing useful left to do.
	_ = json.NewEncoder(w).Encode(body)
}

// decodeJSON decodes the request body, which must be a single JSON object
// sent as application/json, into dst. Unknown fields are rejected. Errors
// are *requestError with a message safe to show the client.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return &requestError{
			status:  http.StatusUnsupportedMediaType,
			code:    codeUnsupportedMediaType,
			message: "Content-Type must be application/json",
		}
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return decodeError(err)
	}
	switch err := dec.Decode(&struct{}{}); {
	case errors.Is(err, io.EOF):
		return nil
	case err == nil:
		return invalidRequest("", "request body must contain a single JSON object")
	default:
		return decodeError(err)
	}
}

// decodeError translates an encoding/json error, whose text names Go types
// and struct fields, into a client-facing request error.
func decodeError(err error) error {
	var (
		maxBytesErr *http.MaxBytesError
		syntaxErr   *json.SyntaxError
		typeErr     *json.UnmarshalTypeError
	)
	switch {
	case errors.Is(err, io.EOF):
		return invalidRequest("", "request body is required")
	case errors.As(err, &maxBytesErr):
		return &requestError{
			status:  http.StatusRequestEntityTooLarge,
			code:    codePayloadTooLarge,
			message: fmt.Sprintf("request body must not exceed %d bytes", maxBytesErr.Limit),
		}
	case errors.As(err, &syntaxErr), errors.Is(err, io.ErrUnexpectedEOF):
		return invalidRequest("", "request body is not valid JSON")
	case errors.As(err, &typeErr):
		if typeErr.Field == "" {
			return invalidRequest("", "request body must be a JSON object")
		}
		return invalidRequest(typeErr.Field, "%s must be %s", typeErr.Field, jsonKind(typeErr.Type))
	}
	// encoding/json has no error type for unknown fields, only this text.
	if quoted, ok := strings.CutPrefix(err.Error(), "json: unknown field "); ok {
		field, _ := strconv.Unquote(quoted)
		return invalidRequest(field, "unsupported field %s", quoted)
	}
	return invalidRequest("", "request body is not valid JSON")
}

// jsonKind describes a Go type in JSON terms, for error messages.
func jsonKind(t reflect.Type) string {
	switch t.Kind() {
	case reflect.String:
		return "a string"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "an integer"
	case reflect.Bool:
		return "a boolean"
	default:
		return "of a different type"
	}
}

// optional is a request field that records whether it was present in the
// body, so that an omitted field, an explicit null and a zero value ("" or
// 0) can be told apart. Without it a PATCH could not distinguish "leave the
// name unchanged" from "set the name to empty".
type optional[T any] struct {
	Value T
	Set   bool // present in the body, possibly as null
	Null  bool // present as null
}

// UnmarshalJSON implements json.Unmarshaler. It is only called for fields
// present in the body, null included.
func (o *optional[T]) UnmarshalJSON(b []byte) error {
	o.Set = true
	if string(b) == "null" {
		o.Null = true
		return nil
	}
	return json.Unmarshal(b, &o.Value)
}

// required returns the value of a field that must be present and non-null.
func (o optional[T]) required(field string) (T, error) {
	if !o.Set || o.Null {
		var zero T
		return zero, invalidRequest(field, "%s is required", field)
	}
	return o.Value, nil
}

// patch returns nil if the field was omitted (leave unchanged), or a
// pointer to its value. Null is rejected: no patchable field is nullable.
func (o optional[T]) patch(field string) (*T, error) {
	switch {
	case !o.Set:
		return nil, nil
	case o.Null:
		return nil, invalidRequest(field, "%s must not be null", field)
	}
	return &o.Value, nil
}
