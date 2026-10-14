package httpapi

import (
	"net/http"
	"net/url"
	"slices"
	"strconv"

	"github.com/uttamsingh23/tenant-control-plane/internal/application"
)

// Query parameters shared by every list endpoint. Lists are keyset
// paginated in creation order: pass the next_cursor of one page as cursor
// to get the next one.
const (
	paramLimit  = "limit"
	paramCursor = "cursor"
)

// parseQuery parses the query string of a list request. Every parameter
// must be one of allowed or a pagination parameter, and may appear at most
// once. An empty value is treated as absent.
func parseQuery(r *http.Request, allowed ...string) (url.Values, application.PageRequest, error) {
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, application.PageRequest{}, invalidRequest("", "query string is malformed")
	}
	for key, values := range q {
		if key != paramLimit && key != paramCursor && !slices.Contains(allowed, key) {
			return nil, application.PageRequest{}, invalidRequest(key, "unsupported query parameter %q", key)
		}
		if len(values) > 1 {
			return nil, application.PageRequest{}, invalidRequest(key, "query parameter %q must be given at most once", key)
		}
	}
	page := application.PageRequest{Cursor: q.Get(paramCursor)}
	if v := q.Get(paramLimit); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > application.MaxPageLimit {
			return nil, application.PageRequest{}, invalidRequest(paramLimit,
				"limit must be an integer between 1 and %d", application.MaxPageLimit)
		}
		page.Limit = n
	}
	return q, page, nil
}

// statusParam returns the "status" filter, or "" if absent. An unknown
// status is rejected rather than matching nothing, so a typo is not
// mistaken for an empty result.
func statusParam[S interface {
	~string
	Valid() bool
}](q url.Values) (S, error) {
	s := S(q.Get("status"))
	if s != "" && !s.Valid() {
		return "", invalidRequest("status", "unknown status %q", string(s))
	}
	return s, nil
}
