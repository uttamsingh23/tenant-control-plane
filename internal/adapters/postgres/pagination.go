package postgres

import (
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/uttamsingh23/tenant-control-plane/internal/application"
	"github.com/uttamsingh23/tenant-control-plane/internal/domain"
)

// Lists use keyset pagination ordered by (created_at, id): stable under
// concurrent inserts and index-backed, unlike OFFSET. The cursor is the
// position of the last row of the previous page, opaque to clients.
type cursor struct {
	CreatedAt time.Time `json:"t"`
	ID        string    `json:"id"`
}

func encodeCursor(createdAt time.Time, id string) string {
	b, _ := json.Marshal(cursor{CreatedAt: createdAt, ID: id}) // cannot fail for this struct
	return base64.RawURLEncoding.EncodeToString(b)
}

// decodeCursor returns nil arguments for an empty cursor (first page) so
// the query's "IS NULL OR" keyset predicate is disabled.
func decodeCursor(s string) (createdAt, id any, err error) {
	if s == "" {
		return nil, nil, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(s)
	var c cursor
	if err == nil {
		err = json.Unmarshal(b, &c)
	}
	if err != nil || c.CreatedAt.IsZero() || !isUUID(c.ID) {
		return nil, nil, &domain.ValidationError{Field: "cursor", Reason: "malformed"}
	}
	return c.CreatedAt, c.ID, nil
}

// pageLimit applies the application default when the caller did not
// normalize the request.
func pageLimit(p application.PageRequest) int {
	if p.Limit <= 0 || p.Limit > application.MaxPageLimit {
		return application.DefaultPageLimit
	}
	return p.Limit
}

// toPage trims the extra row fetched to detect a next page and builds the
// cursor from the last returned item.
func toPage[T any](items []T, limit int, key func(T) (time.Time, string)) application.Page[T] {
	if len(items) <= limit {
		return application.Page[T]{Items: items}
	}
	items = items[:limit]
	createdAt, id := key(items[limit-1])
	return application.Page[T]{Items: items, NextCursor: encodeCursor(createdAt, id)}
}

// nullIfEmpty turns an absent string filter into SQL NULL.
func nullIfEmpty[S ~string](s S) any {
	if s == "" {
		return nil
	}
	return string(s)
}
