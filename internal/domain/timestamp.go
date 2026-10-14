package domain

import (
	"fmt"
	"time"
)

// TimestampLayout is the wire format of every timestamp: ISO 8601 in UTC
// with millisecond precision and a literal Z suffix,
// e.g. 2026-01-02T03:04:05.006Z.
const TimestampLayout = "2006-01-02T15:04:05.000Z"

// NormalizeTime converts t to UTC and truncates it to millisecond precision.
// Domain constructors and mutations store only normalized times, so a value
// survives a format/parse or database round trip unchanged. Truncation also
// strips the monotonic clock reading, making == comparisons meaningful.
func NormalizeTime(t time.Time) time.Time {
	return t.UTC().Truncate(time.Millisecond)
}

// FormatTimestamp renders t in TimestampLayout, converting it to UTC first.
func FormatTimestamp(t time.Time) string {
	return t.UTC().Format(TimestampLayout)
}

// ParseTimestamp parses a string in exactly TimestampLayout. Offsets other
// than Z and missing or extra fractional digits are rejected.
func ParseTimestamp(s string) (time.Time, error) {
	t, err := time.Parse(TimestampLayout, s)
	if err != nil {
		return time.Time{}, &ValidationError{Field: "timestamp", Reason: fmt.Sprintf("%q is not in format %s", s, TimestampLayout)}
	}
	return t, nil
}
