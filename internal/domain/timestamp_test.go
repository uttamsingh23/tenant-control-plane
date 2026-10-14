package domain

import (
	"errors"
	"testing"
	"time"
)

func TestFormatTimestamp(t *testing.T) {
	ist := time.FixedZone("IST", 5*3600+1800)
	pst := time.FixedZone("PST", -8*3600)
	tests := []struct {
		name string
		in   time.Time
		want string
	}{
		{"utc millis", time.Date(2026, 1, 2, 3, 4, 5, 6_000_000, time.UTC), "2026-01-02T03:04:05.006Z"},
		{"zero millis are padded", time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), "2026-01-02T03:04:05.000Z"},
		{"sub-millisecond truncated, not rounded", time.Date(2026, 1, 2, 3, 4, 5, 999_999_999, time.UTC), "2026-01-02T03:04:05.999Z"},
		{"positive offset converted to UTC", time.Date(2026, 1, 2, 8, 34, 5, 6_000_000, ist), "2026-01-02T03:04:05.006Z"},
		{"negative offset crosses date line", time.Date(2026, 12, 31, 20, 0, 0, 0, pst), "2027-01-01T04:00:00.000Z"},
		{"zero time", time.Time{}, "0001-01-01T00:00:00.000Z"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FormatTimestamp(tt.in); got != tt.want {
				t.Errorf("FormatTimestamp = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNormalizeTime(t *testing.T) {
	ist := time.FixedZone("IST", 5*3600+1800)
	in := time.Date(2026, 1, 2, 8, 34, 5, 6_789_123, ist)

	got := NormalizeTime(in)

	if want := time.Date(2026, 1, 2, 3, 4, 5, 6_000_000, time.UTC); got != want {
		t.Errorf("NormalizeTime = %v, want %v", got, want)
	}
	if got.Location() != time.UTC {
		t.Errorf("Location = %v, want UTC", got.Location())
	}
	if !got.Equal(in.Truncate(time.Millisecond)) {
		t.Errorf("NormalizeTime changed the instant: %v vs %v", got, in)
	}
}

func TestNormalizeTimeStripsMonotonicClock(t *testing.T) {
	now := time.Now()
	n := NormalizeTime(now)
	// A round trip through the wire format must yield an identical value,
	// which == only reports when the monotonic reading is gone.
	parsed, err := ParseTimestamp(FormatTimestamp(n))
	if err != nil {
		t.Fatal(err)
	}
	if parsed != n {
		t.Errorf("round trip: got %v, want %v", parsed, n)
	}
}

func TestParseTimestamp(t *testing.T) {
	tests := []struct {
		in    string
		want  time.Time
		valid bool
	}{
		{"2026-01-02T03:04:05.006Z", time.Date(2026, 1, 2, 3, 4, 5, 6_000_000, time.UTC), true},
		{"2026-01-02T03:04:05.000Z", time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), true},
		{"2026-01-02T03:04:05Z", time.Time{}, false},          // missing millis
		{"2026-01-02T03:04:05.006789Z", time.Time{}, false},   // micros
		{"2026-01-02T03:04:05.006+00:00", time.Time{}, false}, // offset instead of Z
		{"2026-01-02T08:34:05.006+05:30", time.Time{}, false}, // non-UTC offset
		{"2026-01-02T03:04:05.006", time.Time{}, false},       // no zone
		{"2026-01-02 03:04:05.006Z", time.Time{}, false},      // space separator
		{"2026-01-02T03:04:05.006z", time.Time{}, false},      // lowercase z
		{"", time.Time{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := ParseTimestamp(tt.in)
			if !tt.valid {
				if !errors.Is(err, ErrInvalidInput) {
					t.Errorf("err = %v, want ErrInvalidInput", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want || got.Location() != time.UTC {
				t.Errorf("got %v (%v), want %v UTC", got, got.Location(), tt.want)
			}
		})
	}
}

// TestConstructorsStoreNormalizedTimes checks that entities never hold a
// non-UTC or sub-millisecond timestamp, whatever clock the caller used.
func TestConstructorsStoreNormalizedTimes(t *testing.T) {
	ist := time.FixedZone("IST", 5*3600+1800)
	raw := time.Date(2026, 1, 2, 8, 34, 5, 6_789_123, ist)
	want := time.Date(2026, 1, 2, 3, 4, 5, 6_000_000, time.UTC)

	tenant, err := NewTenant("tenant-1", "acme", "Acme", raw)
	if err != nil {
		t.Fatal(err)
	}
	task, err := NewTask("task-1", TaskTypeDeploy, tenant.ID, raw)
	if err != nil {
		t.Fatal(err)
	}
	for name, got := range map[string]time.Time{
		"tenant.CreatedAt": tenant.CreatedAt, "tenant.UpdatedAt": tenant.UpdatedAt,
		"task.CreatedAt": task.CreatedAt, "task.UpdatedAt": task.UpdatedAt,
	} {
		if got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}

	if err := tenant.ApplyTaskOutcome(Task{Type: TaskTypeDeploy, TenantID: tenant.ID, Status: TaskStatusDone}, raw.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if got := FormatTimestamp(tenant.UpdatedAt); got != "2026-01-02T03:04:06.006Z" {
		t.Errorf("UpdatedAt after mutation = %q, want 2026-01-02T03:04:06.006Z", got)
	}
}
