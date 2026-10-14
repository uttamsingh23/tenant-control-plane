package domain

import "testing"

func TestErrorMessages(t *testing.T) {
	tests := []struct {
		err  error
		want string
	}{
		{&ValidationError{Field: "slug", Reason: "must not be empty"}, "invalid slug: must not be empty"},
		{&TransitionError{Entity: "task", From: "done", Trigger: "in_progress"}, "task: in_progress not allowed from status done"},
	}
	for _, tt := range tests {
		if got := tt.err.Error(); got != tt.want {
			t.Errorf("Error() = %q, want %q", got, tt.want)
		}
	}
}
