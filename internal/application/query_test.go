package application

import "testing"

func TestPageRequestNormalized(t *testing.T) {
	tests := []struct{ in, want int }{
		{in: 0, want: DefaultPageLimit},
		{in: -5, want: DefaultPageLimit},
		{in: 10, want: 10},
		{in: MaxPageLimit + 1, want: MaxPageLimit},
	}
	for _, tt := range tests {
		if got := (PageRequest{Limit: tt.in}).normalized().Limit; got != tt.want {
			t.Errorf("normalized(%d) = %d, want %d", tt.in, got, tt.want)
		}
	}
}
