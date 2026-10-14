package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"regexp"
	"runtime/debug"
	"time"

	"github.com/google/uuid"
)

const requestIDHeader = "X-Request-ID"

// validRequestID limits client-supplied request IDs to a safe alphabet, so
// they cannot inject anything into logs or headers.
var validRequestID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// logRequests assigns every request an ID (the client's X-Request-ID if it
// is valid, otherwise a new UUID), echoes it in the response and logs one
// line per request once it is served.
func logRequests(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		id := r.Header.Get(requestIDHeader)
		if !validRequestID.MatchString(id) {
			id = uuid.NewString()
		}
		w.Header().Set(requestIDHeader, id)

		lw := &loggingWriter{ResponseWriter: w}
		next.ServeHTTP(lw, r)

		logger.LogAttrs(r.Context(), slog.LevelInfo, "http request",
			slog.String("request_id", id),
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status", lw.statusCode()),
			slog.Int("bytes", lw.bytes),
			slog.Float64("duration_ms", float64(time.Since(start).Microseconds())/1000),
		)
	})
}

// loggingWriter records the status and size of a response.
type loggingWriter struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (lw *loggingWriter) WriteHeader(status int) {
	if lw.status == 0 {
		lw.status = status
	}
	lw.ResponseWriter.WriteHeader(status)
}

func (lw *loggingWriter) Write(b []byte) (int, error) {
	if lw.status == 0 {
		lw.status = http.StatusOK
	}
	n, err := lw.ResponseWriter.Write(b)
	lw.bytes += n
	return n, err
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (lw *loggingWriter) Unwrap() http.ResponseWriter { return lw.ResponseWriter }

func (lw *loggingWriter) statusCode() int {
	if lw.status == 0 {
		return http.StatusOK
	}
	return lw.status
}

// withTimeout gives each request a context deadline. It does not preempt
// the handler; it makes everything that honours the context (database
// queries above all) give up, and the handler then answers 503
// request_timeout through the normal error path. The server's
// WriteTimeout remains the hard stop for anything that does not.
func withTimeout(d time.Duration, next http.Handler) http.Handler {
	if d <= 0 {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), d)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// structuredRouteErrors replaces ServeMux's plain-text 404 and 405
// responses with structured error bodies, preserving the Allow header.
func structuredRouteErrors(mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h, pattern := mux.Handler(r)
		if pattern != "" {
			mux.ServeHTTP(w, r)
			return
		}
		rec := &statusRecorder{header: http.Header{}}
		h.ServeHTTP(rec, r)
		if rec.status == http.StatusMethodNotAllowed {
			w.Header().Set("Allow", rec.header.Get("Allow"))
			writeJSON(w, http.StatusMethodNotAllowed, errorResponse{
				Error: errorDetail{Code: codeMethodNotAllowed, Message: "method not allowed"},
			})
			return
		}
		writeJSON(w, http.StatusNotFound, errorResponse{
			Error: errorDetail{Code: codeRouteNotFound, Message: "route not found"},
		})
	})
}

// statusRecorder captures the status and headers of a response, discarding the body.
type statusRecorder struct {
	header http.Header
	status int
}

func (s *statusRecorder) Header() http.Header         { return s.header }
func (s *statusRecorder) Write(b []byte) (int, error) { return len(b), nil }
func (s *statusRecorder) WriteHeader(status int)      { s.status = status }

// recoverPanics turns a handler panic into a logged, structured 500
// instead of a dropped connection. The stack goes to the log only.
func recoverPanics(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			v := recover()
			if v == nil {
				return
			}
			if err, ok := v.(error); ok && errors.Is(err, http.ErrAbortHandler) {
				panic(v)
			}
			logger.ErrorContext(r.Context(), "panic serving request",
				"request_id", w.Header().Get(requestIDHeader), "method", r.Method, "path", r.URL.Path,
				"panic", v, "stack", string(debug.Stack()))
			writeInternalError(w)
		}()
		next.ServeHTTP(w, r)
	})
}
