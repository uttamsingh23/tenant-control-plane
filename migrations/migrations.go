// Package migrations embeds the versioned SQL schema migrations so the
// binary can apply them without shipping loose files.
//
// Files are goose migrations named NNNNN_description.sql and are applied in
// version order. Never edit a migration that has been committed; add a new
// one instead.
package migrations

import "embed"

// FS holds every migration file.
//
//go:embed *.sql
var FS embed.FS
