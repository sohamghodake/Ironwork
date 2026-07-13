// Package migrations embeds the SQL migration files applied by cmd/migrate
// (and by tests) via goose.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
