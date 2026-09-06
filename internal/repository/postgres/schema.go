// Package postgres holds the database schema. The type-safe query code sqlc
// generates from it lives in the sqlc subpackage.
package postgres

import _ "embed"

// Schema is schema.sql, compiled into any binary that imports this package.
//
// Embedded rather than read from disk so that applying the schema needs
// nothing but the binary: no file to mount, no ConfigMap to keep in step, and
// no second copy that can drift from the one sqlc generates against. The bytes
// sqlc reads at build time are the bytes that reach the database.
//
// It matters that this file is the ONLY copy. A chart cannot read files above
// its own directory, so the obvious alternative — a copy under deploy/helm —
// would be a second source of truth that nothing checks for drift. The failure
// mode there is quiet: the generated Go and the deployed tables disagree, and
// nothing says so until a query hits a column that is not there.
//
// Every statement is IF NOT EXISTS, so running this against an already-migrated
// database does nothing. That is what makes it safe to apply on every install
// and upgrade instead of tracking which version has been applied.
//
//go:embed schema.sql
var Schema string
