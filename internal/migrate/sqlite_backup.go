package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

func escapeSQLiteStringLiteral(value string) string {
	// SQLite uses '' to escape a single quote inside a string literal.
	return strings.ReplaceAll(value, "'", "''")
}

// BackupSQLiteDB writes a consistent backup of the opened SQLite database to backupPath.
// It prefers `VACUUM INTO` (atomic snapshot) and returns an error if it fails.
func BackupSQLiteDB(ctx context.Context, db *sql.DB, backupPath string) error {
	if strings.TrimSpace(backupPath) == "" {
		return fmt.Errorf("backup path is empty")
	}

	// Ensure pending WAL content is checkpointed as much as possible.
	_, _ = db.ExecContext(ctx, `PRAGMA wal_checkpoint(FULL)`)

	stmt := fmt.Sprintf("VACUUM INTO '%s'", escapeSQLiteStringLiteral(backupPath))
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("VACUUM INTO backup %s: %w", backupPath, err)
	}
	return nil
}
