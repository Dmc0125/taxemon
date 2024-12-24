package db_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"taxemon/pkg/dbgen"
	"testing"

	"github.com/test-go/testify/assert"
	"github.com/test-go/testify/require"
	_ "modernc.org/sqlite"
)

func TestFetchDuplicateTimestampsTransactions(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Setup database
	sqlDB, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	defer sqlDB.Close()

	// Run migrations
	migrationSQL, err := os.ReadFile("migrations/00.up.sql")
	require.NoError(t, err)
	_, err = sqlDB.Exec(string(migrationSQL))
	require.NoError(t, err)

	// Create queries handle
	db := dbgen.New(sqlDB)
	ctx := context.Background()

	t.Run("no duplicates", func(t *testing.T) {
		createTransaction(t, sqlDB, "sig1", 100, 1000, nil)
		createTransaction(t, sqlDB, "sig2", 101, 1001, nil)

		duplicates, err := db.FetchDuplicateTimestampsTransactions(ctx)
		require.NoError(t, err)
		assert.Empty(t, duplicates)

		_, err = sqlDB.Exec("DELETE FROM \"transaction\"")
		require.NoError(t, err)
	})

	t.Run("with duplicates", func(t *testing.T) {
		createTransaction(t, sqlDB, "sig3", 200, 2000, nil)
		createTransaction(t, sqlDB, "sig4", 200, 2000, nil)

		duplicates, err := db.FetchDuplicateTimestampsTransactions(ctx)
		require.NoError(t, err)
		assert.Len(t, duplicates, 2)

		sigs := []string{duplicates[0].Signature, duplicates[1].Signature}
		assert.Contains(t, sigs, "sig3")
		assert.Contains(t, sigs, "sig4")

		_, err = sqlDB.Exec("DELETE FROM \"transaction\"")
		require.NoError(t, err)
	})

	t.Run("with block_index set", func(t *testing.T) {
		blockIdx := int64(1)
		createTransaction(t, sqlDB, "sig5", 300, 3000, &blockIdx)
		createTransaction(t, sqlDB, "sig6", 300, 3000, &blockIdx)

		duplicates, err := db.FetchDuplicateTimestampsTransactions(ctx)
		require.NoError(t, err)
		assert.Empty(t, duplicates)

		_, err = sqlDB.Exec("DELETE FROM \"transaction\"")
		require.NoError(t, err)
	})

	t.Run("mixed scenarios", func(t *testing.T) {
		blockIdx := int64(1)
		createTransaction(t, sqlDB, "sig7", 400, 4000, nil)
		createTransaction(t, sqlDB, "sig8", 400, 4000, nil)
		createTransaction(t, sqlDB, "sig9", 400, 4000, &blockIdx)

		duplicates, err := db.FetchDuplicateTimestampsTransactions(ctx)
		require.NoError(t, err)

		assert.Len(t, duplicates, 2)
		var foundSig9 bool
		for _, d := range duplicates {
			if d.Signature == "sig9" {
				foundSig9 = true
				break
			}
		}
		assert.False(t, foundSig9, "Transaction with block_index should not be included")

		_, err = sqlDB.Exec("DELETE FROM \"transaction\"")
		require.NoError(t, err)
	})
}

func createTransaction(t *testing.T, db *sql.DB, signature string, timestamp int64, slot int64, blockIndex *int64) {
	query := `
		INSERT INTO "transaction" (signature, timestamp, slot, block_index, err)
		VALUES (?, ?, ?, ?, false)
	`
	_, err := db.Exec(query, signature, timestamp, slot, blockIndex)
	require.NoError(t, err)
}
