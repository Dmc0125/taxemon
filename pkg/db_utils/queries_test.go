package dbutils

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/test-go/testify/assert"
	"github.com/test-go/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func getProjectDir() string {
	_, fp, _, ok := runtime.Caller(0)
	if !ok {
		fmt.Println("unable to get current filename")
		os.Exit(1)
	}
	projectDir := path.Join(fp, "../../..")
	return projectDir
}

func setupTestDB(t *testing.T) (*sqlx.DB, func()) {
	ctx := context.Background()
	projectDir := getProjectDir()
	postgres, err := postgres.Run(
		ctx,
		"docker.io/postgres:16-alpine",
		postgres.WithInitScripts(
			filepath.Join(projectDir, "/db/migrations/00.up.sql"),
		),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(10*time.Second)),
	)
	require.NoError(t, err)

	dbUrl := postgres.MustConnectionString(ctx, "sslmode=disable")
	db, err := sqlx.Connect("postgres", dbUrl)
	require.NoError(t, err)

	cleanup := func() {
		db.Close()
		if err := testcontainers.TerminateContainer(postgres); err != nil {
			log.Printf("unable to gracefully terminate postgres container: %s", err)
		}
	}

	return db, cleanup
}

func TestSelectOrderedTransactions(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	txs := []*InsertTransactionParams{
		{
			Signature: "sig1",
			Timestamp: time.Now(),
			Slot:      100,
			Err:       false,
			Accounts:  pq.StringArray{"acc1", "acc2"},
			Logs:      pq.StringArray{"log1"},
		},
		{
			Signature: "sig2",
			Timestamp: time.Now().Add(time.Second),
			Slot:      100,
			Err:       false,
			Accounts:  pq.StringArray{"acc3", "acc4"},
			Logs:      pq.StringArray{"log2"},
		},
		{
			Signature: "sig3",
			Timestamp: time.Now().Add(2 * time.Second),
			Slot:      101,
			Err:       false,
			Accounts:  pq.StringArray{"acc5", "acc6"},
			Logs:      pq.StringArray{"log3"},
		},
	}

	_, err := InsertTransactions(db, txs)
	require.NoError(t, err)

	// Update block indexes for some transactions
	err = UpdateTransactionsBlockIndexes(db,
		[]string{"sig1", "sig2"},
		[]int32{1, 2},
	)
	require.NoError(t, err)

	tests := []struct {
		name           string
		fromSlot       int64
		fromBlockIndex int32
		limit          int
		expectedCount  int
	}{
		{
			name:           "Basic query",
			fromSlot:       99,
			fromBlockIndex: 0,
			limit:          10,
			expectedCount:  3,
		},
		{
			name:           "Filter by slot",
			fromSlot:       100,
			fromBlockIndex: 0,
			limit:          10,
			expectedCount:  3,
		},
		{
			name:           "Filter by block index",
			fromSlot:       100,
			fromBlockIndex: 1,
			limit:          10,
			expectedCount:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := SelectOrderedTransactions(db, tt.fromSlot, tt.fromBlockIndex, tt.limit)
			require.NoError(t, err)
			assert.Len(t, results, tt.expectedCount)

			// Verify ordering
			if len(results) > 1 {
				for i := 1; i < len(results); i++ {
					prev := results[i-1]
					curr := results[i]

					// Check if ordering is correct (by slot and block_index)
					if prev.Slot == curr.Slot {
						// If same slot, block_index should be ascending
						if prev.BlockIndex.Valid && curr.BlockIndex.Valid {
							assert.True(t, prev.BlockIndex.Int32 < curr.BlockIndex.Int32)
						}
					} else {
						assert.True(t, prev.Slot < curr.Slot)
					}
				}
			}
		})
	}
}

func TestSelectDuplicateTimestampsTransactions(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	now := time.Now()
	txs := []*InsertTransactionParams{
		{
			Signature: "sig1",
			Timestamp: now,
			Slot:      100,
			Err:       false,
			Accounts:  pq.StringArray{"acc1"},
			Logs:      pq.StringArray{"log1"},
		},
		{
			Signature: "sig2",
			Timestamp: now,
			Slot:      100,
			Err:       false,
			Accounts:  pq.StringArray{"acc2"},
			Logs:      pq.StringArray{"log2"},
		},
		{
			Signature: "sig3",
			Timestamp: now,
			Slot:      101,
			Err:       false,
			Accounts:  pq.StringArray{"acc3"},
			Logs:      pq.StringArray{"log3"},
		},
		{
			Signature: "sig4",
			Timestamp: now.Add(time.Second),
			Slot:      100,
			Err:       false,
			Accounts:  pq.StringArray{"acc4"},
			Logs:      pq.StringArray{"log4"},
		},
	}

	_, err := InsertTransactions(db, txs)
	require.NoError(t, err)

	// Test without block indexes (should find duplicates)
	t.Run("Find duplicates without block indexes", func(t *testing.T) {
		results, err := SelectDuplicateTimestampsTransactions(db)
		require.NoError(t, err)
		assert.Len(t, results, 3) // Should find sig1, sig2 and sig4 as duplicates

		// Verify the results contain the expected duplicates
		signatures := make(map[string]bool)
		for _, r := range results {
			signatures[r.Signature] = true
			assert.Equal(t, int64(100), r.Slot)
		}
		assert.True(t, signatures["sig1"])
		assert.True(t, signatures["sig2"])
		assert.True(t, signatures["sig4"])
	})

	t.Run("No duplicates after adding block indexes", func(t *testing.T) {
		err = UpdateTransactionsBlockIndexes(db,
			[]string{"sig1", "sig2", "sig4"},
			[]int32{1, 2, 3},
		)
		require.NoError(t, err)

		results, err := SelectDuplicateTimestampsTransactions(db)
		require.NoError(t, err)
		assert.Len(t, results, 0)
	})
}
