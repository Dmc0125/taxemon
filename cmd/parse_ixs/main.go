package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"taxemon/pkg/assert"
	"taxemon/pkg/dbgen"
	"taxemon/pkg/fetcher"
	ixparser "taxemon/pkg/ix_parser"
	"taxemon/pkg/logger"

	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	assert.NoErr(err, "unable to read .env")

	err = logger.NewPrettyLogger("", int(slog.LevelDebug))
	assert.NoErr(err, "unable to use pretty logger")

	rpcUrl := os.Getenv("RPC_URL")
	dbPath := os.Getenv("DB_PATH")
	assert.NoEmptyStr(rpcUrl, "Missing RPC_URL")
	assert.NoEmptyStr(dbPath, "Missing DB_PATH")

	db, err := sql.Open("sqlite", fmt.Sprintf("file://%s", dbPath))
	assert.NoErr(err, "unable to open db", "dbPath", dbPath)
	q := dbgen.New(db)

	ctx, _ := context.WithCancel(context.Background())

	txs, err := q.FetchUnknownTransactions(ctx)
	assert.NoErr(err, "unable to fetch unknown transactions")

	for _, tx := range txs {
		dtx, err := fetcher.DeserializeSavedTransaction(tx)
		assert.NoErr(err, "unable to deserialize transaction")

		ixparser.ParseTx(dtx, walletAddress string)
	}
}
