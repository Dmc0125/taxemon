package main

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"taxemon/pkg/assert"
	"taxemon/pkg/dbgen"
	"taxemon/pkg/fetcher"
	"taxemon/pkg/logger"
	"taxemon/pkg/rpc"
	"time"

	"github.com/joho/godotenv"
	_ "modernc.org/sqlite"
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

	rpcClient := rpc.NewClientWithTimer(rpcUrl, 400*time.Millisecond)

	fetcher.SyncWallet(rpcClient, db, q, "")
}
