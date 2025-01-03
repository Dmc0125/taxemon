package main

import (
	"database/sql"
	"fmt"
	"html/template"
	"log"
	"log/slog"
	"os"
	"path"
	"runtime"
	walletshandler "taxemon/handlers/wallets_handler"
	"taxemon/pkg/assert"
	"taxemon/pkg/dbgen"
	"taxemon/pkg/fetcher"
	"taxemon/pkg/logger"
	"taxemon/pkg/rpc"
	"time"

	"github.com/joho/godotenv"
	"github.com/labstack/echo/v4"
	"golang.org/x/sync/errgroup"
	_ "modernc.org/sqlite"
)

func getProjectDir() string {
	_, fp, _, ok := runtime.Caller(0)
	assert.True(ok, "unable to get current filename")
	projectDir := path.Join(fp, "../../..")
	return projectDir
}

func main() {
	err := godotenv.Load()
	assert.NoErr(err, "unable to load .env")

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

	projectDir := getProjectDir()
	t := template.Must(template.ParseGlob(path.Join(projectDir, "/templates/*.html")))

	e := echo.New()

	dir, err := os.Getwd()
	assert.NoErr(err, "unable to get wroking directory")
	e.Static("/assets/", path.Join(dir, "assets"))

	walletshandler.Register(e, q, t)

	var eg errgroup.Group
	eg.TryGo(func() error {
		return fetcher.Start(rpcClient, db, q)
	})
	eg.TryGo(func() error {
		return e.Start(":42069")
	})
	if err = eg.Wait(); err != nil {
		log.Fatal(err)
	}
}
