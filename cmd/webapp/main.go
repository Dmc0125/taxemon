package main

import (
	"html/template"
	"log/slog"
	"os"
	"path"
	"runtime"
	walletshandler "taxemon/handlers/wallets_handler"
	"taxemon/pkg/assert"
	"taxemon/pkg/logger"
	"taxemon/pkg/rpc"
	walletsync "taxemon/pkg/wallet_sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/joho/godotenv"
	"github.com/labstack/echo/v4"
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
	dbUrl := os.Getenv("DB_URL")
	assert.NoEmptyStr(rpcUrl, "Missing RPC_URL")
	assert.NoEmptyStr(dbUrl, "Missing DB_PATH")

	db, err := sqlx.Connect("postgres", dbUrl)
	assert.NoErr(err, "unable to open db", "dbUrl", dbUrl)

	rpcClient := rpc.NewClientWithTimer(rpcUrl, 400*time.Millisecond)

	projectDir := getProjectDir()
	t := template.Must(template.ParseGlob(path.Join(projectDir, "/templates/*.html")))

	e := echo.New()

	dir, err := os.Getwd()
	assert.NoErr(err, "unable to get working directory")
	e.Static("/assets/", path.Join(dir, "assets"))

	walletshandler.Register(e, db, t)

	go walletsync.Start(rpcClient, db)
	e.Logger.Fatal(e.Start(":42069"))
}
