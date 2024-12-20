package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"path"
	"runtime"
	"strings"
	"taxemon/pkg/assert"
	"taxemon/pkg/logger"

	"github.com/joho/godotenv"
	_ "modernc.org/sqlite"
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

func execFile(queries []string, db *sql.DB) {
	tx, err := db.Begin()
	assert.NoErr(err, "unable to begin tx")
	defer tx.Rollback()

	for _, q := range queries {
		_, err := tx.Exec(fmt.Sprintf("%s;", q))
		assert.NoErr(err, "unable to exec query", "query", q)
	}

	err = tx.Commit()
	assert.NoErr(err, "unable to commit file")
}

func main() {
	err := godotenv.Load()
	assert.NoErr(err, "unable to load .env")

	logger.NewPrettyLogger("", -4)

	dbPath := os.Getenv("DB_PATH")
	assert.NoEmptyStr(dbPath, "Missing DB_PATH")
	db, err := sql.Open("sqlite", dbPath)
	assert.NoErr(err, "unable to open sqlite connection")

	var dir string
	flag.StringVar(&dir, "dir", "up", "up / down")
	flag.Parse()

	projectDir := getProjectDir()
	migrationsDir := path.Join(projectDir, "db/migrations")

	files, err := os.ReadDir(migrationsDir)
	assert.NoErr(err, "unable to read migrations dir")

	for _, file := range files {
		var suffix string
		switch dir {
		case "up":
			suffix = ".up.sql"
		case "down":
			suffix = ".down.sql"
		}

		n := file.Name()
		if !strings.HasSuffix(n, suffix) {
			continue
		}

		bytes, err := os.ReadFile(path.Join(migrationsDir, n))
		assert.NoErr(err, "unable to read file")
		queries := strings.Split(string(bytes), ";")

		execFile(queries, db)
		fmt.Printf("Successfully executed migration %s %s\n", dir, n)
	}
}
