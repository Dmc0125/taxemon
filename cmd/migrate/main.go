package main

import (
	"flag"
	"fmt"
	"os"
	"path"
	"runtime"
	"slices"
	"strings"
	"taxemon/pkg/assert"
	"taxemon/pkg/logger"

	"github.com/jmoiron/sqlx"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
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

func main() {
	err := godotenv.Load()
	assert.NoErr(err, "unable to load .env")

	logger.NewPrettyLogger("", -4)

	dbUrl := os.Getenv("DB_URL")
	assert.NoEmptyStr(dbUrl, "Missing DB_PATH")
	db, err := sqlx.Connect("postgres", dbUrl)
	assert.NoErr(err, "unable to open sqlite connection")

	var dir string
	flag.StringVar(&dir, "dir", "up", "up / down")
	flag.Parse()

	projectDir := getProjectDir()
	migrationsDir := path.Join(projectDir, "db/migrations")

	files, err := os.ReadDir(migrationsDir)
	assert.NoErr(err, "unable to read migrations dir")

	if dir == "down" {
		slices.Reverse(files)
	}

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

		q := string(bytes)
		_, err = db.Exec(q)
		assert.NoErr(err, "unable to exec query", "query", q)

		fmt.Printf("Successfully executed migration %s %s\n", dir, n)
	}
}
