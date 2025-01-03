live/server:
	go run github.com/cosmtrek/air@v1.51.0 \
	--build.cmd "go build -o tmp/bin/main ./cmd/webapp/main.go" \
	--build.bin "tmp/bin/main" \
	--build.delay "100" \
	--build.exclude_dir "node_modules" \
	--build.include_ext "go,html" \
	--build.stop_on_error "false" \
	--misc.clean_on_exit true

live/tailwind:
	bunx tailwindcss -i ./input.css -o ./assets/styles.css --minify --watch

live:
	make -j2 live/tailwind live/server
