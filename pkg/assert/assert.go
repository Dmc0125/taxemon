package assert

import (
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"strings"
	"taxemon/pkg/logger"
)

func getStackTrace() string {
	pc := make([]uintptr, 10)
	n := runtime.Callers(2, pc)
	if n == 0 {
		return "Unable to retrieve stack trace"
	}

	pc = pc[:n]
	frames := runtime.CallersFrames(pc)

	var stackTrace strings.Builder
	for {
		frame, more := frames.Next()
		fmt.Fprintf(&stackTrace, "%s\n\tat %s:%d\n", frame.Function, frame.File, frame.Line)
		if !more {
			break
		}
	}

	return stackTrace.String()
}

func NoErr(err error, msg string, args ...any) {
	if err != nil {
		if len(args) > 0 {
			logger.SetOut(os.Stdout)
			slog.Error("Assertion failed", args...)
			fmt.Println()
		}

		fmt.Printf(
			"NoErr assertion failed: %s\nErr: %s\n\nStacktrace: %s",
			msg,
			err,
			getStackTrace(),
		)
		os.Exit(1)
	}
}

func True(cond bool, msg string, args ...any) {
	if !cond {
		if len(args) > 0 {
			logger.SetOut(os.Stdout)
			slog.Error("Assertion failed", args...)
			fmt.Println()
		}

		fmt.Printf("True assertion failed: %s\n\nStacktrace: %s", msg, getStackTrace())
		os.Exit(1)
	}
}

func NoEmptyStr(s string, msg string, args ...any) {
	if s == "" {
		if len(args) > 0 {
			logger.SetOut(os.Stdout)
			slog.Error("Assertion failed", args...)
			fmt.Println()
		}

		fmt.Printf("NoEmptyStr assertion failed: %s\n\nStacktrace: %s", msg, getStackTrace())
		os.Exit(1)
	}
}
