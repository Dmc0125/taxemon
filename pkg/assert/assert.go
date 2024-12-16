package assert

import (
	"fmt"
	"os"
	"runtime"
	"strings"
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
		fmt.Printf("NoErr assertion failed: %s\n\nStacktrace: %s", msg, getStackTrace())
		os.Exit(1)
	}
}

func True(cond bool, msg string, args ...any) {
	if !cond {
		fmt.Printf("True assertion failed: %s\n\nStacktrace: %s", msg, getStackTrace())
		os.Exit(1)
	}
}

func NoEmptyStr(s string, msg string) {
	if s == "" {
		fmt.Printf("NoEmptyStr assertion failed: %s\n\nStacktrace: %s", msg, getStackTrace())
		os.Exit(1)
	}
}
