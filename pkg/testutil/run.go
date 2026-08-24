// Package testutil holds shared helpers for package tests.
//
// Run is the required TestMain entrypoint: it reprints go test results with
// green ✅ / red ❌ marks so pass/fail is readable at a glance. See SKILLS.md
// §1.5 and ARCHITECTURE.md §7.
package testutil

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"testing"
)

const (
	colorReset = "\033[0m"
	colorGreen = "\033[32m"
	colorRed   = "\033[31m"
)

// Run executes m and reprints each test result with a colored mark, then a
// pass/fail summary. Call from every test package:
//
//	func TestMain(m *testing.M) {
//		testutil.Run(m)
//	}
func Run(m *testing.M) {
	ensureVerbose()

	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		os.Exit(m.Run())
	}
	os.Stdout = w

	codeCh := make(chan int, 1)
	go func() {
		codeCh <- m.Run()
		_ = w.Close()
	}()

	var passed, failed int
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(trimmed, "--- PASS:"):
			passed++
			fmt.Fprintf(oldStdout, "✅ %s%s%s\n", colorGreen, testName(trimmed, "--- PASS:"), colorReset)
		case strings.HasPrefix(trimmed, "--- FAIL:"):
			failed++
			fmt.Fprintf(oldStdout, "❌ %s%s%s\n", colorRed, testName(trimmed, "--- FAIL:"), colorReset)
		case strings.HasPrefix(trimmed, "--- SKIP:"):
			fmt.Fprintf(oldStdout, "⏭  %s\n", testName(trimmed, "--- SKIP:"))
		case strings.HasPrefix(line, "=== "):
			// Drop RUN/PAUSE/CONT banners; the mark line is enough.
		case strings.HasPrefix(line, "ok ") || strings.HasPrefix(line, "FAIL ") || line == "PASS" || line == "FAIL":
			// Drop the default package summary; we print our own totals.
		default:
			if line != "" {
				fmt.Fprintln(oldStdout, line)
			}
		}
	}

	total := passed + failed
	fmt.Fprintf(oldStdout, "\n============================\n")
	fmt.Fprintf(oldStdout, "Results: %d/%d passing, %d/%d failing\n", passed, total, failed, total)
	fmt.Fprintf(oldStdout, "============================\n")

	os.Exit(<-codeCh)
}

func testName(trimmed, prefix string) string {
	return strings.TrimSpace(strings.TrimPrefix(trimmed, prefix))
}

func ensureVerbose() {
	for _, arg := range os.Args[1:] {
		if arg == "-test.v" || arg == "-test.v=true" || strings.HasPrefix(arg, "-test.v=") {
			return
		}
	}
	os.Args = append(os.Args, "-test.v")
}
