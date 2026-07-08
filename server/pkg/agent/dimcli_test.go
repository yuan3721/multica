package agent

import (
	"context"
	"log/slog"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// TestDimcliRealACPSmoke drives the REAL official `dim acp` binary.
// Skipped automatically when dim is not on PATH or the session cannot be
// established (e.g. no provider configured). The test verifies basic
// session creation and a simple prompt round-trip without asserting on
// the output content.
func TestDimcliRealACPSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real-binary smoke test in -short mode")
	}
	path, err := exec.LookPath("dim")
	if err != nil {
		t.Skipf("dim not on PATH; skipping real-binary smoke test")
	}

	backend, err := New("dimcli", Config{ExecutablePath: path, Logger: slog.Default()})
	if err != nil {
		t.Fatalf("new dimcli backend: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	session, err := backend.Execute(ctx, "Respond with exactly one word: pong", ExecOptions{
		Cwd:     t.TempDir(),
		Timeout: 20 * time.Second,
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	// Drain messages (we don't care about content here).
	for range session.Messages {
	}

	var result Result
	select {
	case result = <-session.Result:
	case <-ctx.Done():
		t.Fatal("timeout waiting for real dimcli result")
	}

	if result.Status != "completed" && result.Status != "failed" {
		t.Fatalf("expected completed or failed, got status=%q error=%q", result.Status, result.Error)
	}
	if result.Status == "failed" && strings.Contains(result.Error, "initialize failed") {
		// A logged-out dim (no provider configured) fails at initialize — skip
		// instead of failing the test, so CI works against a fresh dim install.
		t.Skipf("dim not configured (initialize failed): %v", result.Error)
	}
	// The output should contain "pong" (or a failure about missing config).
	if result.Status == "completed" && !strings.Contains(result.Output, "pong") {
		t.Fatalf("expected real dim output to contain 'pong', got %q", result.Output)
	}
	if result.SessionID == "" && result.Status == "completed" {
		// dim always returns a session ID on success; empty means something
		// went wrong in session/new and we continued without one.
		t.Error("expected a non-empty session id from real dimcli")
	}
	t.Logf("real dimcli smoke OK: session=%s output=%q", result.SessionID, result.Output)
}
