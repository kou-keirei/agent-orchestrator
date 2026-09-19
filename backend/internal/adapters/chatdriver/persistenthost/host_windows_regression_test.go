//go:build windows

package persistenthost

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestConnectOrStartLaunchesWindowsCodexShimThroughProcessBoundary(t *testing.T) {
	shim := filepath.Join(t.TempDir(), "Codex install", "codex.cmd")
	if err := os.MkdirAll(filepath.Dir(shim), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(shim, []byte("@echo off\r\nif /i not \"%~1\"==\"app-server\" exit /b 64\r\nif not \"%~2\"==\"\" exit /b 65\r\n\"%AO_CHAT_HOST_SHIM_HELPER%\" -test.run=TestProviderHelper\r\nexit /b %ERRORLEVEL%\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	dataDir := t.TempDir()
	cfg := Config{
		SessionID: "windows-shim",
		DataDir:   dataDir,
		Workdir:   t.TempDir(),
		Env: append(os.Environ(),
			"AO_CHAT_HOST_PROVIDER_HELPER=1",
			"AO_CHAT_HOST_SHIM_HELPER="+os.Args[0],
		),
		Argv: []string{shim, "app-server"},
	}
	t.Cleanup(func() {
		_ = Shutdown(context.Background(), dataDir, cfg.SessionID)
	})

	transport, err := ConnectOrStart(context.Background(), cfg)
	if err != nil {
		t.Fatalf("ConnectOrStart: %v", err)
	}
	if transport.Reconnected {
		t.Fatal("Windows shim launch unexpectedly reconnected to an existing host")
	}
	if got := requestProviderPID(t, transport, 1, "pid"); got <= 0 {
		t.Fatalf("provider pid = %d, want a live provider helper", got)
	}
	if err := transport.Stdin.Close(); err != nil {
		t.Fatalf("detach transport: %v", err)
	}

	if err := Shutdown(context.Background(), dataDir, cfg.SessionID); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	path, _ := descriptorPath(dataDir, cfg.SessionID)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("Windows shim host did not remove its descriptor after shutdown")
}
