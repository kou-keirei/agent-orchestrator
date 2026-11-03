//go:build windows

package systemexec

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"golang.org/x/sys/windows"
)

func TestAdapterRunWindowsRealGitCLI(t *testing.T) {
	// Actual installed native CLI through the production port implementation;
	// no renamed/copied executable or fake SCM fixture, and no remote operation.
	git, err := exec.LookPath("git.exe")
	if err != nil {
		t.Fatal("real Git CLI unavailable: ", err)
	}
	var runner ports.CommandRunner = New(t.TempDir())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var stdout, stderr bytes.Buffer
	if err := runner.Run(ctx, []string{git, "--version"}, &stdout, &stderr); err != nil {
		t.Fatalf("native Git: %v; stderr=%q", err, stderr.String())
	}
	if !strings.HasPrefix(stdout.String(), "git version ") || stderr.Len() != 0 {
		t.Fatalf("native Git stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
	t.Logf("native CLI=%s output=%s", git, strings.TrimSpace(stdout.String()))
}

func TestAdapterRunWindowsStdioEnvironmentAndExit(t *testing.T) {
	t.Setenv("AO_W194_VALUE", "inherited")
	shim := filepath.Join(t.TempDir(), "stdio.cmd")
	if err := os.WriteFile(shim, []byte("@echo off\r\nset /p value= || echo stdin-eof\r\necho %AO_W194_VALUE%\r\necho diagnostic 1>&2\r\nexit /b 7\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var shared bytes.Buffer
	err := (Adapter{}).Run(ctx, []string{shim}, &shared, &shared)
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 7 ||
		!strings.Contains(shared.String(), "inherited") || !strings.Contains(shared.String(), "diagnostic") || !strings.Contains(shared.String(), "stdin-eof") {
		t.Fatalf("stdio/exit: error=%v output=%q", err, shared.String())
	}
	if err := (Adapter{}).Run(ctx, nil, io.Discard, io.Discard); err == nil {
		t.Fatal("empty argv accepted")
	}
}

func TestAdapterRunWindowsSubtreeLifecycle(t *testing.T) {
	dir, err := windows.GetSystemDirectory()
	if err != nil {
		t.Fatal(err)
	}
	// A known unrelated suspended child never executes and must survive each
	// target job's teardown. Cleanup uses only the handles created here.
	var survivor windows.ProcessInformation
	app, _ := windows.UTF16PtrFromString(filepath.Join(dir, "cmd.exe"))
	var startup windows.StartupInfo
	startup.Cb = uint32(unsafe.Sizeof(startup))
	if err := windows.CreateProcess(app, nil, nil, nil, false, windows.CREATE_SUSPENDED|windows.CREATE_NO_WINDOW, nil, nil, &startup, &survivor); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := windows.TerminateProcess(survivor.Process, 1); err != nil {
			t.Error(err)
		}
		if state, err := windows.WaitForSingleObject(survivor.Process, 5000); err != nil || state != windows.WAIT_OBJECT_0 {
			t.Errorf("survivor cleanup: state=%d error=%v", state, err)
		}
		windows.CloseHandle(survivor.Thread)
		windows.CloseHandle(survivor.Process)
	}()
	ps := filepath.Join(dir, "WindowsPowerShell", "v1.0", "powershell.exe")
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
	for _, mode := range []string{"cancel", "timeout", "root-exit"} {
		t.Run(mode, func(t *testing.T) {
			scratch := t.TempDir()
			pidFile, gate := filepath.Join(scratch, "child.pid"), filepath.Join(scratch, "exit-root")
			// Exactly one child, fixed depth one, both with finite lifetimes.
			script := "$child=Start-Process -FilePath " + quote(ps) +
				" -ArgumentList '-NoProfile','-NonInteractive','-Command','Start-Sleep -Seconds 15' -PassThru; " +
				"[IO.File]::WriteAllText(" + quote(pidFile+".tmp") + ",[string]$child.Id); Move-Item -LiteralPath " + quote(pidFile+".tmp") + " -Destination " + quote(pidFile) +
				"; Write-Output 'streamed-ready'; for($i=0;$i -lt 200;$i++){if(Test-Path -LiteralPath " + quote(gate) + "){break};Start-Sleep -Milliseconds 50}"
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()
			output := &readyWriter{ready: make(chan struct{}, 1)}
			done := make(chan error, 1)
			go func() {
				done <- (Adapter{}).Run(ctx, []string{ps, "-NoProfile", "-NonInteractive", "-Command", script}, output, io.Discard)
			}()
			joined := false
			defer func() {
				cancel()
				if !joined {
					select {
					case <-done:
					case <-time.After(6 * time.Second):
						t.Error("runner did not settle")
					}
				}
			}()
			select {
			case <-output.ready: // Must stream before root exit/cancellation.
			case err := <-done:
				joined = true
				t.Fatalf("runner exited before readiness: %v", err)
			case <-time.After(4 * time.Second):
				t.Fatal("no streamed readiness within four seconds")
			}
			data, err := os.ReadFile(pidFile)
			if err != nil {
				t.Fatal(err)
			}
			pid, err := strconv.ParseUint(string(data), 10, 32)
			if err != nil {
				t.Fatal(err)
			}
			child, err := windows.OpenProcess(windows.SYNCHRONIZE, false, uint32(pid))
			if err != nil {
				t.Fatal(err)
			}
			defer windows.CloseHandle(child)
			switch mode {
			case "cancel":
				cancel()
			case "root-exit":
				if err := os.WriteFile(gate, nil, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case err := <-done:
				joined = true
				if mode == "cancel" && !errors.Is(err, context.Canceled) ||
					mode == "timeout" && !errors.Is(err, context.DeadlineExceeded) || mode == "root-exit" && err != nil {
					t.Fatalf("%s result=%v", mode, err)
				}
			case <-time.After(12 * time.Second):
				t.Fatal("runner exceeded deadline and cleanup allowance")
			}
			if state, err := windows.WaitForSingleObject(child, 5000); err != nil || state != windows.WAIT_OBJECT_0 {
				t.Fatalf("orphan escaped job cleanup: state=%d error=%v", state, err)
			}
			if state, err := windows.WaitForSingleObject(survivor.Process, 0); err != nil || state != uint32(windows.WAIT_TIMEOUT) {
				t.Fatalf("unrelated survivor changed: state=%d error=%v", state, err)
			}
		})
	}
}

type readyWriter struct{ ready chan struct{} }

func (w *readyWriter) Write(p []byte) (int, error) {
	select {
	case w.ready <- struct{}{}:
	default:
	}
	return len(p), nil
}

func TestAdapterRunWindowsOutputFailures(t *testing.T) {
	shim := filepath.Join(t.TempDir(), "finite output.cmd")
	// Fixed built-in operations, without recursive helpers or child fan-out.
	if err := os.WriteFile(shim, []byte("@echo off\r\nfor /L %%i in (1,1,300) do @echo "+strings.Repeat("x", 1024)+"\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := (Adapter{}).Run(ctx, []string{shim}, io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), "stream limit") {
		t.Fatalf("output fence error=%v", err)
	}
	if err := os.WriteFile(shim, []byte("@echo one write\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	writerCtx, writerCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer writerCancel()
	if err := (Adapter{}).Run(writerCtx, []string{shim}, failingWindowsWriter{}, io.Discard); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("writer error=%v, want closed pipe", err)
	}
}

type failingWindowsWriter struct{}

func (failingWindowsWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
