//go:build windows

package systemexec

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCommandContextWrapsBatchShimWithComSpec(t *testing.T) {
	dir := t.TempDir()
	shim := filepath.Join(dir, "agent tool.cmd")
	if err := os.WriteFile(shim, []byte("@echo off\r\necho %~1\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	comSpec := os.Getenv("ComSpec")
	if comSpec == "" {
		t.Fatal("ComSpec is not set")
	}

	cmd, err := commandContext(context.Background(), shim, "argument with spaces")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.EqualFold(cmd.Path, comSpec) {
		t.Fatalf("Path = %q, want ComSpec", cmd.Path)
	}
	if cmd.SysProcAttr == nil ||
		!strings.Contains(cmd.SysProcAttr.CmdLine, `agent tool.cmd"`) ||
		!strings.Contains(cmd.SysProcAttr.CmdLine, `"argument with spaces"`) {
		t.Fatalf("CmdLine = %q, want quoted shim and argument", cmd.SysProcAttr.CmdLine)
	}
	configureProcessGroup(cmd)
	if cmd.SysProcAttr.CmdLine == "" {
		t.Fatal("configureProcessGroup discarded the batch command line")
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("run batch shim: %v\n%s", err, output)
	}
	if got := strings.TrimSpace(string(output)); got != "argument with spaces" {
		t.Fatalf("output = %q, want batch argument preserved", got)
	}
}

func TestWindowsCapabilityProbeCapture(t *testing.T) {
	t.Setenv("AO_W187_CAPABILITY_ENV", "probe-inherited")
	dir := t.TempDir()
	shim := filepath.Join(dir, "bounded probe.cmd")
	t.Setenv("PATH", dir+";"+os.Getenv("PATH"))
	t.Setenv("PATHEXT", ".COM;.EXE;.BAT;.CMD")
	for _, code := range []string{"0", "7"} {
		if err := os.WriteFile(shim, []byte("@echo off\r\necho %AO_W187_CAPABILITY_ENV%\r\necho probe-stderr 1>&2\r\nexit /b "+code+"\r\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		out, err := capabilityOutput(ctx, "bounded probe")
		cancel()
		if code == "0" {
			if err != nil || out != "probe-inherited" {
				t.Fatalf("capture: out=%q error=%v", out, err)
			}
		} else {
			var exitErr *exec.ExitError
			if out != "" || !errors.As(err, &exitErr) || exitErr.ExitCode() != 7 || strings.TrimSpace(string(exitErr.Stderr)) != "probe-stderr" {
				t.Fatalf("stderr/exit contract: out=%q error=%v exit=%+v", out, err, exitErr)
			}
		}
	}
}

func TestWindowsCapabilityProbeOutputLimit(t *testing.T) {
	shim := filepath.Join(t.TempDir(), "finite output.cmd")
	// Fixed 10,000 built-in echo operations; no subprocess or recursive fixture.
	script := "@echo off\r\nfor /L %%i in (1,1,10000) do @echo 012345678901234567890123456789012345678901234567890123456789012345\r\n"
	if err := os.WriteFile(shim, []byte(script), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err := capabilityOutput(context.Background(), shim)
	if out != "" || err == nil || !strings.Contains(err.Error(), "stream limit") {
		t.Fatalf("output fence: out length=%d error=%v", len(out), err)
	}
}

func TestWindowsCapabilityProbeCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := capabilityOutput(ctx, filepath.Join(t.TempDir(), "never-launch.exe"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel = %v", err)
	}
}

func TestMergeWindowsPathAddsRegistryEntriesWithoutDuplicates(t *testing.T) {
	got := mergeWindowsPath(
		`C:\Windows\System32;C:\Users\tester\AppData\Roaming\npm`,
		`C:\Program Files\Git\cmd;C:\WINDOWS\SYSTEM32`,
		`C:\Users\tester\AppData\Local\Microsoft\WinGet\Links`,
	)
	want := strings.Join([]string{
		`C:\Windows\System32`,
		`C:\Users\tester\AppData\Roaming\npm`,
		`C:\Program Files\Git\cmd`,
		`C:\Users\tester\AppData\Local\Microsoft\WinGet\Links`,
	}, ";")
	if got != want {
		t.Fatalf("mergeWindowsPath() = %q, want %q", got, want)
	}
}
