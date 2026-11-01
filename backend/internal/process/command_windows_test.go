//go:build windows

package process

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

func TestCommandContextHidesConsoleWindow(t *testing.T) {
	cmd := CommandContext(context.Background(), "git", "--version")
	if cmd.SysProcAttr == nil {
		t.Fatal("SysProcAttr = nil, want hidden Windows process attributes")
	}
	if got := cmd.SysProcAttr.CreationFlags; got&windows.CREATE_NO_WINDOW == 0 {
		t.Fatalf("CreationFlags = %#x, want CREATE_NO_WINDOW", got)
	}
	if !cmd.SysProcAttr.HideWindow {
		t.Fatal("HideWindow = false, want true")
	}
}

func TestWindowsCommandBudgetRejectsMissingLimits(t *testing.T) {
	for _, budget := range []WindowsCommandBudget{{}, {MaxProcesses: 1}, {CommitBytes: 1}} {
		job, err := newWindowsCommandJob(budget)
		if err == nil || job != 0 {
			t.Fatalf("budget %+v: job=%v error=%v", budget, job, err)
		}
	}
}

func TestWindowsCommandBudgetNativeLimits(t *testing.T) {
	budget := WindowsCommandBudget{MaxProcesses: 1, CommitBytes: 64 << 20}
	job, err := newWindowsCommandJob(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer windows.CloseHandle(job)
	var limits windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION
	if err := windows.QueryInformationJobObject(job, windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&limits)), uint32(unsafe.Sizeof(limits)), nil); err != nil {
		t.Fatal(err)
	}
	wantFlags := uint32(windows.JOB_OBJECT_LIMIT_ACTIVE_PROCESS | windows.JOB_OBJECT_LIMIT_JOB_MEMORY | windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE)
	if limits.BasicLimitInformation.LimitFlags != wantFlags ||
		limits.BasicLimitInformation.ActiveProcessLimit != budget.MaxProcesses || limits.JobMemoryLimit != budget.CommitBytes {
		t.Fatalf("native job limits = %+v", limits)
	}
}

func TestWindowsCommandBudgetAdmissionBeforeResume(t *testing.T) {
	job, err := newWindowsCommandJob(WindowsCommandBudget{MaxProcesses: 1, CommitBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer windows.CloseHandle(job)
	// Both attempts are direct children and suspended: neither runs code nor
	// creates descendants. The first occupies the only admitted process slot.
	child, err := createWindowsJobCommand(job, windowsTestCommand(t), []string{"/d", "/c", "exit 0"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := windows.TerminateJobObject(job, 1); err != nil {
			t.Error(err)
		}
		state, err := windows.WaitForSingleObject(child.Process, 5000)
		if err != nil || state != windows.WAIT_OBJECT_0 {
			t.Errorf("cleanup wait: state=%d err=%v", state, err)
		}
		windows.CloseHandle(child.Thread)
		windows.CloseHandle(child.Process)
	}()
	second, err := createWindowsJobCommand(job, windowsTestCommand(t), []string{"/d", "/c", "exit 0"})
	if err == nil {
		// Preserve bounded cleanup even if admission regresses.
		windows.TerminateProcess(second.Process, 1)
		windows.WaitForSingleObject(second.Process, 5000)
		windows.CloseHandle(second.Thread)
		windows.CloseHandle(second.Process)
		t.Fatal("second process admitted despite process limit of one")
	}
}

func TestWindowsCommandBudgetRun(t *testing.T) {
	for _, code := range []string{"0", "7"} {
		t.Run(code, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			err := RunWindowsCommand(ctx, WindowsCommandBudget{MaxProcesses: 1, CommitBytes: 64 << 20},
				windowsTestCommand(t), "/d", "/c", "exit "+code)
			if code == "0" && err != nil {
				t.Fatal(err)
			}
			var exitErr *exec.ExitError
			if code == "7" && (!errors.As(err, &exitErr) || exitErr.ExitCode() != 7) {
				t.Fatalf("exit error = %v, want ExitError code 7", err)
			}
		})
	}
}

func TestWindowsCommandBudgetRejectsUnboundedOrCanceledContext(t *testing.T) {
	budget := WindowsCommandBudget{MaxProcesses: 1, CommitBytes: 64 << 20}
	if err := RunWindowsCommand(context.Background(), budget, "never-launch.exe"); err == nil {
		t.Fatal("context without deadline accepted")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	cancel()
	if err := RunWindowsCommand(ctx, budget, "never-launch.exe"); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want canceled before executable lookup", err)
	}
}

func TestWindowsCommandBudgetCommitFence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// A one-byte commit allowance cannot support command initialization. This
	// exercises rejection without allocating a large buffer or spawning a tree.
	err := RunWindowsCommand(ctx, WindowsCommandBudget{MaxProcesses: 1, CommitBytes: 1},
		windowsTestCommand(t), "/d", "/c", "exit 0")
	if err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("commit fence error = %v, want native failure before deadline", err)
	}
}

func TestWindowsCommandBudgetShimAndLookup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	budget := WindowsCommandBudget{MaxProcesses: 1, CommitBytes: 64 << 20}
	shim := filepath.Join(t.TempDir(), "budget command.cmd")
	if err := os.WriteFile(shim, []byte("@exit /b 0\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := RunWindowsCommand(ctx, budget, shim); err != nil {
		t.Fatal(err)
	}
	if err := RunWindowsCommand(ctx, budget, filepath.Join(t.TempDir(), "missing.exe")); err == nil {
		t.Fatal("missing executable accepted")
	}
}

func windowsTestCommand(t *testing.T) string {
	t.Helper()
	dir, err := windows.GetSystemDirectory()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, "cmd.exe")
}

func TestWindowsCommandFilesCaptureAndEnvironment(t *testing.T) {
	t.Setenv("AO_W187_PROCESS_ENV", "inherited-value")
	dir := t.TempDir()
	shim := filepath.Join(dir, "stream contract.cmd")
	if err := os.WriteFile(shim, []byte("@echo off\r\necho %AO_W187_PROCESS_ENV%\r\necho diagnostic 1>&2\r\nexit /b 7\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err := os.Create(filepath.Join(dir, "stdout"))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	diagnostic, err := os.Create(filepath.Join(dir, "stderr"))
	if err != nil {
		t.Fatal(err)
	}
	defer diagnostic.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err = RunWindowsCommandFiles(ctx, WindowsCommandBudget{MaxProcesses: 1, CommitBytes: 64 << 20}, nil, out, diagnostic, shim)
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 7 {
		t.Fatalf("exit = %v, want ExitError 7", err)
	}
	for _, check := range []struct{ file, want string }{{out.Name(), "inherited-value\r\n"}, {diagnostic.Name(), "diagnostic \r\n"}} {
		got, err := os.ReadFile(check.file)
		if err != nil || string(got) != check.want {
			t.Fatalf("%s = %q, error=%v, want %q", check.file, got, err, check.want)
		}
	}
}

func TestWindowsCommandFilesCancellationSubtreeAndSurvivor(t *testing.T) {
	// The unrelated process never executes: it is suspended in a different
	// test-owned job, and must remain alive after the target job is canceled.
	survivorJob, err := newWindowsCommandJob(WindowsCommandBudget{MaxProcesses: 1, CommitBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer windows.CloseHandle(survivorJob)
	survivor, err := createWindowsJobCommand(survivorJob, windowsTestCommand(t), []string{"/d", "/c", "exit 0"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		windows.TerminateJobObject(survivorJob, 1)
		windows.WaitForSingleObject(survivor.Process, 5000)
		windows.CloseHandle(survivor.Thread)
		windows.CloseHandle(survivor.Process)
	}()
	dir, err := windows.GetSystemDirectory()
	if err != nil {
		t.Fatal(err)
	}
	ps := filepath.Join(dir, "WindowsPowerShell", "v1.0", "powershell.exe")
	pidFile := filepath.Join(t.TempDir(), "owned-pids")
	// Fixed depth one: one PowerShell root starts exactly one sleeping child.
	// Both scripts have finite lifetimes even if the cleanup implementation fails.
	script := "$child = Start-Process -FilePath '" + strings.ReplaceAll(ps, "'", "''") +
		"' -ArgumentList '-NoProfile','-NonInteractive','-Command','Start-Sleep -Seconds 15' -PassThru; " +
		"[IO.File]::WriteAllText('" + strings.ReplaceAll(pidFile+".tmp", "'", "''") + "', [string]$PID + ',' + [string]$child.Id); " +
		"Move-Item -LiteralPath '" + strings.ReplaceAll(pidFile+".tmp", "'", "''") + "' -Destination '" + strings.ReplaceAll(pidFile, "'", "''") + "'; Start-Sleep -Seconds 15"
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- RunWindowsCommand(ctx, WindowsCommandBudget{MaxProcesses: 2, CommitBytes: 512 << 20},
			ps, "-NoProfile", "-NonInteractive", "-Command", script)
	}()
	// Always join the runner before the test returns, including failure paths.
	joined := false
	defer func() {
		cancel()
		if !joined {
			select {
			case <-done:
			case <-time.After(6 * time.Second):
				t.Error("runner did not settle after cancellation")
			}
		}
	}()
	var pids []string
	readyBy := time.Now().Add(5 * time.Second)
	for time.Now().Before(readyBy) {
		data, _ := os.ReadFile(pidFile)
		pids = strings.Split(string(data), ",")
		if len(pids) == 2 && pids[0] != "" && pids[1] != "" {
			break
		}
		select {
		case err := <-done:
			joined = true
			t.Fatalf("runner exited before child readiness: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
	}
	if len(pids) != 2 {
		t.Fatal("fixed child did not report readiness within five seconds")
	}
	var handles []windows.Handle
	for _, value := range pids {
		pid, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			t.Fatal(err)
		}
		handle, err := windows.OpenProcess(windows.SYNCHRONIZE, false, uint32(pid))
		if err != nil {
			t.Fatal(err)
		}
		defer windows.CloseHandle(handle)
		handles = append(handles, handle)
	}
	cancel()
	select {
	case err := <-done:
		joined = true
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel result = %v", err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("cancel did not return within cleanup bound")
	}
	for _, handle := range handles {
		state, err := windows.WaitForSingleObject(handle, 5000)
		if err != nil || state != windows.WAIT_OBJECT_0 {
			t.Fatalf("owned subtree did not exit: state=%d err=%v", state, err)
		}
	}
	state, err := windows.WaitForSingleObject(survivor.Process, 0)
	if err != nil || state != uint32(windows.WAIT_TIMEOUT) {
		t.Fatalf("unrelated suspended survivor changed: state=%d err=%v", state, err)
	}
}
