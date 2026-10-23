//go:build windows

package process

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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
			if code == "7" && (err == nil || !strings.Contains(err.Error(), "code 7")) {
				t.Fatalf("exit error = %v, want code 7", err)
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
