//go:build windows

package systemexec

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/registry"

	aoprocess "github.com/aoagents/agent-orchestrator/backend/internal/process"
)

func init() {
	capabilityCommandOutput = windowsCapabilityCommandOutput
}

// Only the fixed, non-interactive Settings capability probes use this budget.
// Installers, Adapter.Run and provider-internal tools are not covered here.
func windowsCapabilityCommandOutput(parent context.Context, name string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	stdout, stdoutWriter, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	defer stdout.Close()
	defer stdoutWriter.Close()
	stderr, stderrWriter, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	defer stderr.Close()
	defer stderrWriter.Close()
	type capture struct {
		data []byte
		err  error
	}
	read := func(file *os.File) <-chan capture {
		done := make(chan capture, 1)
		go func() {
			const outputLimit = 256 << 10
			var buffer bytes.Buffer
			_, err := io.CopyN(&buffer, file, outputLimit+1)
			if errors.Is(err, io.EOF) {
				err = nil
			}
			if buffer.Len() > outputLimit {
				err = errors.New("Windows capability probe output exceeds 256 KiB stream limit")
				cancel()
			}
			done <- capture{data: buffer.Bytes(), err: err}
		}()
		return done
	}
	outDone, errDone := read(stdout), read(stderr)
	// Allows a shim, its interpreter and small fixed probe fan-out. This is
	// committed memory, not Node's much larger reserved virtual address space.
	err = aoprocess.RunWindowsCommandFiles(ctx, aoprocess.WindowsCommandBudget{
		MaxProcesses: 4, CommitBytes: 512 << 20,
	}, nil, stdoutWriter, stderrWriter, name, args...)
	stdoutWriter.Close()
	stderrWriter.Close()
	// Normal job teardown closes descendant pipe writers. Bound capture even
	// if an external process acquired a writer outside this supported subtree.
	timer := time.AfterFunc(5*time.Second, func() {
		stdout.Close()
		stderr.Close()
	})
	out, diagnostic := <-outDone, <-errDone
	timer.Stop()
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		exitErr.Stderr = diagnostic.data
	}
	if captureErr := errors.Join(out.err, diagnostic.err); captureErr != nil {
		err = errors.Join(err, fmt.Errorf("capture Windows capability probe: %w", captureErr))
	}
	return out.data, err
}

func commandContext(ctx context.Context, name string, args ...string) (*exec.Cmd, error) {
	resolved, err := exec.LookPath(name)
	if err != nil {
		return nil, err
	}
	return aoprocess.CommandContext(ctx, resolved, args...), nil
}

func refreshExecutablePath() {
	paths := []string{os.Getenv("Path")}
	paths = append(paths,
		registryPath(registry.LOCAL_MACHINE, `SYSTEM\CurrentControlSet\Control\Session Manager\Environment`),
		registryPath(registry.CURRENT_USER, `Environment`),
	)
	if merged := mergeWindowsPath(paths...); merged != "" {
		_ = os.Setenv("Path", merged)
	}
}

func registryPath(root registry.Key, keyPath string) string {
	key, err := registry.OpenKey(root, keyPath, registry.QUERY_VALUE)
	if err != nil {
		return ""
	}
	defer func() { _ = key.Close() }()
	value, valueType, err := key.GetStringValue("Path")
	if err != nil {
		return ""
	}
	if valueType == registry.EXPAND_SZ {
		if expanded, expandErr := registry.ExpandString(value); expandErr == nil {
			value = expanded
		}
	}
	return value
}

func mergeWindowsPath(values ...string) string {
	seen := make(map[string]struct{})
	merged := make([]string, 0)
	for _, value := range values {
		for _, entry := range strings.Split(value, ";") {
			entry = strings.TrimSpace(entry)
			if entry == "" {
				continue
			}
			key := strings.ToLower(strings.TrimRight(entry, `\/`))
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			merged = append(merged, entry)
		}
	}
	return strings.Join(merged, ";")
}

func configureProcessGroup(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.CreationFlags |= windows.CREATE_NO_WINDOW | windows.CREATE_NEW_PROCESS_GROUP
	cmd.SysProcAttr.HideWindow = true
}

func killProcessTree(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}
	kill := exec.Command("taskkill", "/PID", strconv.Itoa(cmd.Process.Pid), "/T", "/F")
	kill.SysProcAttr = &syscall.SysProcAttr{CreationFlags: windows.CREATE_NO_WINDOW, HideWindow: true}
	if err := kill.Run(); err != nil {
		return cmd.Process.Kill()
	}
	return nil
}
