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
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/registry"

	aoprocess "github.com/aoagents/agent-orchestrator/backend/internal/process"
)

func init() {
	capabilityCommandOutput = windowsCapabilityCommandOutput
	commandRun = windowsCommandRun
}

// windowsCommandRun admits only the non-interactive CommandRunner boundary.
// Parent deadlines are preserved up to a 30-second command ceiling; output is
// limited to 256 KiB per stream to bound caller-owned capture buffers. As with
// exec.Cmd, supplied writers must return from Write; arbitrary blocked writer
// implementations cannot be canceled by closing the command's pipe handles.
func windowsCommandRun(parent context.Context, argv []string, stdout, stderr io.Writer) error {
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	out, outWriter, err := os.Pipe()
	if err != nil {
		return err
	}
	defer out.Close()
	defer outWriter.Close()
	diagnostic, errWriter, err := os.Pipe()
	if err != nil {
		return err
	}
	defer diagnostic.Close()
	defer errWriter.Close()
	// Serialize both streams, including when the caller supplies one shared
	// bytes.Buffer or another writer that is not safe for concurrent writes.
	var outputMu sync.Mutex
	copyStream := func(reader *os.File, writer io.Writer) <-chan error {
		done := make(chan error, 1)
		if writer == nil {
			writer = io.Discard
		}
		go func() {
			buffer := make([]byte, 32<<10)
			total := 0
			for {
				n, readErr := reader.Read(buffer)
				if n > 0 {
					total += n
					if total > 256<<10 {
						cancel()
						done <- errors.New("Windows command output exceeds 256 KiB stream limit")
						return
					}
					outputMu.Lock()
					written, writeErr := writer.Write(buffer[:n])
					outputMu.Unlock()
					if writeErr == nil && written != n {
						writeErr = io.ErrShortWrite
					}
					if writeErr != nil {
						cancel()
						done <- writeErr
						return
					}
				}
				if readErr != nil {
					if errors.Is(readErr, io.EOF) {
						readErr = nil
					}
					done <- readErr
					return
				}
			}
		}()
		return done
	}
	outDone, errDone := copyStream(out, stdout), copyStream(diagnostic, stderr)
	err = aoprocess.RunWindowsCommandFiles(ctx, aoprocess.WindowsCommandBudget{
		MaxProcesses: 4, CommitBytes: 512 << 20,
	}, nil, outWriter, errWriter, argv[0], argv[1:]...)
	outWriter.Close()
	errWriter.Close()
	// Job teardown closes inherited writers even when the root exits first.
	// Retain exec.Cmd's finite pipe-drain allowance for handles retained outside
	// the supported subtree. This does not attest to provider-internal tools.
	timer := time.AfterFunc(5*time.Second, func() {
		out.Close()
		diagnostic.Close()
	})
	outErr, diagnosticErr := <-outDone, <-errDone
	timer.Stop()
	return errors.Join(err, outErr, diagnosticErr)
}

// Only the fixed, non-interactive Settings capability probes use this budget.
// Installers and provider-internal tools are not covered here. Adapter.Run has
// its own streaming admission path above.
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
