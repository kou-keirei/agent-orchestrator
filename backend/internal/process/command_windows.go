//go:build windows

package process

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

// WindowsCommandBudget bounds a single AO-created command job, including its
// ordinary descendants. MaxProcesses includes the root; CommitBytes is the
// job-wide committed-memory limit, not a working-set or physical-memory limit.
type WindowsCommandBudget struct {
	MaxProcesses uint32
	CommitBytes  uintptr
}

// RunWindowsCommand runs an explicitly admitted, non-interactive command in a
// fresh Windows job. It requires a deadline, inherits the environment and working
// directory, and connects standard streams to NUL. Use RunWindowsCommandFiles
// to supply explicit file streams.
//
// Only explicitly integrated AO callers are covered; this is not evidence of
// containment of provider-internal Codex/ACP tool subprocesses.
// Windows 10+ job-list admission must succeed; there is no uncontained fallback.
func RunWindowsCommand(ctx context.Context, budget WindowsCommandBudget, name string, args ...string) (err error) {
	return RunWindowsCommandFiles(ctx, budget, nil, nil, nil, name, args...)
}

// RunWindowsCommandFiles is RunWindowsCommand with explicit standard streams.
// Nil streams use NUL. Only duplicates of these three file handles are inherited;
// the job handle is never inherited. Files remain owned by the caller. The
// environment and working directory are inherited unchanged, as in exec.Cmd
// with nil Env and empty Dir. No arbitrary writer goroutines run in this layer.
func RunWindowsCommandFiles(ctx context.Context, budget WindowsCommandBudget, stdin, stdout, stderr *os.File, name string, args ...string) (err error) {
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("Windows command budget requires a context deadline")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	job, err := newWindowsCommandJob(budget)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, windows.CloseHandle(job)) }()
	child, err := createWindowsJobCommandFiles(job, stdin, stdout, stderr, name, args)
	if err != nil {
		return err
	}
	defer func() {
		// This exact job is ours, and is never inherited by the child. Terminate
		// remaining descendants even when the root exited successfully.
		err = errors.Join(err, windows.TerminateJobObject(job, 1))
		state, waitErr := windows.WaitForSingleObject(child.Process, 5000)
		err = errors.Join(err, waitErr)
		if waitErr == nil && state != windows.WAIT_OBJECT_0 {
			err = errors.Join(err, errors.New("Windows command cleanup wait timed out"))
		}
		err = errors.Join(err, windows.CloseHandle(child.Thread), windows.CloseHandle(child.Process))
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := windows.ResumeThread(child.Thread); err != nil {
		return fmt.Errorf("resume admitted Windows command: %w", err)
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		state, err := windows.WaitForSingleObject(child.Process, 10)
		if err != nil {
			return fmt.Errorf("wait for Windows command: %w", err)
		}
		if state == uint32(windows.WAIT_TIMEOUT) {
			continue
		}
		if state != windows.WAIT_OBJECT_0 {
			return fmt.Errorf("unexpected Windows command wait state: %d", state)
		}
		// Wait on a Go-owned duplicate only after the native wait signaled, so
		// callers retain the usual *exec.ExitError and ProcessState contract.
		process, err := os.FindProcess(int(child.ProcessId))
		if err != nil {
			return err
		}
		processState, err := process.Wait()
		if err != nil {
			return err
		}
		if !processState.Success() {
			return &exec.ExitError{ProcessState: processState}
		}
		return nil
	}
}

func newWindowsCommandJob(budget WindowsCommandBudget) (windows.Handle, error) {
	if budget.MaxProcesses == 0 || budget.CommitBytes == 0 {
		return 0, errors.New("Windows command budget requires positive process and commit limits")
	}
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return 0, fmt.Errorf("create Windows command job: %w", err)
	}
	limits := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{JobMemoryLimit: budget.CommitBytes}
	limits.BasicLimitInformation.ActiveProcessLimit = budget.MaxProcesses
	limits.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_ACTIVE_PROCESS |
		windows.JOB_OBJECT_LIMIT_JOB_MEMORY | windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err := windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&limits)), uint32(unsafe.Sizeof(limits))); err != nil {
		return 0, errors.Join(fmt.Errorf("set Windows command job limits: %w", err), windows.CloseHandle(job))
	}
	return job, nil
}

func createWindowsJobCommand(job windows.Handle, name string, args []string) (windows.ProcessInformation, error) {
	return createWindowsJobCommandFiles(job, nil, nil, nil, name, args)
}

func createWindowsJobCommandFiles(job windows.Handle, stdin, stdout, stderr *os.File, name string, args []string) (windows.ProcessInformation, error) {
	var child windows.ProcessInformation
	// Resolve first: an extensionless name such as npm may select npm.cmd.
	// Shim preparation must see that resolved extension, as in systemexec.
	resolved, err := exec.LookPath(name)
	if err != nil {
		return child, err
	}
	invocation := prepareCommand(resolved, args)
	path, err := exec.LookPath(invocation.name)
	if err != nil {
		return child, err
	}
	app, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return child, err
	}
	line := windows.ComposeCommandLine(append([]string{path}, invocation.args...))
	if invocation.commandLine != "" {
		line = windows.EscapeArg(path) + " " + invocation.commandLine
	}
	commandLine, err := windows.UTF16PtrFromString(line)
	if err != nil {
		return child, err
	}
	attributes, err := windows.NewProcThreadAttributeList(2)
	if err != nil {
		return child, err
	}
	defer attributes.Delete()
	// PROC_THREAD_ATTRIBUTE_JOB_LIST (Windows 10+): admission is part of
	// CreateProcess, before any child code executes. No post-start PID attach.
	const jobListAttribute = 0x0002000d
	if err := attributes.Update(jobListAttribute, unsafe.Pointer(&job), unsafe.Sizeof(job)); err != nil {
		return child, fmt.Errorf("configure Windows command job admission: %w", err)
	}
	startup := windows.StartupInfoEx{ProcThreadAttributeList: attributes.List()}
	startup.Cb = uint32(unsafe.Sizeof(startup))
	null, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		return child, err
	}
	defer null.Close()
	var inherited []windows.Handle
	defer func() {
		for _, handle := range inherited {
			windows.CloseHandle(handle)
		}
	}()
	for _, file := range []*os.File{stdin, stdout, stderr} {
		if file == nil {
			file = null
		}
		var duplicate windows.Handle
		if err := windows.DuplicateHandle(windows.CurrentProcess(), windows.Handle(file.Fd()),
			windows.CurrentProcess(), &duplicate, 0, true, windows.DUPLICATE_SAME_ACCESS); err != nil {
			return child, err
		}
		inherited = append(inherited, duplicate)
	}
	if err := attributes.Update(windows.PROC_THREAD_ATTRIBUTE_HANDLE_LIST, unsafe.Pointer(&inherited[0]),
		uintptr(len(inherited))*unsafe.Sizeof(inherited[0])); err != nil {
		return child, err
	}
	startup.Flags = windows.STARTF_USESTDHANDLES
	startup.StdInput, startup.StdOutput, startup.StdErr = inherited[0], inherited[1], inherited[2]
	flags := uint32(windows.EXTENDED_STARTUPINFO_PRESENT | windows.CREATE_SUSPENDED | windows.CREATE_NO_WINDOW)
	if err := windows.CreateProcess(app, commandLine, nil, nil, true, flags, nil, nil, &startup.StartupInfo, &child); err != nil {
		return child, fmt.Errorf("create admitted Windows command: %w", err)
	}
	return child, nil
}

type commandInvocation struct {
	name        string
	args        []string
	commandLine string
}

func configureHidden(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.CreationFlags |= windows.CREATE_NO_WINDOW
	cmd.SysProcAttr.HideWindow = true
}

// prepareCommand gives Windows command shims the same invocation semantics as
// a native executable. os/exec deliberately does not invoke cmd.exe for .cmd
// and .bat files, even though those are the executables npm puts on PATH.
// Keeping this at the process boundary covers non-interactive Codex probes,
// app-server/account clients, and interactive login without teaching each
// caller a subtly different shell command shape.
func prepareCommand(name string, args []string) commandInvocation {
	ext := strings.ToLower(filepath.Ext(name))
	if ext != ".cmd" && ext != ".bat" {
		return commandInvocation{name: name, args: args}
	}
	shell := strings.TrimSpace(os.Getenv("ComSpec"))
	if shell == "" {
		shell = strings.TrimSpace(os.Getenv("COMSPEC"))
	}
	if shell == "" {
		shell = "cmd.exe"
	}
	// cmd.exe's /s /c parsing uses the raw command line, not native argv
	// semantics. Quote the complete command explicitly so spaces in the shim
	// path and arguments cannot change where the command ends.
	prepared := make([]string, 0, len(args)+5)
	prepared = append(prepared, "/d", "/v:off", "/s", "/c", name)
	prepared = append(prepared, args...)
	return commandInvocation{
		name:        shell,
		args:        prepared,
		commandLine: `/d /v:off /s /c "` + windowsBatchCommandLine(name, args) + `"`,
	}
}

func setCommandLine(cmd *exec.Cmd, commandLine string) {
	if commandLine != "" {
		cmd.SysProcAttr.CmdLine = commandLine
	}
}

func windowsBatchCommandLine(executable string, args []string) string {
	parts := make([]string, 0, len(args)+1)
	parts = append(parts, quoteWindowsBatchArg(executable))
	for _, arg := range args {
		parts = append(parts, quoteWindowsBatchArg(arg))
	}
	return strings.Join(parts, " ")
}

func quoteWindowsBatchArg(value string) string {
	// Only a percent-delimited environment name is expanded by cmd in this
	// boundary. Leave literal percent and batch-parameter forms on the native
	// quoting path; they are not syntax until they occur in a batch source line.
	if windowsBatchEnvReference(value) {
		trailingSlashes := 0
		for i := len(value) - 1; i >= 0 && value[i] == '\\'; i-- {
			trailingSlashes++
		}
		var quoted strings.Builder
		quoted.WriteByte('"')
		remainder := strings.TrimRight(value, `\`)
		for {
			start, end := windowsBatchEnvReferenceBounds(remainder)
			if start < 0 {
				quoted.WriteString(strings.ReplaceAll(remainder, `"`, `\"`))
				quoted.WriteString(strings.Repeat(`\`, trailingSlashes*2))
				quoted.WriteByte('"')
				return quoted.String()
			}
			quoted.WriteString(strings.ReplaceAll(remainder[:start], `"`, `\"`))
			quoted.WriteByte('"')
			quoted.WriteString(`^%`)
			quoted.WriteString(remainder[start+1 : end-1])
			quoted.WriteString(`^%`)
			quoted.WriteByte('"')
			remainder = remainder[end:]
		}
	}
	if strings.Contains(value, `^`) && strings.ContainsAny(value, `"&|<>()`) {
		return `"` + strings.ReplaceAll(value, `"`, `"""`) + `"`
	}
	// A literal caret without another cmd metacharacter is consumed once while
	// cmd launches the batch file and once again when the batch forwards %* to
	// the native child. Keep this narrow escape for the values where the two
	// parsing layers are deterministic; mixed caret/metacharacter input remains
	// subject to cmd's batch-file grammar.
	if strings.Contains(value, `^`) && !strings.ContainsAny(value, " \t\"&|<>()%!") {
		return strings.ReplaceAll(value, `^`, `^^^^`)
	}
	return quoteWindowsNativeArg(value)
}

func windowsBatchEnvReference(value string) bool {
	start, _ := windowsBatchEnvReferenceBounds(value)
	return start >= 0
}

func windowsBatchEnvReferenceBounds(value string) (int, int) {
	for start := 0; start < len(value); start++ {
		if value[start] != '%' || start+2 >= len(value) || !isWindowsBatchEnvNameStart(value[start+1]) {
			continue
		}
		end := start + 2
		for end < len(value) && isWindowsBatchEnvNameChar(value[end]) {
			end++
		}
		if end < len(value) && value[end] == '%' {
			return start, end + 1
		}
	}
	return -1, -1
}

func isWindowsBatchEnvNameStart(char byte) bool {
	return char == '_' || char >= 'A' && char <= 'Z' || char >= 'a' && char <= 'z'
}

func isWindowsBatchEnvNameChar(char byte) bool {
	return isWindowsBatchEnvNameStart(char) || char >= '0' && char <= '9'
}

func quoteWindowsNativeArg(value string) string {
	var quoted strings.Builder
	quoted.WriteByte('"')
	backslashes := 0
	for _, char := range value {
		if char == '\\' {
			backslashes++
			continue
		}
		if char == '"' {
			quoted.WriteString(strings.Repeat(`\`, backslashes*2+1))
			quoted.WriteRune(char)
			backslashes = 0
			continue
		}
		quoted.WriteString(strings.Repeat(`\`, backslashes))
		quoted.WriteRune(char)
		backslashes = 0
	}
	quoted.WriteString(strings.Repeat(`\`, backslashes*2))
	quoted.WriteByte('"')
	return quoted.String()
}
