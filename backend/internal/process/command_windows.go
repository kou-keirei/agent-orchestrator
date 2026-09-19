//go:build windows

package process

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sys/windows"
)

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
	// A batch command line treats every percent token as syntax: paired
	// percent signs can name an environment variable, while leading percent
	// forms can be batch parameters (including %% and %0/%*/%~1). Escape all
	// percent signs before cmd gets a chance to classify the token. This also
	// covers mixed values where a literal percent is adjacent to quotes,
	// carets, metacharacters, or trailing slashes.
	if strings.Contains(value, `%`) {
		trailingSlashes := 0
		for i := len(value) - 1; i >= 0 && value[i] == '\\'; i-- {
			trailingSlashes++
		}
		var quoted strings.Builder
		quoted.WriteByte('"')
		remainder := value
		for {
			index := strings.IndexByte(remainder, '%')
			if index < 0 {
				quoted.WriteString(strings.ReplaceAll(remainder, `"`, `\"`))
				quoted.WriteString(strings.Repeat(`\`, trailingSlashes))
				quoted.WriteByte('"')
				return quoted.String()
			}
			quoted.WriteString(strings.ReplaceAll(remainder[:index], `"`, `\"`))
			quoted.WriteByte('"')
			quoted.WriteString(`^%`)
			remainder = remainder[index+1:]
			if remainder == "" {
				return quoted.String()
			}
			quoted.WriteByte('"')
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
