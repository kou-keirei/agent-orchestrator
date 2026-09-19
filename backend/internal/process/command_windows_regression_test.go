//go:build windows

package process

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestCommandContextInvokesWindowsScriptShimWithOriginalArgv(t *testing.T) {
	path := filepath.Join(t.TempDir(), "Codex install", "codex.cmd")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("@echo off\r\necho %~1\r\necho %~2\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	comSpec := strings.TrimSpace(os.Getenv("ComSpec"))
	if comSpec == "" {
		comSpec = strings.TrimSpace(os.Getenv("COMSPEC"))
	}
	if comSpec == "" {
		comSpec = "cmd.exe"
	}

	expectedShell, err := exec.LookPath(comSpec)
	if err != nil {
		t.Fatalf("resolve command interpreter %q: %v", comSpec, err)
	}
	cmd := CommandContext(context.Background(), path, "app-server", "value with spaces")
	if !strings.EqualFold(cmd.Path, expectedShell) {
		t.Fatalf("command path = %q, want resolved shell %q", cmd.Path, expectedShell)
	}
	if want := []string{"/d", "/v:off", "/s", "/c", path, "app-server", "value with spaces"}; !reflect.DeepEqual(cmd.Args[1:], want) {
		t.Fatalf("command argv = %#v, want %#v", cmd.Args[1:], want)
	}
	if cmd.SysProcAttr == nil || !strings.Contains(cmd.SysProcAttr.CmdLine, `"`+path+`" "app-server" "value with spaces"`) {
		t.Fatalf("command line = %q, want quoted shim and arguments", cmd.SysProcAttr.CmdLine)
	}

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("script shim: %v (%s)", err, out)
	}
	if got, want := string(out), "app-server\r\nvalue with spaces\r\n"; got != want {
		t.Fatalf("script output = %q, want %q", got, want)
	}
}

func TestCommandContextPreservesCmdSensitiveShimArgv(t *testing.T) {
	t.Setenv("AO_PROCESS_DELAYED_VALUE", "expanded")
	quoted := []string{
		"",
		`embedded "quote"`,
		`embedded "quote"\`,
		`backslash\ "quote`,
		`backslash\\ "quote"`,
		`C:\trailing\`,
		`C:\trailing\\`,
	}
	if got := runWindowsShimArgv(t, quoted); !reflect.DeepEqual(got, quoted) {
		t.Fatalf("shim argv = %#v, want %#v", got, quoted)
	}
	percent := []string{
		`100%`,
		`percent%value`,
		`100% complete 100%`,
		`%`,
		`%%`,
		`%%%`,
		`%0`,
		`%*`,
		`%~1`,
		`%VALUE`,
		`prefix %0 suffix`,
		`%0\`,
		`%%\`,
		`prefix %*\`,
		`%0 "quoted"`,
		`%* &pipe`,
		`%~1|pipe`,
		`%%0`,
		`%%%0`,
		`%% %0`,
		`!bang!`,
		`!AO_PROCESS_DELAYED_VALUE!`,
		`prefix !AO_PROCESS_DELAYED_VALUE! suffix`,
	}
	if got := runWindowsShimArgv(t, percent); !reflect.DeepEqual(got, percent) {
		t.Fatalf("shim argv = %#v, want %#v", got, percent)
	}
}

func TestCommandContextPreservesBatShimArgv(t *testing.T) {
	want := []string{
		"",
		`100%`,
		`%%`,
		`%0`,
		`%*`,
		`%~1`,
		`%VALUE`,
		`prefix %0 suffix`,
		`%0\`,
		`%%\`,
		`^caret^ "quoted" &pipe|less<greater> (paren)`,
		`C:\trailing\\`,
	}
	if got := runWindowsShimArgvWithExtension(t, ".bat", want); !reflect.DeepEqual(got, want) {
		t.Fatalf("bat shim argv = %#v, want %#v", got, want)
	}
}

func TestCommandContextPreservesLiteralCaretShimArgv(t *testing.T) {
	want := []string{`^caret^`, `^`, `^^`}
	if got := runWindowsShimArgv(t, want); !reflect.DeepEqual(got, want) {
		t.Fatalf("shim argv = %#v, want %#v", got, want)
	}
}

func TestCommandContextPreservesShellMetacharShimArgv(t *testing.T) {
	want := []string{`amp&pipe|less<greater>`, `paren(inside)`}
	if got := runWindowsShimArgv(t, want); !reflect.DeepEqual(got, want) {
		t.Fatalf("shim argv = %#v, want %#v", got, want)
	}
}

func TestCommandContextPreservesPairedPercentExpansionAndMixedShimArgv(t *testing.T) {
	t.Setenv("AO_PROCESS_PERCENT_VALUE", "expanded")
	want := []string{
		"before%AO_PROCESS_PERCENT_VALUE%after",
		`^caret^ "quoted" &pipe|%AO_PROCESS_PERCENT_VALUE%`,
		`%AO_PROCESS_PERCENT_VALUE%\`,
	}
	input := []string{
		"before%AO_PROCESS_PERCENT_VALUE%after",
		`^caret^ "quoted" &pipe|%AO_PROCESS_PERCENT_VALUE%`,
		`%AO_PROCESS_PERCENT_VALUE%\`,
	}
	if got := runWindowsShimArgv(t, input); !reflect.DeepEqual(got, want) {
		t.Fatalf("shim argv = %#v, want %#v", got, want)
	}
}

func runWindowsShimArgv(t *testing.T, want []string) []string {
	return runWindowsShimArgvWithExtension(t, ".cmd", want)
}

func runWindowsShimArgvWithExtension(t *testing.T, extension string, want []string) []string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "Codex install", "codex"+extension)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("@echo off\r\nsetlocal DisableDelayedExpansion\r\n\"%AO_PROCESS_ARGV_HELPER_EXE%\" -test.run=TestCommandShimArgvHelper -- %*\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	outputPath := filepath.Join(t.TempDir(), "argv.jsonl")
	cmd := CommandContext(context.Background(), path, want...)
	cmd.Env = append(os.Environ(),
		"AO_PROCESS_ARGV_HELPER_EXE="+os.Args[0],
		"AO_PROCESS_ARGV_OUTPUT="+outputPath,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("script shim: %v (%s)", err, out)
	}
	out, err = os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read helper output: %v", err)
	}
	var got []string
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		line = strings.TrimSuffix(line, "\r")
		var arg string
		if err := json.Unmarshal([]byte(line), &arg); err != nil {
			t.Fatalf("decode helper output %q: %v", line, err)
		}
		got = append(got, arg)
	}
	return got
}

func TestCommandShimArgvHelper(t *testing.T) {
	outputPath := os.Getenv("AO_PROCESS_ARGV_OUTPUT")
	if outputPath == "" {
		return
	}
	separator := -1
	for i, arg := range os.Args {
		if arg == "--" {
			separator = i
			break
		}
	}
	if separator == -1 {
		t.Fatal("helper invocation has no argument separator")
	}
	var lines []string
	for _, arg := range os.Args[separator+1:] {
		encoded, err := json.Marshal(arg)
		if err != nil {
			t.Fatal(err)
		}
		lines = append(lines, string(encoded))
	}
	if err := os.WriteFile(outputPath, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}
