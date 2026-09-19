package agentlaunch

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
)

// FinalWorkspacePath resolves the cwd used by a Codex child. An unavailable
// cwd is an error: a provider must never silently launch against a fallback
// directory.
func FinalWorkspacePath(workspacePath string) (string, error) {
	workspacePath = strings.TrimSpace(workspacePath)
	if workspacePath == "" {
		var err error
		workspacePath, err = os.Getwd()
		if err != nil {
			return "", fmt.Errorf("resolve Codex workspace path: %w", err)
		}
	}
	// Windows test fixtures and provider payloads may use a rooted POSIX-style
	// path (for example, /tmp/ws). It is still rooted and must not be replaced
	// with the daemon cwd; preserve it as an explicit workspace identity.
	rootedWindowsPath := runtime.GOOS == "windows" && strings.HasPrefix(workspacePath, "/")
	if !filepath.IsAbs(workspacePath) && !rootedWindowsPath {
		absolute, err := filepath.Abs(workspacePath)
		if err != nil {
			return "", fmt.Errorf("resolve Codex workspace path: %w", err)
		}
		workspacePath = absolute
	}
	if rootedWindowsPath {
		return workspacePath, nil
	}
	return filepath.Clean(workspacePath), nil
}

// CodexEnvironment builds the complete environment for a Codex child. Overlay
// values win while the daemon environment remains available to the provider.
func CodexEnvironment(ctx context.Context, executable string, overlay map[string]string) []string {
	merged := make(map[string]string, len(overlay)+1)
	for key, value := range overlay {
		merged[key] = value
	}
	if _, ok := merged["PATH"]; !ok {
		merged["PATH"] = os.Getenv("PATH")
	}
	AugmentRuntimePATHForLaunchBinary(ctx, merged, []string{executable}, nil, PinnedDir(os.Executable, merged["AO_DATA_DIR"]))

	base := make(map[string]string, len(os.Environ())+len(merged))
	for _, entry := range os.Environ() {
		key, _, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		if runtime.GOOS == "windows" {
			key = strings.ToUpper(key)
		}
		base[key] = entry
	}
	keys := make([]string, 0, len(merged))
	for key := range merged {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		canonical := key
		if runtime.GOOS == "windows" {
			canonical = strings.ToUpper(key)
		}
		base[canonical] = key + "=" + merged[key]
	}
	env := make([]string, 0, len(base))
	for _, entry := range base {
		env = append(env, entry)
	}
	sort.Strings(env)
	return env
}

// CodexLaunch is the canonical executable/workspace/environment tuple used by
// Codex probes and provider launches.
type CodexLaunch struct {
	Executable    string
	WorkspacePath string
	Environment   []string
}

func NewCodexLaunch(ctx context.Context, executable, workspacePath string, overlay map[string]string) (CodexLaunch, error) {
	if err := ctx.Err(); err != nil {
		return CodexLaunch{}, err
	}
	executable = strings.TrimSpace(executable)
	if executable == "" {
		return CodexLaunch{}, errors.New("Codex launch executable is required")
	}
	workspacePath, err := FinalWorkspacePath(workspacePath)
	if err != nil {
		return CodexLaunch{}, err
	}
	return CodexLaunch{
		Executable:    executable,
		WorkspacePath: workspacePath,
		Environment:   CodexEnvironment(ctx, executable, overlay),
	}, nil
}

func (c CodexLaunch) WithEnvironment(ctx context.Context, overlay map[string]string) (CodexLaunch, error) {
	return NewCodexLaunch(ctx, c.Executable, c.WorkspacePath, overlay)
}
