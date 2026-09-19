package codexappserver

import (
	"context"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/aoagents/agent-orchestrator/backend/internal/agentlaunch"
)

// LaunchContract is the complete process boundary for one Codex operation.
// Every preflight and launch path must consume the same executable, workspace,
// and merged environment rather than resolving one of them independently.
type LaunchContract struct {
	Executable    string
	WorkspacePath string
	Environment   []string
}

func newLaunchContract(ctx context.Context, executable, workspacePath string, overlay map[string]string) (LaunchContract, error) {
	contract, err := agentlaunch.NewCodexLaunch(ctx, executable, workspacePath, overlay)
	if err != nil {
		return LaunchContract{}, err
	}
	return LaunchContract{
		Executable:    contract.Executable,
		WorkspacePath: contract.WorkspacePath,
		Environment:   contract.Environment,
	}, nil
}

func (c LaunchContract) withEnvironment(ctx context.Context, overlay map[string]string) (LaunchContract, error) {
	return newLaunchContract(ctx, c.Executable, c.WorkspacePath, overlay)
}

// cacheIdentity excludes credentials and other volatile environment values while
// retaining every input that can change which Codex executable or Codex state
// root is observed.
func (c LaunchContract) cacheIdentity() string {
	return c.Executable + "\x00" + c.WorkspacePath + "\x00" +
		launchEnvironmentValue(c.Environment, "PATH") + "\x00" +
		launchEnvironmentValue(c.Environment, "CODEX_HOME")
}

func launchEnvironmentValue(environment []string, key string) string {
	prefix := key + "="
	for _, entry := range environment {
		if strings.HasPrefix(entry, prefix) {
			return strings.TrimPrefix(entry, prefix)
		}
	}
	return ""
}

func finalWorkspacePath(workspacePath string) (string, error) {
	return agentlaunch.FinalWorkspacePath(workspacePath)
}

func isAbsoluteWorkspacePath(workspacePath string) bool {
	return filepath.IsAbs(workspacePath) ||
		(runtime.GOOS == "windows" && strings.HasPrefix(workspacePath, "/"))
}
