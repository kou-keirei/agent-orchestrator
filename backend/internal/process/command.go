package process

import (
	"context"
	"os/exec"
)

// Command creates a non-interactive child process. On Windows it suppresses
// transient console windows for CLI tools launched by the desktop daemon.
func Command(name string, args ...string) *exec.Cmd {
	invocation := prepareCommand(name, args)
	cmd := exec.Command(invocation.name, invocation.args...)
	configureHidden(cmd)
	setCommandLine(cmd, invocation.commandLine)
	return cmd
}

// CommandContext is Command with cancellation support.
func CommandContext(ctx context.Context, name string, args ...string) *exec.Cmd {
	invocation := prepareCommand(name, args)
	cmd := exec.CommandContext(ctx, invocation.name, invocation.args...)
	configureHidden(cmd)
	setCommandLine(cmd, invocation.commandLine)
	return cmd
}
