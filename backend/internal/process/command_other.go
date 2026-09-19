//go:build !windows

package process

import "os/exec"

type commandInvocation struct {
	name        string
	args        []string
	commandLine string
}

func configureHidden(_ *exec.Cmd) {}

func prepareCommand(name string, args []string) commandInvocation {
	return commandInvocation{name: name, args: args}
}

func setCommandLine(_ *exec.Cmd, _ string) {}
