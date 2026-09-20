// Package commandpolicy contains the exact shell command identities shared by
// reviewer adapters. The AO submit executable is an environment-pinned
// absolute path; the variable spelling is part of the allowlisted command.
package commandpolicy

import "regexp"

const (
	PosixSubmitPrefix      = `"$AO_CLI" review submit`
	PowerShellSubmitPrefix = `& "$env:AO_CLI" review submit`
)

// SubmitGlobs are provider-policy patterns for the only AO command a reviewer
// may invoke. They intentionally do not match another executable or another
// AO subcommand.
func SubmitGlobs() []string {
	return []string{
		PosixSubmitPrefix + " *",
		PowerShellSubmitPrefix + " *",
	}
}

// SubmitRegex returns a full-command matcher for Kiro's regex policy.
func SubmitRegex(session string) string {
	return `(?:"\$AO_CLI"|& "\$env:AO_CLI") review submit --session ` + regexp.QuoteMeta(session) + ` --reviews -`
}
