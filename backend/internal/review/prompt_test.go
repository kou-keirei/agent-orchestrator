package review

import (
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

func TestReviewTextsIncludesMultiPRQueue(t *testing.T) {
	spec := launchSpec()
	spec.RunID = "run-2"
	spec.PRURL = "https://github.com/o/r/pull/2"
	spec.TargetSHA = "sha2"
	spec.ReviewIndex = 1
	spec.ReviewQueue = []ports.ReviewTask{
		{RunID: "run-1", PRURL: "https://github.com/o/r/pull/1", TargetSHA: "sha1"},
		{RunID: "run-2", PRURL: "https://github.com/o/r/pull/2", TargetSHA: "sha2"},
	}

	prompt, _ := reviewTextsForOS(spec, false)
	for _, want := range []string{
		"AO created 2 review tasks",
		"Review every queued PR, then submit all results together",
		"Complete every review task in the queue autonomously",
		"Do not ask the user whether to continue to the next PR",
		"* 1. https://github.com/o/r/pull/1 (head commit sha1, run run-1)",
		"* 2. https://github.com/o/r/pull/2 (head commit sha2, run run-2)",
		"After every PR has its own GitHub review from step 1",
		"printf '%s'",
		"do not use a heredoc",
		`"$AO_CLI" review submit --session mer-1 --reviews -`,
		`"reviews": [`,
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing %q:\n%s", want, prompt)
		}
	}
}

func TestReviewTextsUsesShellAwareReviewerCommands(t *testing.T) {
	spec := launchSpec()

	for _, test := range []struct {
		name          string
		windows       bool
		githubCommand string
		submitCommand string
		quoteRule     string
		absent        string
	}{
		{
			name:          "posix",
			githubCommand: "printf '%s'",
			submitCommand: `| "$AO_CLI" review submit --session mer-1 --reviews -`,
			quoteRule:     "shell-escape any single quotes",
			absent:        "& $env:AO_CLI",
		},
		{
			name:          "windows powershell",
			windows:       true,
			githubCommand: "Write-Output",
			submitCommand: `| & "$env:AO_CLI" review submit --session mer-1 --reviews -`,
			quoteRule:     "double any single quotes",
			absent:        `"$AO_CLI" review submit`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			prompt, _ := reviewTextsForOS(spec, test.windows)
			for _, want := range []string{test.githubCommand, test.submitCommand, test.quoteRule, "gh api --method POST", `"event": "COMMENT"`} {
				if !strings.Contains(prompt, want) {
					t.Fatalf("prompt missing %q:\n%s", want, prompt)
				}
			}
			if strings.Contains(prompt, test.absent) {
				t.Fatalf("prompt contains wrong-shell command %q:\n%s", test.absent, prompt)
			}
		})
	}
}
