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
		"jq -cn",
		"do not use a heredoc",
		`"$AO_CLI" review submit --session mer-1 --reviews -`,
		"--arg body",
		"exactly one object per queued PR/run",
		"one object to \"comments\" per inline finding",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing %q:\n%s", want, prompt)
		}
	}
}

func TestReviewTextsQueueAndCommentCardinality(t *testing.T) {
	for _, tc := range []struct {
		name   string
		queue  []ports.ReviewTask
		want   []string
		absent []string
	}{
		{name: "zero", want: []string{"Review task queue is empty", "zero review objects"}, absent: []string{"* 1."}},
		{name: "one", queue: []ports.ReviewTask{{RunID: "run-1", PRURL: "https://github.com/o/r/pull/1", TargetSHA: "sha1"}}, want: []string{"* 1. https://github.com/o/r/pull/1", "exactly one object per queued PR/run"}},
		{name: "many", queue: []ports.ReviewTask{{RunID: "run-1"}, {RunID: "run-2"}}, want: []string{"AO created 2 review tasks", "one for every item in a larger queue"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := launchSpec()
			spec.ReviewQueue = tc.queue
			if tc.name == "zero" {
				spec.RunID = ""
				spec.PRURL = ""
				spec.TargetSHA = ""
			}
			prompt, _ := reviewTextsForOS(spec, false)
			for _, want := range tc.want {
				if !strings.Contains(prompt, want) {
					t.Fatalf("prompt missing %q:\n%s", want, prompt)
				}
			}
			for _, absent := range tc.absent {
				if strings.Contains(prompt, absent) {
					t.Fatalf("prompt contains %q:\n%s", absent, prompt)
				}
			}
		})
	}
}

func TestReviewTextsRequiresSerializerSafePayloadValues(t *testing.T) {
	prompt, _ := reviewTextsForOS(launchSpec(), false)
	promptWindows, _ := reviewTextsForOS(launchSpec(), true)
	for _, want := range []string{
		"multiline text, quotes, backslashes, dollar signs, backticks, Unicode, braces, and control characters",
		"--arg path \"$path\"",
		"--arg finding \"$finding\"",
		"--arg body \"$body\"",
		"do not hand-escape",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing serializer-safety requirement %q:\n%s", want, prompt)
		}
	}
	if !strings.Contains(promptWindows, "ConvertTo-Json -Compress") {
		t.Fatalf("PowerShell prompt missing real serializer:\n%s", promptWindows)
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
			githubCommand: "jq -cn",
			submitCommand: `| "$AO_CLI" review submit --session mer-1 --reviews -`,
			quoteRule:     "Use jq -cn for real JSON",
			absent:        "& $env:AO_CLI",
		},
		{
			name:          "windows powershell",
			windows:       true,
			githubCommand: "ConvertTo-Json -Compress",
			submitCommand: `| & "$env:AO_CLI" review submit --session mer-1 --reviews -`,
			quoteRule:     "Use ConvertTo-Json -Compress for real JSON",
			absent:        `"$AO_CLI" review submit`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			prompt, _ := reviewTextsForOS(spec, test.windows)
			for _, want := range []string{test.githubCommand, test.submitCommand, test.quoteRule, "gh api --method POST", "event", "COMMENT", "do not hand-escape", "Never resolve ao or ao.exe through PATH", "invoke any other AO subcommand"} {
				if !strings.Contains(prompt, want) {
					t.Fatalf("prompt missing %q:\n%s", want, prompt)
				}
			}
			if strings.Contains(prompt, test.absent) {
				t.Fatalf("prompt contains wrong-shell command %q:\n%s", test.absent, prompt)
			}
			forbidden := []string{`printf '%s' '{`, `Write-Output '{`, `"body": "<your full review markdown>"`, `ao review submit`, `"$OTHER_CLI" review submit`}
			for _, value := range forbidden {
				if strings.Contains(prompt, value) {
					t.Fatalf("prompt contains unsafe literal %q:\n%s", value, prompt)
				}
			}
		})
	}
}
