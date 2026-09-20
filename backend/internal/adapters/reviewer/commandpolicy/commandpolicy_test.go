package commandpolicy

import (
	"regexp"
	"testing"
)

func TestSubmitRegexIsExact(t *testing.T) {
	re := regexp.MustCompile("^" + SubmitRegex("sess-1") + "$")
	for _, command := range []string{
		`"$AO_CLI" review submit --session sess-1 --reviews -`,
		`& "$env:AO_CLI" review submit --session sess-1 --reviews -`,
	} {
		if !re.MatchString(command) {
			t.Fatalf("submit regex rejected allowed command %q", command)
		}
	}
	for _, command := range []string{
		`ao review submit --session sess-1 --reviews -`,
		`"$OTHER_CLI" review submit --session sess-1 --reviews -`,
		`"$AO_CLI" browser snapshot`,
		`"$AO_CLI" review list --session sess-1`,
		`"$AO_CLI" review submit --session sess-2 --reviews -`,
		`"$AO_CLI" review submit --session sess-1 --reviews - --json`,
	} {
		if re.MatchString(command) {
			t.Fatalf("submit regex broadened to denylisted command %q", command)
		}
	}
}

func TestSubmitGlobsUseOnlyPinnedReferences(t *testing.T) {
	for _, pattern := range SubmitGlobs() {
		if pattern != PosixSubmitPrefix+" *" && pattern != PowerShellSubmitPrefix+" *" {
			t.Fatalf("unexpected submit pattern %q", pattern)
		}
	}
}
