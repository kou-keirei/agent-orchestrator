package codexappserver

import (
	"context"
	"strings"
	"testing"
)

func TestLaunchContractMergesEnvironmentAndInvalidatesIdentity(t *testing.T) {
	workspace := t.TempDir()
	contract, err := newLaunchContract(context.Background(), "codex", workspace, map[string]string{
		"PATH":       `C:\workspace\node_modules\.bin;C:\Windows\System32`,
		"CODEX_HOME": `C:\ao\codex-home`,
	})
	if err != nil {
		t.Fatalf("newLaunchContract: %v", err)
	}
	if contract.WorkspacePath != workspace {
		t.Fatalf("workspace = %q, want %q", contract.WorkspacePath, workspace)
	}
	if got := launchEnvironmentValue(contract.Environment, "PATH"); !strings.HasPrefix(got, `C:\workspace\node_modules\.bin`) {
		t.Fatalf("PATH = %q, want workspace launcher precedence", got)
	}
	if got := launchEnvironmentValue(contract.Environment, "CODEX_HOME"); got != `C:\ao\codex-home` {
		t.Fatalf("CODEX_HOME = %q", got)
	}

	changedPath, err := contract.withEnvironment(context.Background(), map[string]string{"PATH": `C:\other\bin`})
	if err != nil {
		t.Fatalf("withEnvironment: %v", err)
	}
	if contract.cacheIdentity() == changedPath.cacheIdentity() {
		t.Fatal("cache identity did not change when launch PATH changed")
	}

	changedHome, err := contract.withEnvironment(context.Background(), map[string]string{
		"PATH":       `C:\workspace\node_modules\.bin;C:\Windows\System32`,
		"CODEX_HOME": `C:\other\codex-home`,
	})
	if err != nil {
		t.Fatalf("withEnvironment with changed Codex home: %v", err)
	}
	if contract.cacheIdentity() == changedHome.cacheIdentity() {
		t.Fatal("cache identity did not change when CODEX_HOME changed")
	}
}
