package hooksjson

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

func TestCanonicalAOHookFormsAreShellAwareAndCrossPlatform(t *testing.T) {
	posix := CanonicalAOHookPrefixForOS("demo", false)
	windows := CanonicalAOHookPrefixForOS("demo", true)
	if posix != `"${AO_CLI:?AO_CLI is not set}" hooks demo ` {
		t.Fatalf("POSIX prefix = %q", posix)
	}
	if windows != `if (-not $env:AO_CLI) { throw 'AO_CLI is not set' }; & "$env:AO_CLI" hooks demo ` {
		t.Fatalf("Windows prefix = %q", windows)
	}
	if CanonicalAOHookShellForOS(false) != "bash" || CanonicalAOHookShellForOS(true) != "powershell" {
		t.Fatalf("shells = (%q, %q), want (bash, powershell)", CanonicalAOHookShellForOS(false), CanonicalAOHookShellForOS(true))
	}
	if got := CanonicalAOHookPrefixes("demo"); !reflect.DeepEqual(got, []string{posix, windows}) {
		t.Fatalf("prefixes = %#v, want %#v", got, []string{posix, windows})
	}
	wantCurrent := posix
	if runtime.GOOS == "windows" {
		wantCurrent = windows
	}
	if got := CanonicalAOHookPrefix("demo"); got != wantCurrent {
		t.Fatalf("current prefix = %q, want %q", got, wantCurrent)
	}
}

func TestManagerMigratesBothCanonicalPlatformsAndOwnsUninstall(t *testing.T) {
	workspace := t.TempDir()
	hooksPath := filepath.Join(workspace, "hooks.json")
	prefixes := CanonicalAOHookPrefixes("current")
	current := CanonicalAOHookPrefix("current")
	opposite := prefixes[0]
	if opposite == current {
		opposite = prefixes[1]
	}
	managed := current + "stop"
	original := map[string]any{
		"hooks": map[string]any{
			"Stop": []any{map[string]any{
				"hooks": []any{
					map[string]any{"type": "command", "command": prefixes[0] + "stop"},
					map[string]any{"type": "command", "command": prefixes[1] + "stop"},
					map[string]any{"type": "command", "command": "ao hooks sibling stop"},
				},
			}},
		},
	}
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(hooksPath, data, 0o600); err != nil {
		t.Fatal(err)
	}

	manager := Manager{
		Label:                    "current",
		CommandPrefix:            CanonicalAOHookPrefix("current"),
		LegacyCommandPrefixes:    prefixes,
		UninstallCommandPrefixes: prefixes,
		Path:                     func(string) string { return hooksPath },
		Managed:                  []HookSpec{{Event: "Stop", Command: managed}},
	}
	if err := manager.Install(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	assertHookCommandCount(t, hooksPath, current+"stop", 1)
	assertHookCommandCount(t, hooksPath, opposite+"stop", 0)
	assertHookCommandCount(t, hooksPath, "ao hooks sibling stop", 1)

	if err := manager.Uninstall(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	assertHookCommandCount(t, hooksPath, current+"stop", 0)
	assertHookCommandCount(t, hooksPath, opposite+"stop", 0)
	assertHookCommandCount(t, hooksPath, "ao hooks sibling stop", 1)
}

func assertHookCommandCount(t *testing.T, hooksPath, command string, want int) {
	t.Helper()
	data, err := os.ReadFile(hooksPath)
	if err != nil {
		t.Fatal(err)
	}
	var settings struct {
		Hooks map[string][]MatcherGroup `json:"hooks"`
	}
	if err := json.Unmarshal(data, &settings); err != nil {
		t.Fatal(err)
	}
	got := 0
	for _, group := range settings.Hooks["Stop"] {
		for _, hook := range group.Hooks {
			if hook.Command == command {
				got++
			}
		}
	}
	if got != want {
		t.Fatalf("command %q count = %d, want %d", command, got, want)
	}
}

func TestReconcileHookMovesCommandToDeclaredMatcher(t *testing.T) {
	oldMatcher := "old"
	newMatcher := "new"
	managed := "ao hooks test session-start"
	userOld := HookEntry{Type: "command", Command: "user old", Timeout: 3}
	userNew := HookEntry{Type: "command", Command: "user new", Timeout: 4}
	updated := HookEntry{Type: "command", Command: managed, Timeout: 30}
	groups := []MatcherGroup{
		{Matcher: &oldMatcher, Hooks: []HookEntry{{Type: "command", Command: managed, Timeout: 5}, userOld}},
		{Matcher: &newMatcher, Hooks: []HookEntry{userNew}},
	}

	got := reconcileHook(groups, updated, &newMatcher)
	want := []MatcherGroup{
		{Matcher: &oldMatcher, Hooks: []HookEntry{userOld}},
		{Matcher: &newMatcher, Hooks: []HookEntry{userNew, updated}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("reconcileHook()\nwant: %#v\n got: %#v", want, got)
	}
}

func TestReconcileHookDeduplicatesCommandAcrossGroups(t *testing.T) {
	firstMatcher := "first"
	targetMatcher := "target"
	managed := "ao hooks test session-start"
	user := HookEntry{Type: "command", Command: "user hook", Timeout: 7}
	updated := HookEntry{Type: "command", Command: managed, Timeout: 30}
	groups := []MatcherGroup{
		{Hooks: []HookEntry{{Type: "command", Command: managed, Timeout: 1}}},
		{Matcher: &firstMatcher, Hooks: []HookEntry{user, {Type: "command", Command: managed, Timeout: 2}}},
		{Matcher: &targetMatcher, Hooks: []HookEntry{{Type: "command", Command: managed, Timeout: 3}}},
	}

	got := reconcileHook(groups, updated, &targetMatcher)
	want := []MatcherGroup{
		{Matcher: &firstMatcher, Hooks: []HookEntry{user}},
		{Matcher: &targetMatcher, Hooks: []HookEntry{updated}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("reconcileHook()\nwant: %#v\n got: %#v", want, got)
	}
}

func TestReconcileHookDropsGroupsEmptiedByMove(t *testing.T) {
	oldMatcher := "old"
	newMatcher := "new"
	managed := "ao hooks test session-start"
	updated := HookEntry{Type: "command", Command: managed, Timeout: 30}
	groups := []MatcherGroup{
		{Matcher: &oldMatcher, Hooks: []HookEntry{{Type: "command", Command: managed, Timeout: 5}}},
	}

	got := reconcileHook(groups, updated, &newMatcher)
	want := []MatcherGroup{
		{Matcher: &newMatcher, Hooks: []HookEntry{updated}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("reconcileHook()\nwant: %#v\n got: %#v", want, got)
	}
}

func TestManagerPreservesUnknownUserHookFields(t *testing.T) {
	workspace := t.TempDir()
	hooksPath := filepath.Join(workspace, "hooks.json")
	original := []byte(`{
  "customTop": {"keep": true},
  "hooks": {
    "Notification": [{
      "matcher": "permission",
      "groupExtension": {"version": 2},
      "hooks": [{
        "type": "command",
        "command": "user hook",
        "timeout": 7,
        "async": true,
        "commandWindows": "user-hook.cmd",
        "command_windows": "legacy-user-hook.cmd",
        "future": {"enabled": true}
      }]
    }]
  }
}`)
	if err := os.WriteFile(hooksPath, original, 0o600); err != nil {
		t.Fatal(err)
	}

	manager := Manager{
		Label:         "test",
		CommandPrefix: "ao hooks test ",
		Timeout:       30,
		Path:          func(string) string { return hooksPath },
		Managed: []HookSpec{
			{Event: "Notification", Command: "ao hooks test notification"},
		},
	}
	if err := manager.Install(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	assertUnknownUserHookFields(t, hooksPath)

	if err := manager.Uninstall(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	assertUnknownUserHookFields(t, hooksPath)
}

func TestManagerUninstallPreservesSiblingLegacyPrefix(t *testing.T) {
	workspace := t.TempDir()
	hooksPath := filepath.Join(workspace, "hooks.json")
	original := []byte(`{"hooks":{"Stop":[{"hooks":[
{"type":"command","command":"ao hooks current stop"},
{"type":"command","command":"ao hooks sibling stop"},
{"type":"command","command":"ao hooks current-legacy stop"}
]}]}}`)
	if err := os.WriteFile(hooksPath, original, 0o600); err != nil {
		t.Fatal(err)
	}

	manager := Manager{
		Label:                    "current",
		CommandPrefix:            "ao hooks current ",
		LegacyCommandPrefixes:    []string{"ao hooks sibling "},
		UninstallCommandPrefixes: []string{"ao hooks current-legacy "},
		Path:                     func(string) string { return hooksPath },
		Managed:                  []HookSpec{{Event: "Stop", Command: "ao hooks current stop"}},
	}
	if err := manager.Uninstall(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(hooksPath)
	if err != nil {
		t.Fatal(err)
	}
	body := string(data)
	if strings.Contains(body, "ao hooks current stop") || strings.Contains(body, "ao hooks current-legacy stop") {
		t.Fatalf("current hooks survived uninstall: %s", body)
	}
	if !strings.Contains(body, "ao hooks sibling stop") {
		t.Fatalf("sibling hook was removed: %s", body)
	}
	if installed, err := manager.AreInstalled(t.Context(), workspace); err != nil || installed {
		t.Fatalf("AreInstalled after uninstall = (%v, %v), want (false, nil)", installed, err)
	}
}

func TestManagerPreservesManagedHookExtensionsOnReconcile(t *testing.T) {
	workspace := t.TempDir()
	hooksPath := filepath.Join(workspace, "hooks.json")
	original := []byte(`{
  "hooks": {
    "Notification": [{
      "groupExtension": "keep",
      "hooks": [{
        "type": "command",
        "command": "ao hooks test notification",
        "timeout": 7,
        "async": true,
        "commandWindows": "ao-hooks.cmd"
      }]
    }]
  }
}`)
	if err := os.WriteFile(hooksPath, original, 0o600); err != nil {
		t.Fatal(err)
	}

	manager := Manager{
		Label:         "test",
		CommandPrefix: "ao hooks test ",
		Timeout:       30,
		Path:          func(string) string { return hooksPath },
		Managed: []HookSpec{
			{Event: "Notification", Command: "ao hooks test notification"},
		},
	}
	if err := manager.Install(t.Context(), workspace); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(hooksPath)
	if err != nil {
		t.Fatal(err)
	}
	var settings map[string]any
	if err := json.Unmarshal(data, &settings); err != nil {
		t.Fatal(err)
	}
	group := settings["hooks"].(map[string]any)["Notification"].([]any)[0].(map[string]any)
	if group["groupExtension"] != "keep" {
		t.Fatalf("group extension changed: %#v", group["groupExtension"])
	}
	hook := group["hooks"].([]any)[0].(map[string]any)
	if hook["timeout"] != float64(30) || hook["async"] != true || hook["commandWindows"] != "ao-hooks.cmd" {
		t.Fatalf("managed hook fields = %#v", hook)
	}
}

func assertUnknownUserHookFields(t *testing.T, hooksPath string) {
	t.Helper()
	data, err := os.ReadFile(hooksPath)
	if err != nil {
		t.Fatal(err)
	}
	var settings map[string]any
	if err := json.Unmarshal(data, &settings); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(settings["customTop"], map[string]any{"keep": true}) {
		t.Fatalf("top-level extension changed: %#v", settings["customTop"])
	}
	hooks := settings["hooks"].(map[string]any)
	groups := hooks["Notification"].([]any)
	userGroup := groups[0].(map[string]any)
	if !reflect.DeepEqual(userGroup["groupExtension"], map[string]any{"version": float64(2)}) {
		t.Fatalf("matcher-group extension changed: %#v", userGroup["groupExtension"])
	}
	userHook := userGroup["hooks"].([]any)[0].(map[string]any)
	want := map[string]any{
		"type":            "command",
		"command":         "user hook",
		"timeout":         float64(7),
		"async":           true,
		"commandWindows":  "user-hook.cmd",
		"command_windows": "legacy-user-hook.cmd",
		"future":          map[string]any{"enabled": true},
	}
	if !reflect.DeepEqual(userHook, want) {
		t.Fatalf("user hook fields changed\nwant: %#v\n got: %#v", want, userHook)
	}
}
