package codexproto

import (
	"embed"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// These inventories record names and directions only. They do not assert that
// reply payloads from different provider releases are interchangeable.
//
//go:embed inventories/*.json
var inventories embed.FS

// MethodInventory selects an exact provider release. Unknown releases must be
// exported and reviewed explicitly; neither nearest-version nor union fallback
// can establish what an installed provider actually declares.
func MethodInventory(version string) (map[string]string, error) {
	var path string
	switch version {
	case "codex-cli 0.159.3":
		path = "inventories/0.159.3.json"
	case "codex-cli 0.163.0":
		path = "inventories/0.163.0.json"
	default:
		return nil, fmt.Errorf("unsupported Codex protocol version %q", version)
	}
	raw, err := inventories.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var inventory struct {
		ProviderVersion string            `json:"providerVersion"`
		Methods         map[string]string `json:"methods"`
	}
	if err := json.Unmarshal(raw, &inventory); err != nil {
		return nil, err
	}
	if inventory.ProviderVersion != version || len(inventory.Methods) == 0 {
		return nil, fmt.Errorf("invalid inventory for %s", version)
	}
	for name, direction := range inventory.Methods {
		if name == "" {
			return nil, fmt.Errorf("empty method name in %s", version)
		}
		switch direction {
		case "ClientRequest", "ClientNotification", "ServerRequest", "ServerNotification":
		default:
			return nil, fmt.Errorf("invalid direction %q for %s in %s", direction, name, version)
		}
	}
	return inventory.Methods, nil
}

// ValidateMethodInventory requires exact equality, including directions. Extra
// methods are drift too: silently accepting them would defeat version selection.
func ValidateMethodInventory(version string, actual map[string]string) error {
	expected, err := MethodInventory(version)
	if err != nil {
		return err
	}
	var differences []string
	for name, direction := range expected {
		if got, ok := actual[name]; !ok {
			differences = append(differences, fmt.Sprintf("missing %s (%s)", name, direction))
		} else if got != direction {
			differences = append(differences, fmt.Sprintf("%s direction %q, want %q", name, got, direction))
		}
	}
	for name, direction := range actual {
		if _, ok := expected[name]; !ok {
			differences = append(differences, fmt.Sprintf("unexpected %s (%s)", name, direction))
		}
	}
	if len(differences) != 0 {
		sort.Strings(differences)
		return fmt.Errorf("%s method inventory mismatch: %s", version, strings.Join(differences, "; "))
	}
	return nil
}
