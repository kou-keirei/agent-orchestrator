package codexproto

import (
	"strings"
	"testing"
)

func TestVersionInventories(t *testing.T) {
	for version, count := range map[string]int{"codex-cli 0.159.3": 198, "codex-cli 0.163.0": 201} {
		t.Run(version, func(t *testing.T) {
			methods, err := MethodInventory(version)
			if err != nil || len(methods) != count {
				t.Fatalf("inventory count=%d err=%v", len(methods), err)
			}
			if err := ValidateMethodInventory(version, methods); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{MethodInitialize, MethodThreadStart, MethodThreadResume, MethodThreadRead, MethodTurnStart} {
				if methods[name] != "ClientRequest" {
					t.Errorf("required request %s direction=%q", name, methods[name])
				}
			}
			if methods[MethodInitialized] != "ClientNotification" {
				t.Fatal("initialized notification absent")
			}
			for _, name := range []string{"thread/attachmentOwner/list", "thread/prediction/updated", "thread/readState/changed"} {
				_, exists := methods[name]
				if exists != (version == "codex-cli 0.163.0") {
					t.Errorf("%s declaration in %s = %v", name, version, exists)
				}
			}
		})
	}
}

func TestGeneratedVersionInventoryMatchesAllGeneratedMethods(t *testing.T) {
	methods := make(map[string]string)
	for _, method := range Methods {
		if _, duplicate := methods[method.Name]; duplicate {
			t.Fatalf("duplicate generated method %s", method.Name)
		}
		methods[method.Name] = method.Direction
	}
	if err := ValidateMethodInventory(ProviderVersion, methods); err != nil {
		t.Fatal(err)
	}
}

func TestVersionInventoriesRejectDrift(t *testing.T) {
	for _, version := range []string{"codex-cli 0.159.3", "codex-cli 0.163.0"} {
		for _, change := range []string{"resume absent", "history absent", "send absent", "unrelated absent", "extra", "direction", "other version"} {
			t.Run(version+"/"+change, func(t *testing.T) {
				methods, err := MethodInventory(version)
				if err != nil {
					t.Fatal(err)
				}
				switch change {
				case "resume absent":
					delete(methods, MethodThreadResume)
				case "history absent":
					delete(methods, MethodThreadRead)
				case "send absent":
					delete(methods, MethodTurnStart)
				case "unrelated absent":
					delete(methods, MethodModelList)
				case "extra":
					methods["thread/unknown"] = "ClientRequest"
				case "direction":
					methods[MethodThreadResume] = "ServerNotification"
				case "other version":
					other := "codex-cli 0.159.3"
					if version == other {
						other = "codex-cli 0.163.0"
					}
					methods, err = MethodInventory(other)
					if err != nil {
						t.Fatal(err)
					}
				}
				if err := ValidateMethodInventory(version, methods); err == nil || !strings.Contains(err.Error(), "mismatch") {
					t.Fatalf("drift admitted: %v", err)
				}
			})
		}
	}
}

func TestVersionSelectionFailsClosed(t *testing.T) {
	for _, version := range []string{"", "unknown", "codex-cli 0.159.4", "codex-cli 0.163.1", "codex-cli 0.164.0", "0.159.3", "codex-cli 0.159.3-dev"} {
		if err := ValidateMethodInventory(version, nil); err == nil || !strings.Contains(err.Error(), "unsupported") {
			t.Errorf("version %q admitted: %v", version, err)
		}
	}
}
