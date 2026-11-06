//go:build windows

package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Tokens are used ONLY to construct a disposable fixture with an explicit owner.
// The production validator never uses a process token or derives an issuer from
// target metadata. This injected fixture issuer is not a production AO issuer.
func windowsOwnerFixture(t *testing.T) (WindowsSizeOwnerClaim, *os.File) {
	t.Helper()
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		t.Fatal(err)
	}
	claim := WindowsSizeOwnerClaim{Scope: WindowsSizeOwnerScope{SessionID: "fixture-session", ProjectID: "fixture-project",
		Generation: 7, OwnerID: "workspace:fixture", Root: MetadataRoot{Path: filepath.Join(windowsOwnerTempDir(t), "owned"),
			Trusted: Yes, Canonical: Yes, Quiescent: Yes, NoReparseOrSymlinks: Yes, AliasUnambiguous: Yes}},
		OwnerSID: WindowsOwnerSID(user.User.Sid.String())}
	file, err := createWindowsOwnerFixture(claim.Scope.Root.Path, claim.OwnerSID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	return claim, file
}

// Fixture setup only: Windows TEMP may itself use an 8.3 alias. Resolve the
// disposable directory's long spelling before issuing the fixture claim; the
// validator must continue to reject unqualified short spellings.
func windowsOwnerTempDir(t *testing.T) string {
	t.Helper()
	name, err := windows.UTF16PtrFromString(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	buffer := make([]uint16, 4097)
	n, err := windows.GetLongPathName(name, &buffer[0], uint32(len(buffer)))
	if err != nil || n == 0 || n >= uint32(len(buffer)) {
		t.Fatalf("fixture long spelling: %d, %v", n, err)
	}
	return windows.UTF16ToString(buffer[:n])
}

func differentWindowsFixtureSID(sid WindowsOwnerSID) WindowsOwnerSID {
	if sid == "S-1-5-18" {
		return "S-1-5-19"
	}
	return "S-1-5-18"
}

func createWindowsOwnerFixture(path string, owner WindowsOwnerSID) (*os.File, error) {
	sd, err := windows.SecurityDescriptorFromString("O:" + string(owner) + "D:P(A;;FA;;;" + string(owner) + ")(A;;FA;;;SY)(A;;FA;;;BA)")
	if err != nil {
		return nil, err
	}
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	attributes := windows.SecurityAttributes{Length: uint32(unsafe.Sizeof(windows.SecurityAttributes{})), SecurityDescriptor: sd}
	handle, err := windows.CreateFile(name, windows.GENERIC_READ|windows.GENERIC_WRITE|windows.READ_CONTROL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, &attributes, windows.CREATE_NEW,
		windows.FILE_FLAG_OPEN_REPARSE_POINT|windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(handle), path)
	if _, err := file.WriteString("retained fixture bytes"); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func fixtureWindowsOwnerSource(claim WindowsSizeOwnerClaim) TrustedWindowsSizeOwnerSource {
	return func(context.Context, WindowsSizeOwnerScope) ([]WindowsSizeOwnerClaim, error) {
		return []WindowsSizeOwnerClaim{claim}, nil
	}
}

func requireWindowsOwnerGap(t *testing.T, got WindowsSizeOwnerEvidence, err error, code ProbeCode) {
	t.Helper()
	if !errors.Is(err, ErrSourceGap) || got.State != WindowsOwnerSourceGap || got.Diagnostic != code {
		t.Fatalf("expected typed owner HOLD (%s), got %+v, %v", code, got, err)
	}
}

func TestWindowsSizeOwnerNativeBinding(t *testing.T) {
	claim, file := windowsOwnerFixture(t)
	calls := 0
	source := func(_ context.Context, scope WindowsSizeOwnerScope) ([]WindowsSizeOwnerClaim, error) {
		calls++
		if scope != claim.Scope {
			t.Fatal("source queried for another owner")
		}
		return []WindowsSizeOwnerClaim{claim}, nil
	}
	got, err := ValidateWindowsSizeOwner(context.Background(), claim.Scope, source, file)
	if err != nil || got.State != WindowsOwnerBound || got.Diagnostic != "" || calls != 2 {
		t.Fatalf("fixture principal binding: %+v, %v, reads=%d", got, err, calls)
	}
	data, err := os.ReadFile(claim.Scope.Root.Path)
	if err != nil || string(data) != "retained fixture bytes" {
		t.Fatalf("fixture changed: %q, %v", data, err)
	}
	if _, err := file.Stat(); err != nil {
		t.Fatalf("caller handle closed: %v", err)
	}
	// This identity seam does not add a Windows traversal source.
	if node, err := openSizeRoot(claim.Scope.Root.Path); node != nil || probeErrorCode(err) != ProbeUnsupported {
		t.Fatalf("Windows size fallback changed: %v, %v", node, err)
	}
}

func TestWindowsSizeOwnerNativeForeignPrincipal(t *testing.T) {
	claim, file := windowsOwnerFixture(t)
	claim.OwnerSID = differentWindowsFixtureSID(claim.OwnerSID)
	got, err := ValidateWindowsSizeOwner(context.Background(), claim.Scope, fixtureWindowsOwnerSource(claim), file)
	requireWindowsOwnerGap(t, got, err, ProbeUnsafe)
}

func TestWindowsSizeOwnerNativeNonProcessOwner(t *testing.T) {
	claim, _ := windowsOwnerFixture(t)
	claim.Scope.Root.Path = filepath.Join(windowsOwnerTempDir(t), "group-owned")
	claim.OwnerSID = "S-1-5-32-544" // An enabled owner group can own a file independently of TokenUser.
	file, err := createWindowsOwnerFixture(claim.Scope.Root.Path, claim.OwnerSID)
	if errors.Is(err, windows.ERROR_INVALID_OWNER) || errors.Is(err, windows.ERROR_PRIVILEGE_NOT_HELD) || errors.Is(err, windows.ERROR_ACCESS_DENIED) {
		t.Skipf("fixture token cannot assign an alternate owner without extra privilege: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	got, err := ValidateWindowsSizeOwner(context.Background(), claim.Scope, fixtureWindowsOwnerSource(claim), file)
	if err != nil || got.State != WindowsOwnerBound {
		t.Fatalf("explicit non-process owner rejected: %+v, %v", got, err)
	}
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		t.Fatal(err)
	}
	claim.OwnerSID = WindowsOwnerSID(user.User.Sid.String())
	got, err = ValidateWindowsSizeOwner(context.Background(), claim.Scope, fixtureWindowsOwnerSource(claim), file)
	requireWindowsOwnerGap(t, got, err, ProbeUnsafe)
}

func TestWindowsSizeOwnerInjectedPrincipalIndependentOfProcessUser(t *testing.T) {
	claim, file := windowsOwnerFixture(t)
	processUser := claim.OwnerSID
	claim.OwnerSID = differentWindowsFixtureSID(processUser)
	// This is a synthetic native readback, explicitly separate from the real
	// alternate-owner fixture above. It never changes or impersonates a user.
	read := func(f *os.File) (windowsSizeOwnerObservation, error) {
		got, err := readWindowsSizeOwner(f)
		got.owner = claim.OwnerSID
		return got, err
	}
	got, err := validateWindowsSizeOwner(context.Background(), claim.Scope, fixtureWindowsOwnerSource(claim), file, read)
	if err != nil || got.State != WindowsOwnerBound {
		t.Fatalf("explicit injected non-process owner: %+v, %v", got, err)
	}
	wrong := claim
	wrong.OwnerSID = processUser
	got, err = validateWindowsSizeOwner(context.Background(), claim.Scope, fixtureWindowsOwnerSource(wrong), file, read)
	requireWindowsOwnerGap(t, got, err, ProbeUnsafe)
}

func TestWindowsSizeOwnerSIDNativeStrictParsing(t *testing.T) {
	for _, value := range []WindowsOwnerSID{"S-1-5-18", "S-1-5-32-544", "S-1-5-21-1-2-3-1001"} {
		if _, ok := parseWindowsOwnerSID(value); !ok {
			t.Errorf("canonical SID rejected: %q", value)
		}
	}
	for _, value := range []WindowsOwnerSID{"", "SY", "SYSTEM", "1001", "s-1-5-18", " S-1-5-18", "S-1-5-18 ",
		"S-1-5-18\x00ignored", "S-1-5-018", "S-1-05-18", "S-2-5-18", "S-1-5--18", "S-1-5-0x12", "S-1-5-１８",
		"S-1-5-4294967296", "S-1-281474976710656-1", WindowsOwnerSID("S-1-5" + strings.Repeat("-1", 16)),
		WindowsOwnerSID(strings.Repeat("1", 185))} {
		if _, ok := parseWindowsOwnerSID(value); ok {
			t.Errorf("invalid or noncanonical SID accepted: %q", value)
		}
	}
}

func TestWindowsSizeOwnerMissingAuthorityBeforeHandleIO(t *testing.T) {
	claim, file := windowsOwnerFixture(t)
	read := func(*os.File) (windowsSizeOwnerObservation, error) {
		t.Fatal("filesystem accessed without trusted issuer")
		return windowsSizeOwnerObservation{}, nil
	}
	got, err := validateWindowsSizeOwner(context.Background(), claim.Scope, nil, file, read)
	requireWindowsOwnerGap(t, got, err, ProbeUnsupported)
	for _, source := range []TrustedWindowsSizeOwnerSource{
		func(context.Context, WindowsSizeOwnerScope) ([]WindowsSizeOwnerClaim, error) { return nil, nil },
		func(context.Context, WindowsSizeOwnerScope) ([]WindowsSizeOwnerClaim, error) {
			return []WindowsSizeOwnerClaim{claim, claim}, nil
		},
		func(context.Context, WindowsSizeOwnerScope) ([]WindowsSizeOwnerClaim, error) {
			return nil, errors.New("unknown issuer: sensitive path")
		},
	} {
		got, err := validateWindowsSizeOwner(context.Background(), claim.Scope, source, file, read)
		requireWindowsOwnerGap(t, got, err, ProbeInvalid)
		if err.Error() != "SOURCE_GAP" {
			t.Fatal("issuer diagnostic leaked")
		}
	}
}

func TestWindowsSizeOwnerScopeAndClaimRejectionBeforeHandleIO(t *testing.T) {
	claim, file := windowsOwnerFixture(t)
	changes := []struct {
		name   string
		change func(*WindowsSizeOwnerScope)
	}{
		{"session", func(s *WindowsSizeOwnerScope) { s.SessionID = "other" }},
		{"project", func(s *WindowsSizeOwnerScope) { s.ProjectID = "other" }},
		{"generation", func(s *WindowsSizeOwnerScope) { s.Generation++ }},
		{"owner", func(s *WindowsSizeOwnerScope) { s.OwnerID = "other" }},
		{"root", func(s *WindowsSizeOwnerScope) { s.Root.Path += "-other" }},
	}
	for _, test := range changes {
		t.Run(test.name, func(t *testing.T) {
			other := claim
			test.change(&other.Scope)
			got, err := validateWindowsSizeOwner(context.Background(), claim.Scope, fixtureWindowsOwnerSource(other), file,
				func(*os.File) (windowsSizeOwnerObservation, error) {
					t.Fatal("mismatched claim reached handle")
					return windowsSizeOwnerObservation{}, nil
				})
			requireWindowsOwnerGap(t, got, err, ProbeInvalid)
		})
	}
	for _, test := range []struct {
		name   string
		change func(*WindowsSizeOwnerScope)
	}{
		{"missing-session", func(s *WindowsSizeOwnerScope) { s.SessionID = "" }},
		{"missing-project", func(s *WindowsSizeOwnerScope) { s.ProjectID = "" }},
		{"missing-owner", func(s *WindowsSizeOwnerScope) { s.OwnerID = "" }},
		{"missing-generation", func(s *WindowsSizeOwnerScope) { s.Generation = 0 }},
		{"negative-generation", func(s *WindowsSizeOwnerScope) { s.Generation = -1 }},
		{"untrusted", func(s *WindowsSizeOwnerScope) { s.Root.Trusted = Unknown }},
		{"not-canonical", func(s *WindowsSizeOwnerScope) { s.Root.Canonical = Unknown }},
		{"live", func(s *WindowsSizeOwnerScope) { s.Root.Quiescent = No }},
		{"links-or-mounts", func(s *WindowsSizeOwnerScope) { s.Root.NoReparseOrSymlinks = Unknown }},
		{"aliases-unknown", func(s *WindowsSizeOwnerScope) { s.Root.AliasUnambiguous = Unknown }},
		{"unicode", func(s *WindowsSizeOwnerScope) { s.Root.Path += "é" }},
		{"8.3", func(s *WindowsSizeOwnerScope) { s.Root.Path += "~1" }},
		{"relative", func(s *WindowsSizeOwnerScope) { s.Root.Path = "relative" }},
		{"traversal", func(s *WindowsSizeOwnerScope) { s.Root.Path += `\..` }},
	} {
		t.Run(test.name, func(t *testing.T) {
			other := claim
			test.change(&other.Scope)
			got, err := ValidateWindowsSizeOwner(context.Background(), other.Scope,
				func(context.Context, WindowsSizeOwnerScope) ([]WindowsSizeOwnerClaim, error) {
					t.Fatal("invalid scope reached issuer")
					return nil, nil
				}, file)
			requireWindowsOwnerGap(t, got, err, ProbeInvalid)
		})
	}
	for _, sid := range []WindowsOwnerSID{"", "SY", "S-1-5-4294967296"} {
		other := claim
		other.OwnerSID = sid
		got, err := validateWindowsSizeOwner(context.Background(), claim.Scope, fixtureWindowsOwnerSource(other), file,
			func(*os.File) (windowsSizeOwnerObservation, error) {
				t.Fatal("invalid SID reached handle")
				return windowsSizeOwnerObservation{}, nil
			})
		requireWindowsOwnerGap(t, got, err, ProbeInvalid)
	}
}

func TestWindowsSizeOwnerReadbackChanges(t *testing.T) {
	claim, file := windowsOwnerFixture(t)
	for _, field := range []string{"session", "project", "generation", "owner", "root", "sid", "qualification", "missing", "ambiguous", "error"} {
		t.Run(field, func(t *testing.T) {
			claims := []WindowsSizeOwnerClaim{claim}
			calls := 0
			source := func(context.Context, WindowsSizeOwnerScope) ([]WindowsSizeOwnerClaim, error) {
				calls++
				if calls == 1 {
					return claims, nil
				}
				switch field {
				case "session":
					claims[0].Scope.SessionID = "other"
				case "project":
					claims[0].Scope.ProjectID = "other"
				case "generation":
					claims[0].Scope.Generation++
				case "owner":
					claims[0].Scope.OwnerID = "other"
				case "root":
					claims[0].Scope.Root.Path += "-other"
				case "sid":
					claims[0].OwnerSID = differentWindowsFixtureSID(claim.OwnerSID)
				case "qualification":
					claims[0].Scope.Root.AliasUnambiguous = Unknown
				case "missing":
					return nil, nil
				case "ambiguous":
					return append(claims, claim), nil
				case "error":
					return nil, errors.New("readback unavailable")
				}
				return claims, nil
			}
			got, err := ValidateWindowsSizeOwner(context.Background(), claim.Scope, source, file)
			requireWindowsOwnerGap(t, got, err, ProbeChanged)
		})
	}
	// Deterministic owner/identity/path changes on the held observation. No
	// privilege-dependent live owner mutation or timing race is needed.
	for _, field := range []string{"owner", "identity", "path", "attributes", "links"} {
		t.Run("native-"+field, func(t *testing.T) {
			calls := 0
			read := func(f *os.File) (windowsSizeOwnerObservation, error) {
				got, err := readWindowsSizeOwner(f)
				calls++
				if calls == 2 {
					switch field {
					case "owner":
						got.owner = differentWindowsFixtureSID(claim.OwnerSID)
					case "identity":
						got.indexLow++
					case "path":
						got.path += "-other"
					case "attributes":
						got.attributes ^= windows.FILE_ATTRIBUTE_READONLY
					case "links":
						got.links++
					}
				}
				return got, err
			}
			got, err := validateWindowsSizeOwner(context.Background(), claim.Scope, fixtureWindowsOwnerSource(claim), file, read)
			requireWindowsOwnerGap(t, got, err, ProbeChanged)
		})
	}
}

func TestWindowsSizeOwnerNativeHandleAndNamespaceHolds(t *testing.T) {
	claim, file := windowsOwnerFixture(t)
	_, otherFile := windowsOwnerFixture(t)
	got, err := ValidateWindowsSizeOwner(context.Background(), claim.Scope, fixtureWindowsOwnerSource(claim), otherFile)
	requireWindowsOwnerGap(t, got, err, ProbeUnsafe)
	alias := claim
	alias.Scope.Root.Path = strings.ToUpper(alias.Scope.Root.Path)
	got, err = ValidateWindowsSizeOwner(context.Background(), alias.Scope, fixtureWindowsOwnerSource(alias), file)
	requireWindowsOwnerGap(t, got, err, ProbeUnsafe)
	if err := os.Link(claim.Scope.Root.Path, claim.Scope.Root.Path+"-hardlink"); err != nil {
		t.Fatal(err)
	}
	got, err = ValidateWindowsSizeOwner(context.Background(), claim.Scope, fixtureWindowsOwnerSource(claim), file)
	requireWindowsOwnerGap(t, got, err, ProbeUnsafe)
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	got, err = ValidateWindowsSizeOwner(context.Background(), claim.Scope, fixtureWindowsOwnerSource(claim), file)
	requireWindowsOwnerGap(t, got, err, ProbeIO)
	got, err = ValidateWindowsSizeOwner(context.Background(), claim.Scope, fixtureWindowsOwnerSource(claim), nil)
	requireWindowsOwnerGap(t, got, err, ProbeInvalid)
}

func TestWindowsSizeOwnerNativeReparseHolds(t *testing.T) {
	claim, _ := windowsOwnerFixture(t)
	link := filepath.Join(windowsOwnerTempDir(t), "junction")
	makeSizeProbeJunction(t, link, t.TempDir())
	claim.Scope.Root.Path = link
	name, err := windows.UTF16PtrFromString(link)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := windows.CreateFile(name, windows.READ_CONTROL|windows.FILE_READ_ATTRIBUTES,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil, windows.OPEN_EXISTING,
		windows.FILE_FLAG_OPEN_REPARSE_POINT|windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		t.Fatal(err)
	}
	file := os.NewFile(uintptr(handle), link)
	defer file.Close()
	got, err := ValidateWindowsSizeOwner(context.Background(), claim.Scope, fixtureWindowsOwnerSource(claim), file)
	requireWindowsOwnerGap(t, got, err, ProbeUnsafe)
}

func TestWindowsSizeOwnerCancellation(t *testing.T) {
	claim, file := windowsOwnerFixture(t)
	for _, stage := range []string{"before", "issuer", "readback"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if stage == "before" {
				cancel()
			}
			calls := 0
			source := func(context.Context, WindowsSizeOwnerScope) ([]WindowsSizeOwnerClaim, error) {
				calls++
				if stage == "issuer" || (stage == "readback" && calls == 2) {
					cancel()
				}
				return []WindowsSizeOwnerClaim{claim}, nil
			}
			got, err := ValidateWindowsSizeOwner(ctx, claim.Scope, source, file)
			requireWindowsOwnerGap(t, got, err, ProbeCanceled)
		})
	}
}
