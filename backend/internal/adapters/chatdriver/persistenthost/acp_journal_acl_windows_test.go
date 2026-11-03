//go:build windows

package persistenthost

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Windows mode bits do not describe confidentiality. Both new journals and
// existing journals with inherited Everyone read access must become private.
func TestACPPromptJournalWindowsDACLConfidentiality(t *testing.T) {
	for _, existing := range []bool{false, true} {
		name := "new"
		if existing {
			name = "existing"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			user, err := windows.GetCurrentProcessToken().GetTokenUser()
			if err != nil {
				t.Fatal(err)
			}
			// Deliberately broad parent ACL, confined to this disposable fixture.
			setJournalFixtureDACL(t, dir, "D:P(A;OICI;FA;;;"+user.User.Sid.String()+")(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)(A;OICI;FR;;;WD)")
			path := filepath.Join(dir, "prompt.journal")
			if existing {
				writeSameOwnerJournalFixture(t, path, "old content")
				sd, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("insecure fixture before open: %s", sd.String())
				dacl, _, err := sd.DACL()
				if err != nil || dacl == nil {
					t.Fatalf("fixture DACL: %v", err)
				}
				world, err := windows.CreateWellKnownSid(windows.WinWorldSid)
				if err != nil {
					t.Fatal(err)
				}
				broadRead := false
				for i := uint32(0); i < uint32(dacl.AceCount); i++ {
					var ace *windows.ACCESS_ALLOWED_ACE
					if err := windows.GetAce(dacl, i, &ace); err != nil {
						t.Fatal(err)
					}
					sid := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
					if ace.Header.AceType == windows.ACCESS_ALLOWED_ACE_TYPE &&
						ace.Header.AceFlags&windows.INHERITED_ACE != 0 &&
						uint32(ace.Mask)&windows.FILE_READ_DATA != 0 && sid.Equals(world) {
						broadRead = true
					}
				}
				if !broadRead {
					t.Fatal("fixture did not inherit Everyone data-read access")
				}
			}
			testPrivateWindowsJournal(t, path)
		})
	}
}

func testPrivateWindowsJournal(t *testing.T, path string) {
	t.Helper()
	journal, err := openACPPromptJournal(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = journal.close(context.Background()) })
	sd, err := windows.GetSecurityInfo(windows.Handle(journal.file.Fd()), windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		t.Fatalf("journal security descriptor: %v", err)
	}
	t.Logf("journal security descriptor: %s", sd.String())
	control, _, err := sd.Control()
	if err != nil || control&windows.SE_DACL_PROTECTED == 0 {
		t.Fatalf("journal DACL must be protected: control=%#x, err=%v", control, err)
	}
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		t.Fatal(err)
	}
	owner, _, err := sd.Owner()
	if err != nil || owner == nil || !owner.Equals(user.User.Sid) {
		t.Fatalf("journal owner must be current user: owner=%v, err=%v", owner, err)
	}
	system, err := windows.CreateWellKnownSid(windows.WinLocalSystemSid)
	if err != nil {
		t.Fatal(err)
	}
	administrators, err := windows.CreateWellKnownSid(windows.WinBuiltinAdministratorsSid)
	if err != nil {
		t.Fatal(err)
	}
	dacl, _, err := sd.DACL()
	if err != nil || dacl == nil {
		t.Fatalf("journal must have a non-null DACL: %v", err)
	}
	if dacl.AceCount != 3 {
		t.Fatalf("journal DACL has %d ACEs, want owner, SYSTEM and Administrators only", dacl.AceCount)
	}
	const sensitiveAccess = windows.FILE_READ_DATA | windows.FILE_WRITE_DATA | windows.FILE_APPEND_DATA |
		windows.GENERIC_READ | windows.GENERIC_WRITE | windows.GENERIC_ALL | windows.WRITE_DAC | windows.WRITE_OWNER
	for i := uint32(0); i < uint32(dacl.AceCount); i++ {
		var ace *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(dacl, i, &ace); err != nil || ace == nil {
			t.Fatalf("journal DACL ACE %d: %v", i, err)
		}
		if ace.Header.AceFlags&(windows.INHERIT_ONLY_ACE|windows.INHERITED_ACE) != 0 {
			t.Fatalf("journal ACE must be explicit and effective: flags=%#x", ace.Header.AceFlags)
		}
		if ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE {
			t.Fatalf("journal DACL has unsupported ACE type %d", ace.Header.AceType)
		}
		sid := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
		account, domain, _, lookupErr := sid.LookupAccount("")
		t.Logf("journal allow ACE: sid=%s account=%s\\%s mask=%#x lookupErr=%v",
			sid.String(), domain, account, ace.Mask, lookupErr)
		if uint32(ace.Mask)&sensitiveAccess != 0 && !sid.Equals(user.User.Sid) && !sid.Equals(system) && !sid.Equals(administrators) {
			t.Errorf("SECURITY_SOURCE_GAP: journal grants sensitive access to unintended principal %s (%s\\%s), mask=%#x",
				sid.String(), domain, account, ace.Mask)
		}
	}
	// Even an authorized owner's concurrent reader must be excluded while new
	// provider frames can be written through the secured journal handle.
	for _, flags := range []int{os.O_RDONLY, os.O_WRONLY, os.O_RDWR} {
		reopened, err := os.OpenFile(path, flags, 0)
		if reopened != nil {
			_ = reopened.Close()
			t.Fatalf("concurrent open with flags %d unexpectedly succeeded", flags)
		}
		if !errors.Is(err, windows.ERROR_SHARING_VIOLATION) {
			t.Fatalf("concurrent open with flags %d: %v, want native sharing violation", flags, err)
		}
	}
	info, err := journal.file.Stat()
	if err != nil || info.Size() != 0 {
		t.Fatalf("secured journal was not truncated: info=%v, err=%v", info, err)
	}
	span, err := journal.append(context.Background(), []byte("private frame\n"))
	if err != nil {
		t.Fatal(err)
	}
	var replay bytes.Buffer
	if err := journal.replayTo(context.Background(), &replay, span); err != nil || replay.String() != "private frame\n" {
		t.Fatalf("private replay = %q, err=%v", replay.String(), err)
	}
	if err := journal.reset(context.Background()); err != nil {
		t.Fatal(err)
	}
	journal.size = maxACPJournalBytes
	if _, err := journal.append(context.Background(), []byte("overflow")); !errors.Is(err, errACPJournalFull) {
		t.Fatalf("private journal quota: %v", err)
	}
	// Once the secured handle closes, the owner can open the existing file
	// normally. Cleanup still removes it through the journal's usual lifecycle.
	if err := journal.file.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("owner reopen after secured handle closes: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestACPPromptJournalWindowsRetainedReaderBlocksOpen(t *testing.T) {
	dir := t.TempDir()
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		t.Fatal(err)
	}
	setJournalFixtureDACL(t, dir, "D:P(A;OICI;FA;;;"+user.User.Sid.String()+")(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)(A;OICI;FR;;;WD)")
	path := filepath.Join(dir, "retained-reader.journal")
	const original = "old journal frames"
	writeSameOwnerJournalFixture(t, path, original)
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		t.Fatal(err)
	}
	// This read handle permits the old implementation's read/write open. Its
	// granted data-read rights would survive replacing the file's inherited ACL.
	handle, err := windows.CreateFile(name, windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE, nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		t.Fatal(err)
	}
	reader := os.NewFile(uintptr(handle), path)
	defer reader.Close()
	journal, err := openACPPromptJournal(context.Background(), path)
	if journal != nil {
		_ = journal.file.Close()
		t.Fatal("journal open succeeded while an old reader retained access")
	}
	if !errors.Is(err, windows.ERROR_SHARING_VIOLATION) {
		t.Fatalf("retained reader open: %v, want native sharing violation", err)
	}
	got := make([]byte, len(original))
	if n, err := reader.ReadAt(got, 0); err != nil || n != len(original) || string(got) != original {
		t.Fatalf("rejected open changed previous frames: %q, n=%d, err=%v", got, n, err)
	}
	info, err := reader.Stat()
	if err != nil || info.Size() != int64(len(original)) {
		t.Fatalf("rejected open changed journal size: info=%v, err=%v", info, err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	// Only after the retained reader is gone may AO secure/truncate and append.
	// Reuse all protected-DACL, sharing, replay/reset/quota and owner checks.
	testPrivateWindowsJournal(t, path)
}

// An existing journal whose ACL forbids WRITE_DAC must not be returned or
// truncated. OWNER RIGHTS suppresses the owner's implicit WRITE_DAC grant.
func TestACPPromptJournalWindowsSecurityFailureReturnsNoJournal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "unsecurable.journal")
	const original = "existing private data"
	writeSameOwnerJournalFixture(t, path, original)
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		t.Fatal(err)
	}
	setJournalFixtureDACL(t, path, "D:P(A;;FRFW;;;OW)(A;;FRFW;;;"+user.User.Sid.String()+")")
	journal, err := openACPPromptJournal(context.Background(), path)
	if journal != nil {
		_ = journal.file.Close()
		t.Fatal("security failure returned a journal")
	}
	if !errors.Is(err, windows.ERROR_ACCESS_DENIED) {
		t.Fatalf("security failure = %v, want native access denied", err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != original {
		t.Fatalf("security failure changed original data: %q, err=%v", got, err)
	}
	t.Log("native ACL prevents securing journal; no journal returned and original bytes preserved")
}

// os.WriteFile uses the token's default owner, which need not be TOKEN_USER
// (for example, an elevated token may default to Administrators). These fixtures
// exercise securing a same-user existing journal, not accepting group owners.
func writeSameOwnerJournalFixture(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		t.Fatal(err)
	}
	sd, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.OWNER_SECURITY_INFORMATION)
	if err != nil {
		t.Fatal(err)
	}
	owner, _, err := sd.Owner()
	if err != nil || owner == nil {
		t.Fatalf("fixture owner: owner=%v, err=%v", owner, err)
	}
	t.Logf("existing fixture before owner setup: TOKEN_USER=%s OWNER=%s", user.User.Sid.String(), owner.String())
	if !owner.Equals(user.User.Sid) {
		// Keep the real default-owner mismatch as a negative case before
		// assigning the owner required by the positive fixture's contract.
		journal, err := openACPPromptJournal(context.Background(), path)
		if journal != nil {
			_ = journal.file.Close()
			t.Fatal("foreign-owner fixture returned a journal")
		}
		if err == nil || !strings.Contains(err.Error(), "owner is not current user") {
			t.Fatalf("foreign-owner fixture: %v, want owner rejection", err)
		}
		got, err := os.ReadFile(path)
		if err != nil || string(got) != content {
			t.Fatalf("owner rejection changed original data: %q, err=%v", got, err)
		}
	}
	// Set only the owner; preserve the inherited broad DACL used by the tests.
	if err := windows.SetNamedSecurityInfo(path, windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION, user.User.Sid, nil, nil, nil); err != nil {
		t.Fatalf("set same-user fixture owner: %v", err)
	}
	sd, err = windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.OWNER_SECURITY_INFORMATION)
	if err != nil {
		t.Fatal(err)
	}
	owner, _, err = sd.Owner()
	if err != nil || owner == nil || !owner.Equals(user.User.Sid) {
		t.Fatalf("fixture owner must be TOKEN_USER: owner=%v, err=%v", owner, err)
	}
	t.Logf("existing fixture after owner setup: TOKEN_USER=%s OWNER=%s", user.User.Sid.String(), owner.String())
}

func setJournalFixtureDACL(t *testing.T, path, sddl string) {
	t.Helper()
	sd, err := windows.SecurityDescriptorFromString(sddl)
	if err != nil {
		t.Fatal(err)
	}
	dacl, _, err := sd.DACL()
	if err != nil {
		t.Fatal(err)
	}
	if err := windows.SetNamedSecurityInfo(path, windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION, nil, nil, dacl, nil); err != nil {
		t.Fatal(err)
	}
}
