//go:build windows

package persistenthost

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
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
				if err := os.WriteFile(path, []byte("old content"), 0o600); err != nil {
					t.Fatal(err)
				}
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
	// Exercise the current user's effective read/write access through a new
	// handle, rather than inferring authorization from a single allow ACE.
	reopened, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("current user journal read/write access: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
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
}

// An existing journal whose ACL forbids WRITE_DAC must not be returned or
// truncated. OWNER RIGHTS suppresses the owner's implicit WRITE_DAC grant.
func TestACPPromptJournalWindowsSecurityFailureReturnsNoJournal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "unsecurable.journal")
	const original = "existing private data"
	if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
		t.Fatal(err)
	}
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
