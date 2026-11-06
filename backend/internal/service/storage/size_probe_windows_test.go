//go:build windows

package storage

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf16"

	"golang.org/x/sys/windows"
)

// These are SOURCE_GAP canaries, not qualification of a Windows walker. The
// native source must decline before opening even a fully attested fixture:
// OwnerUID has no trusted Windows SID mapping. Generic injected-node tests
// separately cover budgets, mid-observation mutation and logical sparse length.
func windowsGapRequest(t *testing.T) SizeProbeRequest {
	t.Helper()
	r := probeRequest(t)
	if err := os.Mkdir(r.Root.Path, 0700); err != nil {
		t.Fatal(err)
	}
	return r
}

func requireWindowsGap(t *testing.T, r SizeProbeRequest) {
	t.Helper()
	result, err := ProbeSize(context.Background(), r)
	requireProbe(t, result, err, ProbeUnsupported, nil)
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	encodedPath, err := json.Marshal(r.Root.Path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), string(encodedPath)) ||
		!strings.Contains(string(encoded), `"logicalBytes":null`) ||
		!strings.Contains(string(encoded), `"physicalBytes":null`) {
		t.Fatalf("path or fabricated measurement in evidence: %s", encoded)
	}
}

func TestSizeProbeWindowsOwnerUIDCannotAuthorizeSID(t *testing.T) {
	r := windowsGapRequest(t)
	for _, owner := range []uint32{0, 42, 1000, ^uint32(0)} {
		t.Run(fmt.Sprint(owner), func(t *testing.T) {
			r.OwnerUID = &owner
			requireWindowsGap(t, r)
		})
	}
	// Missing and existing roots produce the same native capability failure;
	// no target metadata may be used to invent an owner mapping.
	r.Root.Path = filepath.Join(r.Root.Path, "missing")
	if node, err := openSizeRoot(r.Root.Path); node != nil || probeErrorCode(err) != ProbeUnsupported {
		t.Fatalf("native source unexpectedly opened a root: %v, %v", node, err)
	}
	requireWindowsGap(t, r)
}

func TestSizeProbeWindowsGapWithThousandFilesAndSparseLength(t *testing.T) {
	r := windowsGapRequest(t)
	for i := 0; i < 1001; i++ {
		if err := os.WriteFile(filepath.Join(r.Root.Path, fmt.Sprintf("file-%04d", i)), []byte("ao"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	f, err := os.Create(filepath.Join(r.Root.Path, "sparse"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var returned uint32
	if err := windows.DeviceIoControl(windows.Handle(f.Fd()), windows.FSCTL_SET_SPARSE, nil, 0, nil, 0, &returned, nil); err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(1 << 20); err != nil {
		t.Fatal(err)
	}
	info, err := f.Stat()
	if err != nil || info.Size() != 1<<20 {
		t.Fatalf("sparse fixture length: %v, %v", info, err)
	}
	requireWindowsGap(t, r)
	// Even an otherwise valid budget too small for this tree must not cause
	// partial enumeration or a known zero before the owner/source gate.
	r.Limits.MaxEntries, r.Limits.MaxDepth, r.Limits.MaxLogicalBytes = 2, 1, 1
	requireWindowsGap(t, r)
}

func TestSizeProbeWindowsGapWithHardlinkAndAliases(t *testing.T) {
	r := windowsGapRequest(t)
	path := filepath.Join(r.Root.Path, "long-file-name")
	if err := os.WriteFile(path, []byte("retained"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(path, filepath.Join(r.Root.Path, "hardlink")); err != nil {
		t.Fatal(err)
	}
	for _, alias := range []struct {
		name string
		code ProbeCode
	}{
		{"LONG-FILE-NAME", ProbeUnsupported},
		{"LONGFI~1", ProbeUnsupported},
		{"e\u0301", ProbeInvalid},
		{"\u00e9", ProbeInvalid},
	} {
		// A spelling is not evidence of a distinct file, canonical Unicode or
		// an actual 8.3 alias. No such path may yield measured bytes here.
		aliased := r
		aliased.Root.Path = filepath.Join(r.Root.Path, alias.name)
		result, err := ProbeSize(context.Background(), aliased)
		requireProbe(t, result, err, alias.code, nil)
	}
	requireWindowsGap(t, r)
	r.Root.Path += string(filepath.Separator) + string([]byte{0xff})
	result, err := ProbeSize(context.Background(), r)
	requireProbe(t, result, err, ProbeInvalid, nil)
}

func TestSizeProbeWindowsGapWithReparsePoints(t *testing.T) {
	for _, kind := range []string{"child-junction", "root-junction", "ancestor-junction", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			r := windowsGapRequest(t)
			target := t.TempDir()
			if err := os.WriteFile(filepath.Join(target, "retained"), []byte("ao"), 0600); err != nil {
				t.Fatal(err)
			}
			link := filepath.Join(r.Root.Path, "link")
			if kind == "symlink" {
				if err := os.Symlink(target, link); err != nil {
					if errors.Is(err, windows.ERROR_PRIVILEGE_NOT_HELD) {
						t.Skipf("symlink privilege unavailable: %v", err)
					}
					t.Fatal(err)
				}
			} else {
				makeSizeProbeJunction(t, link, target)
			}
			info, err := os.Lstat(link)
			if err != nil || info.Mode()&(os.ModeSymlink|os.ModeIrregular) == 0 {
				t.Fatalf("fixture is not a reparse link: %v, %v", info, err)
			}
			switch kind {
			case "root-junction":
				r.Root.Path = link
			case "ancestor-junction":
				r.Root.Path = filepath.Join(link, "retained")
			}
			requireWindowsGap(t, r)
			data, err := os.ReadFile(filepath.Join(target, "retained"))
			if err != nil || string(data) != "ao" {
				t.Fatalf("reparse target changed: %q, %v", data, err)
			}
		})
	}
}

// Create a mount-point reparse fixture entirely inside test-owned temp dirs.
// Remove the junction itself before TempDir cleanup; never recurse through it.
func makeSizeProbeJunction(t *testing.T, link, target string) {
	t.Helper()
	if err := os.Mkdir(link, 0700); err != nil {
		t.Fatal(err)
	}
	name, err := windows.UTF16PtrFromString(link)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := windows.CreateFile(name, windows.GENERIC_WRITE, 0, nil, windows.OPEN_EXISTING,
		windows.FILE_FLAG_OPEN_REPARSE_POINT|windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer windows.CloseHandle(handle)
	substitute := utf16.Encode([]rune(`\??\` + target))
	printName := utf16.Encode([]rune(target))
	pathUnits := append(append(append(substitute, 0), printName...), 0)
	// REPARSE_DATA_BUFFER: 8-byte header, 8-byte mount-point name offsets,
	// then UTF-16 names. Offsets are relative to the name buffer, in bytes.
	buffer := make([]byte, 16+2*len(pathUnits))
	binary.LittleEndian.PutUint32(buffer, windows.IO_REPARSE_TAG_MOUNT_POINT)
	binary.LittleEndian.PutUint16(buffer[4:], uint16(len(buffer)-8))
	binary.LittleEndian.PutUint16(buffer[10:], uint16(2*len(substitute)))
	binary.LittleEndian.PutUint16(buffer[12:], uint16(2*(len(substitute)+1)))
	binary.LittleEndian.PutUint16(buffer[14:], uint16(2*len(printName)))
	for i, unit := range pathUnits {
		binary.LittleEndian.PutUint16(buffer[16+2*i:], unit)
	}
	var returned uint32
	if err := windows.DeviceIoControl(handle, windows.FSCTL_SET_REPARSE_POINT, &buffer[0], uint32(len(buffer)), nil, 0, &returned, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Remove(link); err != nil {
			t.Errorf("remove fixture junction: %v", err)
		}
	})
}

func TestSizeProbeWindowsGapAfterNamespaceSwapAndCancellation(t *testing.T) {
	r := windowsGapRequest(t)
	requireWindowsGap(t, r)
	if err := os.Rename(r.Root.Path, r.Root.Path+"-old"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(r.Root.Path, 0700); err != nil {
		t.Fatal(err)
	}
	// No native observation starts, so replacing a root between requests
	// cannot authorize logical bytes either. This is not mutation detection.
	requireWindowsGap(t, r)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := ProbeSize(ctx, r)
	requireProbe(t, result, err, ProbeCanceled, nil)
}
