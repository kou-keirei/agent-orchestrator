//go:build linux

package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func nativeProbeRequest(t *testing.T) SizeProbeRequest {
	t.Helper()
	r := probeRequest(t)
	if err := os.Mkdir(r.Root.Path, 0700); err != nil {
		t.Fatal(err)
	}
	owner := uint32(os.Getuid())
	r.OwnerUID = &owner
	return r
}

func TestSizeProbeLinuxSparseThousandEntries(t *testing.T) {
	r := nativeProbeRequest(t)
	for i := 0; i < 1001; i++ {
		if err := os.WriteFile(filepath.Join(r.Root.Path, fmt.Sprintf("file-%04d", i)), []byte("ao"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(r.Root.Path, "sparse")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(1 << 20); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	result, err := ProbeSize(context.Background(), r)
	// Check native st_dev separately from fixture device 7.
	if result.DeviceID == nil {
		t.Fatalf("native device unknown: %+v, %v", result, err)
	}
	device := "linux-st_dev:7"
	result.DeviceID = &device
	logical := int64(1001*2 + 1<<20)
	requireProbe(t, result, err, ProbeExtents, &logical)
}

func TestSizeProbeLinuxRejectsLinksAndAliases(t *testing.T) {
	for _, kind := range []string{"symlink", "hardlink", "case", "unicode", "fifo", "root-link"} {
		t.Run(kind, func(t *testing.T) {
			r := nativeProbeRequest(t)
			path := filepath.Join(r.Root.Path, "file")
			if err := os.WriteFile(path, []byte("ao"), 0600); err != nil {
				t.Fatal(err)
			}
			var err error
			code := ProbeUnsafe
			switch kind {
			case "symlink":
				err = os.Symlink(t.TempDir(), filepath.Join(r.Root.Path, "link"))
			case "hardlink":
				err = os.Link(path, filepath.Join(r.Root.Path, "link"))
			case "case":
				err = os.WriteFile(filepath.Join(r.Root.Path, "FILE"), nil, 0600)
				code = ProbeAlias
			case "unicode":
				err = os.WriteFile(filepath.Join(r.Root.Path, "é"), nil, 0600)
				code = ProbeAlias
			case "fifo":
				err = unix.Mkfifo(filepath.Join(r.Root.Path, "fifo"), 0600)
			case "root-link":
				link := filepath.Join(filepath.Dir(r.Root.Path), "link")
				err = os.Symlink(r.Root.Path, link)
				r.Root.Path = link
			}
			if err != nil {
				t.Fatal(err)
			}
			result, err := ProbeSize(context.Background(), r)
			requireProbe(t, result, err, code, nil)
		})
	}
}

func TestSizeProbeLinuxDetectsHeldObjectReplacement(t *testing.T) {
	r := nativeProbeRequest(t)
	path := filepath.Join(r.Root.Path, "file")
	if err := os.WriteFile(path, []byte("ao"), 0600); err != nil {
		t.Fatal(err)
	}
	root, err := openSizeRoot(r.Root.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer root.close()
	child, err := root.open("file")
	if err != nil {
		t.Fatal(err)
	}
	defer child.close()
	if err := os.Rename(path, path+"-old"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("ao"), 0600); err != nil {
		t.Fatal(err)
	}
	if code := probeErrorCode(child.check()); code != ProbeChanged {
		t.Fatalf("replacement missed: %s", code)
	}
}
