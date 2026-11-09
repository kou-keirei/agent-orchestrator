//go:build linux

package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

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

func TestSizeProbeLinuxAllowsAncestorSiblingMutation(t *testing.T) {
	r := nativeProbeRequest(t)
	parent := filepath.Dir(r.Root.Path)
	calls := 0
	result, err := probeSize(context.Background(), r, func(path string) (sizeNode, error) {
		root, err := openSizeRoot(path)
		if err != nil {
			return nil, err
		}
		calls++
		// A directory sibling changes the ancestor's link count as well as its
		// contents. Force distinct timestamps without relying on clock timing.
		if err := os.Mkdir(filepath.Join(parent, fmt.Sprintf("sibling-%d", calls)), 0700); err != nil {
			_ = root.close()
			return nil, err
		}
		stamp := time.Unix(int64(calls), 0)
		if err := os.Chtimes(parent, stamp, stamp); err != nil {
			_ = root.close()
			return nil, err
		}
		return root, nil
	}, time.Now)
	if result.DeviceID == nil {
		t.Fatalf("stable subtree rejected: %+v, %v", result, err)
	}
	device := "linux-st_dev:7"
	result.DeviceID = &device
	logical := int64(0)
	requireProbe(t, result, err, ProbeExtents, &logical)
	if calls != 2 || result.Entries != 1 {
		t.Fatalf("two-pass root accounting: calls=%d, %+v", calls, result)
	}
}

func TestSizeProbeLinuxDetectsAncestorAndSubtreeMutation(t *testing.T) {
	for _, kind := range []string{"ancestor-replacement", "ancestor-symlink", "root-replacement", "root-contents", "root-mtime", "child-directory-mtime", "held-file"} {
		t.Run(kind, func(t *testing.T) {
			parent := filepath.Join(t.TempDir(), "parent")
			path := filepath.Join(parent, "ao")
			if err := os.MkdirAll(path, 0700); err != nil {
				t.Fatal(err)
			}
			file := filepath.Join(path, "file")
			if err := os.WriteFile(file, []byte("ao"), 0600); err != nil {
				t.Fatal(err)
			}
			root, err := openSizeRoot(path)
			if err != nil {
				t.Fatal(err)
			}
			defer root.close()
			check := root.check
			switch kind {
			case "ancestor-replacement", "ancestor-symlink":
				if err := os.Rename(parent, parent+"-old"); err != nil {
					t.Fatal(err)
				}
				if kind == "ancestor-symlink" {
					err = os.Symlink(parent+"-old", parent)
				} else {
					err = os.MkdirAll(path, 0700)
				}
			case "root-replacement":
				if err := os.Rename(path, path+"-old"); err != nil {
					t.Fatal(err)
				}
				err = os.Mkdir(path, 0700)
			case "root-contents":
				err = os.Mkdir(filepath.Join(path, "new"), 0700)
			case "root-mtime":
				stamp := time.Unix(1, 0)
				err = os.Chtimes(path, stamp, stamp)
			case "child-directory-mtime":
				directory := filepath.Join(path, "directory")
				if err := os.Mkdir(directory, 0700); err != nil {
					t.Fatal(err)
				}
				child, openErr := root.open("directory")
				if openErr != nil {
					t.Fatal(openErr)
				}
				defer child.close()
				check = child.check
				stamp := time.Unix(1, 0)
				err = os.Chtimes(directory, stamp, stamp)
			case "held-file":
				child, openErr := root.open("file")
				if openErr != nil {
					t.Fatal(openErr)
				}
				defer child.close()
				check = child.check
				err = os.WriteFile(file, []byte("changed"), 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			if code := probeErrorCode(check()); code != ProbeChanged {
				t.Fatalf("%s missed: %s", kind, code)
			}
		})
	}
}

func TestSizeProbeLinuxAncestorIdentityFields(t *testing.T) {
	for _, field := range []string{"device", "inode", "owner", "directory", "regular"} {
		t.Run(field, func(t *testing.T) {
			r := nativeProbeRequest(t)
			node, err := openSizeRoot(r.Root.Path)
			if err != nil {
				t.Fatal(err)
			}
			defer node.close()
			root := node.(*linuxSizeNode)
			ancestor := root.ancestors[len(root.ancestors)-1]
			// Simulate identity/owner/type drift without privileged chown or mounts.
			switch field {
			case "device":
				ancestor.initial.Device++
			case "inode":
				ancestor.initial.Inode++
			case "owner":
				ancestor.initial.Owner++
			case "directory":
				ancestor.initial.Directory = false
			case "regular":
				ancestor.initial.Regular = true
			}
			if code := probeErrorCode(root.check()); code != ProbeChanged {
				t.Fatalf("ancestor %s drift missed: %s", field, code)
			}
		})
	}
}
