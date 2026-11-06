package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type reparseMetadata struct {
	os.FileInfo
	mode os.FileMode
}

func (m reparseMetadata) Mode() os.FileMode { return m.mode }

func TestStorageObservedLinkAndReparseMetadataRetained(t *testing.T) {
	// Deterministic adapter-level representation of links/reparse entries.
	// This is not native Windows junction or physical-extent qualification.
	root := t.TempDir()
	info, err := os.Lstat(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []os.FileMode{os.ModeSymlink | os.ModeDir, os.ModeIrregular | os.ModeDir} {
		calls := 0
		d := metadataDescriptor(root, "junction", Worktree)
		d.Path = filepath.Join(d.Path, "child")
		result, err := collectMetadata([]MetadataRoot{trustedMetadataRoot(root)}, []FileDescriptor{d}, func(path string) (os.FileInfo, error) {
			calls++
			if path == root {
				return info, nil
			}
			return reparseMetadata{FileInfo: info, mode: mode}, nil
		})
		if !errors.Is(err, ErrSourceGap) || calls != 2 || result.Metadata[0].LogicalSizeKnown || !result.Plan.Decisions[0].Retain || result.Plan.ExecutableCandidates != 0 {
			t.Fatalf("followed or trusted reparse entry: %+v %v calls=%d", result, err, calls)
		}
	}
}

func TestStorageConcurrentNamespaceCannotEstablishPhysicalSize(t *testing.T) {
	root := t.TempDir()
	rootInfo, err := os.Lstat(root)
	if err != nil {
		t.Fatal(err)
	}
	d := metadataDescriptor(root, "changing", Other)
	// A caller reporting concurrent mutation cannot assert quiescence. Reject
	// before the first lookup instead of attempting a second containment engine.
	unquiet := trustedMetadataRoot(root)
	unquiet.Quiescent = No
	calls := 0
	_, err = collectMetadata([]MetadataRoot{unquiet}, []FileDescriptor{d}, func(string) (os.FileInfo, error) { calls++; return rootInfo, nil })
	if !errors.Is(err, ErrSourceGap) || calls != 0 {
		t.Fatalf("probed mutating namespace: %v %d", err, calls)
	}
}
