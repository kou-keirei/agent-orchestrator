package storage

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func metadataDescriptor(root, name string, category Category) FileDescriptor {
	return FileDescriptor{Root: root, Path: filepath.Join(root, name), Observation: Observation{
		ID: name, Category: category, Size: Measurement{MeasurementID: "metadata-" + name, Extent: []string{"fixture", name}},
	}}
}

// Only disposable fixtures created and held quiescent by this test caller use
// these attestations. They are not evidence about any installed AO data root.
func trustedMetadataRoot(path string) MetadataRoot {
	return MetadataRoot{
		Path: path, Trusted: Yes, Canonical: Yes, Quiescent: Yes,
		NoReparseOrSymlinks: Yes, AliasUnambiguous: Yes,
	}
}

func TestCollectMetadataExplicitFilesAndPlanner(t *testing.T) {
	root := t.TempDir()
	roots := []MetadataRoot{trustedMetadataRoot(root)}
	descriptors := []FileDescriptor{}
	for i, item := range []struct {
		name     string
		category Category
		bytes    int
	}{{"db", Database, 100}, {"wal", WAL, 20}, {"shm", SHM, 3}, {"snapshot", Snapshot, 40}, {"worktree-a", Worktree, 10}, {"worktree-b", Worktree, 11}, {"worktree-c", Worktree, 12}} {
		d := metadataDescriptor(root, item.name, item.category)
		if err := os.WriteFile(d.Path, make([]byte, item.bytes), 0600); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			d.Observation.Facts = FactsCurrent
		}
		descriptors = append(descriptors, d)
	}
	descriptors = append(descriptors, metadataDescriptor(root, "missing", Other))
	// An unrequested file must have no effect, regardless of its size.
	if err := os.WriteFile(filepath.Join(root, "not-allowlisted"), make([]byte, 999), 0600); err != nil {
		t.Fatal(err)
	}
	allowedPaths := map[string]bool{root: true}
	for _, d := range descriptors {
		allowedPaths[d.Path] = true
	}
	got, err := collectMetadata(roots, descriptors, func(path string) (os.FileInfo, error) {
		if !allowedPaths[path] {
			t.Fatalf("queried a path outside explicit fixtures: %q", path)
		}
		return os.Lstat(path)
	})
	if !errors.Is(err, ErrSourceGap) {
		t.Fatalf("want SOURCE_GAP, got %v", err)
	}
	logical := int64(0)
	for _, m := range got.Metadata {
		logical += m.LogicalBytes
	}
	if logical != 196 || len(got.Metadata) != 8 {
		t.Fatalf("unexpected logical metadata: %+v", got.Metadata)
	}
	if got.Plan.Inventory.Total != (ByteTotal{UnknownEntries: 8, Complete: false}) || got.Plan.ExecutableCandidates != 0 {
		t.Fatalf("logical lengths were counted as physical bytes: %+v", got.Plan)
	}
	for i, o := range got.Plan.Inventory.Entries {
		if o.Size.Known || o.Size.Bytes != 0 || !got.Plan.Decisions[i].Retain {
			t.Fatal("measurement or deletion authority inferred")
		}
	}
	for i, j := 0, len(descriptors)-1; i < j; i, j = i+1, j-1 {
		descriptors[i], descriptors[j] = descriptors[j], descriptors[i]
	}
	reversed, err := CollectMetadata(roots, descriptors)
	if !errors.Is(err, ErrSourceGap) || !reflect.DeepEqual(got, reversed) {
		t.Fatal("result depends on descriptor order")
	}
}

func TestCollectMetadataRejectsBeforeIO(t *testing.T) {
	root := t.TempDir()
	for _, kind := range []string{"untrusted", "unknown-root", "relative", "traversal", "outside", "glob", "stream", "duplicate-id", "duplicate-extent", "duplicate-measurement", "alias", "overlap", "known-size", "limit", "generation"} {
		t.Run(kind, func(t *testing.T) {
			roots := []MetadataRoot{trustedMetadataRoot(root)}
			a, b := metadataDescriptor(root, "a", Other), metadataDescriptor(root, "b", Other)
			descriptors := []FileDescriptor{a, b}
			switch kind {
			case "untrusted":
				roots[0].Canonical = Unknown
			case "unknown-root":
				descriptors[0].Root = filepath.Join(root, "untrusted")
			case "relative":
				descriptors[0].Path = "relative"
			case "traversal":
				descriptors[0].Path = root + string(filepath.Separator) + ".." + string(filepath.Separator) + "escape"
			case "outside":
				descriptors[0].Path = filepath.Join(filepath.Dir(root), "outside")
			case "glob":
				descriptors[0].Path = filepath.Join(root, "*")
			case "stream":
				descriptors[0].Path = filepath.Join(root, "file:stream")
			case "duplicate-id":
				descriptors[1].Observation.ID = a.Observation.ID
			case "duplicate-extent":
				descriptors[1].Observation.Size.Extent = a.Observation.Size.Extent
			case "duplicate-measurement":
				descriptors[1].Observation.Size.MeasurementID = a.Observation.Size.MeasurementID
			case "alias":
				descriptors[1].Path = strings.ToUpper(a.Path)
			case "overlap":
				descriptors[1].Observation.Size.Extent = []string{"fixture"}
			case "known-size":
				descriptors[0].Observation.Size.Known = true
			case "limit":
				descriptors = make([]FileDescriptor, MaxMetadataObservations+1)
			case "generation":
				descriptors[0].Observation = cleanupWorktree()
				descriptors[0].Observation.Size.Known = false
				descriptors[0].Observation.Size.Bytes = 0
				descriptors[0].Observation.Worktree.CurrentGeneration++
			}
			calls := 0
			got, err := collectMetadata(roots, descriptors, func(string) (os.FileInfo, error) { calls++; return nil, fs.ErrPermission })
			if !errors.Is(err, ErrSourceGap) || calls != 0 || len(got.Metadata) != 0 {
				t.Fatalf("unsafe preflight: calls=%d, result=%+v, error=%v", calls, got, err)
			}
		})
	}
}

func TestCollectMetadataMissingDeniedAndDirectory(t *testing.T) {
	root := t.TempDir()
	for _, tc := range []struct {
		name   string
		cause  error
		reason string
	}{
		{"missing", fs.ErrNotExist, "metadata_missing"}, {"denied", fs.ErrPermission, "metadata_denied"}, {"failed", errors.New("probe failed"), "metadata_unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := metadataDescriptor(root, tc.name, Other)
			got, err := collectMetadata([]MetadataRoot{trustedMetadataRoot(root)}, []FileDescriptor{d}, func(path string) (os.FileInfo, error) {
				if path == d.Path {
					return nil, &os.PathError{Op: "lstat", Path: path, Err: tc.cause}
				}
				return os.Lstat(path)
			})
			if !errors.Is(err, ErrSourceGap) || got.Metadata[0].Reason != tc.reason || got.Metadata[0].LogicalSizeKnown || got.Plan.Inventory.Total.Complete {
				t.Fatalf("unknown metadata lost: %+v, %v", got, err)
			}
		})
	}
	d := metadataDescriptor(root, "directory", Worktree)
	if err := os.Mkdir(d.Path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d.Path, "not-enumerated"), []byte("private"), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := CollectMetadata([]MetadataRoot{trustedMetadataRoot(root)}, []FileDescriptor{d})
	if !errors.Is(err, ErrSourceGap) || !got.Metadata[0].Directory || got.Metadata[0].LogicalSizeKnown || got.Metadata[0].Reason != "directory_not_enumerated" {
		t.Fatalf("directory was measured recursively: %+v, %v", got, err)
	}
}

func TestCollectMetadataStopsAtSymlinks(t *testing.T) {
	// Deliberately violate the caller's no-link attestation to exercise the
	// observed-link defense. This does not establish race-safe containment.
	root := t.TempDir()
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, "file"), []byte("secret"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, directory := range []bool{false, true} {
		name, destination := "file-link", filepath.Join(target, "file")
		if directory {
			name, destination = "dir-link", target
		}
		link := filepath.Join(root, name)
		if err := os.Symlink(destination, link); err != nil {
			t.Skipf("OS does not permit disposable symlinks: %v", err)
		}
		d := metadataDescriptor(root, name, Other)
		if directory {
			d.Path = filepath.Join(link, "file")
		}
		calls := []string{}
		got, err := collectMetadata([]MetadataRoot{trustedMetadataRoot(root)}, []FileDescriptor{d}, func(path string) (os.FileInfo, error) {
			calls = append(calls, path)
			return os.Lstat(path)
		})
		if !errors.Is(err, ErrSourceGap) || got.Metadata[0].Reason != "symlink_or_irregular_path" || got.Metadata[0].LogicalSizeKnown {
			t.Fatalf("link accepted: %+v, %v", got, err)
		}
		if directory && len(calls) != 2 {
			t.Fatalf("queried past directory link: %v", calls)
		}
	}
	rootLink := filepath.Join(root, "root-link")
	if err := os.Symlink(target, rootLink); err != nil {
		t.Fatal(err)
	}
	d := metadataDescriptor(rootLink, "file", Other)
	calls := 0
	got, err := collectMetadata([]MetadataRoot{trustedMetadataRoot(rootLink)}, []FileDescriptor{d}, func(path string) (os.FileInfo, error) { calls++; return os.Lstat(path) })
	if !errors.Is(err, ErrSourceGap) || calls != 1 || got.Metadata[0].LogicalSizeKnown {
		t.Fatalf("followed symlink root: %+v, %v, calls=%d", got, err, calls)
	}
}

func TestCollectMetadataHardlinkIsNeverCountedAsDisjoint(t *testing.T) {
	root := t.TempDir()
	a, b := metadataDescriptor(root, "a", Other), metadataDescriptor(root, "b", Other)
	if err := os.WriteFile(a.Path, []byte("shared"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(a.Path, b.Path); err != nil {
		t.Skipf("OS does not permit disposable hardlinks: %v", err)
	}
	got, err := CollectMetadata([]MetadataRoot{trustedMetadataRoot(root)}, []FileDescriptor{a, b})
	if !errors.Is(err, ErrSourceGap) || got.Plan.Inventory.Total.MeasuredBytes != 0 || got.Plan.Inventory.Total.Complete {
		t.Fatalf("aliased bytes counted: %+v, %v", got, err)
	}
}

func TestCollectMetadataPreservesProtectionAndEmptyInput(t *testing.T) {
	root := t.TempDir()
	d := metadataDescriptor(root, "worktree", Worktree)
	d.Observation = cleanupWorktree()
	d.Observation.Size.Known, d.Observation.Size.Bytes = false, 0
	d.Observation.Worktree.Pinned, d.Observation.Worktree.Stable, d.Observation.Worktree.UniqueCommits = Yes, Yes, Unknown
	got, err := CollectMetadata([]MetadataRoot{trustedMetadataRoot(root)}, []FileDescriptor{d})
	if !errors.Is(err, ErrSourceGap) || !reflect.DeepEqual(got.Plan.Inventory.Entries[0], d.Observation) || !got.Plan.Decisions[0].Retain {
		t.Fatal("protection provenance changed")
	}
	d.Observation.Worktree.Cleanup.RowRevision++
	if got.Plan.Inventory.Entries[0].Worktree.Cleanup.RowRevision != 19 {
		t.Fatal("result aliases caller provenance")
	}
	empty, err := CollectMetadata(nil, nil)
	if err != nil || len(empty.Metadata) != 0 || !empty.Plan.Inventory.Total.Complete {
		t.Fatalf("bad empty result: %+v, %v", empty, err)
	}
}

func TestCollectMetadataMaximumAndKnownEmptyLogicalFile(t *testing.T) {
	root := t.TempDir()
	d := metadataDescriptor(root, "empty", Other)
	if err := os.WriteFile(d.Path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	got, err := CollectMetadata([]MetadataRoot{trustedMetadataRoot(root)}, []FileDescriptor{d})
	if !errors.Is(err, ErrSourceGap) || !got.Metadata[0].LogicalSizeKnown || got.Metadata[0].LogicalBytes != 0 || got.Plan.Inventory.Total.Complete {
		t.Fatalf("empty logical file lost: %+v, %v", got, err)
	}
	descriptors := make([]FileDescriptor, MaxMetadataObservations)
	for i := range descriptors {
		descriptors[i] = metadataDescriptor(root, strings.Repeat("x", i+1), Other)
	}
	// Inject denied metadata only within the disposable root, without creating
	// files with names longer than the host filesystem supports.
	got, err = collectMetadata([]MetadataRoot{trustedMetadataRoot(root)}, descriptors, func(string) (os.FileInfo, error) { return nil, fs.ErrPermission })
	if !errors.Is(err, ErrSourceGap) || len(got.Metadata) != MaxMetadataObservations || got.Plan.Inventory.Total.UnknownEntries != MaxMetadataObservations {
		t.Fatalf("maximum rejected or truncated: %+v, %v", got, err)
	}
}

func TestCollectMetadataRootAttestationsRejectWithoutIO(t *testing.T) {
	root := t.TempDir()
	d := metadataDescriptor(root, "must-not-probe", Other)
	for _, field := range []string{"trusted", "canonical", "quiescent", "no-links", "aliases"} {
		for _, state := range []Fact{Unknown, No, Fact("invalid")} {
			t.Run(field+"/"+string(state), func(t *testing.T) {
				bad := trustedMetadataRoot(root)
				switch field {
				case "trusted":
					bad.Trusted = state
				case "canonical":
					bad.Canonical = state
				case "quiescent":
					bad.Quiescent = state
				case "no-links":
					bad.NoReparseOrSymlinks = state
				case "aliases":
					bad.AliasUnambiguous = state
				}
				// A later rejected root must also prevent reads of an earlier
				// otherwise trusted descriptor. The callback performs no I/O.
				for _, roots := range [][]MetadataRoot{{bad}, {trustedMetadataRoot(filepath.Join(root, "other")), bad}} {
					descriptors := []FileDescriptor{d}
					if len(roots) == 2 {
						descriptors = append([]FileDescriptor{metadataDescriptor(roots[0].Path, "first", Other)}, d)
					}
					calls := 0
					got, err := collectMetadata(roots, descriptors, func(string) (os.FileInfo, error) {
						calls++
						return nil, fs.ErrPermission
					})
					if !errors.Is(err, ErrSourceGap) || calls != 0 || len(got.Metadata) != 0 {
						t.Fatalf("rejected attestation probed source: calls=%d result=%+v err=%v", calls, got, err)
					}
				}
			})
		}
	}
}

func TestCollectMetadataHighRiskRootsRejectWithoutIO(t *testing.T) {
	root := t.TempDir()
	d := metadataDescriptor(root, "must-not-probe", Other)
	for _, kind := range []string{"zero", "relative", "traversal", "volume-root", "stream", "glob", "root-alias", "nested-roots"} {
		t.Run(kind, func(t *testing.T) {
			roots := []MetadataRoot{trustedMetadataRoot(root)}
			switch kind {
			case "zero":
				roots[0] = MetadataRoot{Path: root}
			case "relative":
				roots[0].Path = "relative"
			case "traversal":
				roots[0].Path = root + string(filepath.Separator) + ".." + string(filepath.Separator) + "escape"
			case "volume-root":
				roots[0].Path = filepath.VolumeName(root) + string(filepath.Separator)
			case "stream":
				roots[0].Path = root + ":stream"
			case "glob":
				roots[0].Path = filepath.Join(root, "*")
			case "root-alias":
				roots = append(roots, trustedMetadataRoot(strings.ToUpper(root)))
			case "nested-roots":
				roots = append(roots, trustedMetadataRoot(filepath.Join(root, "nested")))
			}
			calls := 0
			got, err := collectMetadata(roots, []FileDescriptor{d}, func(string) (os.FileInfo, error) {
				calls++
				return nil, fs.ErrPermission
			})
			if !errors.Is(err, ErrSourceGap) || calls != 0 || len(got.Metadata) != 0 {
				t.Fatalf("high-risk root probed source: calls=%d result=%+v err=%v", calls, got, err)
			}
		})
	}
}

func TestCollectMetadataOnePathFailureStopsAtRoot(t *testing.T) {
	root := t.TempDir()
	d := metadataDescriptor(root, "must-not-probe", Other)
	calls := []string{}
	got, err := collectMetadata([]MetadataRoot{trustedMetadataRoot(root)}, []FileDescriptor{d}, func(path string) (os.FileInfo, error) {
		calls = append(calls, path)
		return nil, &os.PathError{Op: "lstat", Path: path, Err: fs.ErrPermission}
	})
	if !errors.Is(err, ErrSourceGap) || !reflect.DeepEqual(calls, []string{root}) {
		t.Fatalf("queried past failing root: calls=%v err=%v", calls, err)
	}
	if len(got.Metadata) != 1 || got.Metadata[0].Reason != "metadata_denied" || got.Metadata[0].LogicalSizeKnown ||
		got.Plan.Inventory.Total.Complete || got.Plan.Inventory.Total.UnknownEntries != 1 ||
		got.Plan.ExecutableCandidates != 0 || !got.Plan.Decisions[0].Retain {
		t.Fatalf("failing root lost retained unknown: %+v", got)
	}
}
