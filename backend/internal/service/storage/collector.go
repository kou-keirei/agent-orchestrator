package storage

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// ErrSourceGap means metadata cannot establish a physical measurement or the
// requested path's canonical provenance. A nonempty result may accompany it;
// such a result reports metadata and retained unknowns, never physical totals.
var ErrSourceGap = errors.New("SOURCE_GAP")

const MaxMetadataObservations = 256

// MetadataRoot expresses LSTAT_ONLY_TRUSTED_QUIESCENT_ROOTS. Every Fact must
// explicitly be Yes before any filesystem probe. These are caller attestations,
// not properties established by path strings or by this adapter.
//
// The caller must externally hold exclusive quiescence for the entire call,
// covering the root, its ancestors and requested descendants. Live or mutating
// sources are outside this capability. NoReparseOrSymlinks covers that same
// namespace; AliasUnambiguous asserts canonical naming, not physical allocation
// uniqueness. Unknown, No and invalid attestations reject the entire batch.
//
// A boolean attestation cannot prove or prevent hostile parent replacement.
// This contract relies on the caller maintaining these preconditions; it offers
// no general symlink-race-safe lookup. Physical measurements remain unknown.
type MetadataRoot struct {
	Path                string
	Trusted             Fact
	Canonical           Fact
	Quiescent           Fact
	NoReparseOrSymlinks Fact
	AliasUnambiguous    Fact
}

// FileDescriptor supplies one exact path and its protection provenance. The
// observation must carry an explicit identity/extent and an unknown size. No
// fact, cleanup revision, generation, or cleanliness is derived from metadata.
type FileDescriptor struct {
	Root        string
	Path        string
	Observation Observation
}

// FileMetadata describes only an explicit directory or regular file's metadata.
// LogicalBytes is file length, NOT allocated/physical bytes. Directories are
// never enumerated or assigned an aggregate length. Path names are not physical
// extent identities. Reason explains why the planner measurement stays unknown.
type FileMetadata struct {
	ID               string
	LogicalBytes     int64
	LogicalSizeKnown bool
	Directory        bool
	Reason           string
}

type MetadataCollection struct {
	Metadata []FileMetadata
	Plan     Plan
}

// CollectMetadata performs only Lstat metadata queries on explicitly attested
// trusted, canonical, quiescent roots and paths/components below them. All root
// attestations and descriptors are checked before the first metadata query.
// Under the caller-maintained MetadataRoot preconditions it stops at observed
// links/reparse paths. It never reads content, enumerates directories, or invokes
// native identity/allocated-extent probes. It cannot enforce those preconditions
// against a lying caller or concurrent namespace replacement.
//
// Lstat alone cannot prove alias-free physical allocation, or prevent concurrent
// ancestor replacement. Thus every supplied size remains unknown and every
// nonempty collection returns ErrSourceGap, even when logical lengths are known.
// This is a metadata bridge, not a physical census or an execution capability.
func CollectMetadata(roots []MetadataRoot, descriptors []FileDescriptor) (MetadataCollection, error) {
	return collectMetadata(roots, descriptors, os.Lstat)
}

func collectMetadata(roots []MetadataRoot, descriptors []FileDescriptor, lstat func(string) (os.FileInfo, error)) (MetadataCollection, error) {
	if len(descriptors) > MaxMetadataObservations || len(roots) > MaxMetadataObservations {
		return MetadataCollection{}, fmt.Errorf("%w: at most %d descriptors/roots", ErrSourceGap, MaxMetadataObservations)
	}
	allowed := make(map[string]bool, len(roots))
	for i, root := range roots {
		if root.Trusted != Yes || root.Canonical != Yes || root.Quiescent != Yes ||
			root.NoReparseOrSymlinks != Yes || root.AliasUnambiguous != Yes || !boundedAbsolute(root.Path) {
			return MetadataCollection{}, fmt.Errorf("%w: root requires explicit trusted canonical quiescent no-link unambiguous attestations", ErrSourceGap)
		}
		for _, earlier := range roots[:i] {
			if strings.EqualFold(earlier.Path, root.Path) {
				return MetadataCollection{}, fmt.Errorf("%w: ambiguous root alias", ErrSourceGap)
			}
			if nestedMetadataRoot(earlier.Path, root.Path) || nestedMetadataRoot(root.Path, earlier.Path) {
				return MetadataCollection{}, fmt.Errorf("%w: overlapping roots", ErrSourceGap)
			}
		}
		allowed[root.Path] = true
	}
	observations := make([]Observation, 0, len(descriptors))
	byID := make(map[string]FileDescriptor, len(descriptors))
	for i, d := range descriptors {
		if !allowed[d.Root] || !boundedAbsolute(d.Path) {
			return MetadataCollection{}, fmt.Errorf("%w: path/root not explicitly allowlisted", ErrSourceGap)
		}
		rel, err := filepath.Rel(d.Root, d.Path)
		if err != nil || rel == "." || !filepath.IsLocal(rel) || filepath.Join(d.Root, rel) != d.Path {
			return MetadataCollection{}, fmt.Errorf("%w: path escapes or aliases its bounded root", ErrSourceGap)
		}
		for _, earlier := range descriptors[:i] {
			if strings.EqualFold(earlier.Path, d.Path) {
				return MetadataCollection{}, fmt.Errorf("%w: ambiguous path alias", ErrSourceGap)
			}
		}
		if d.Observation.Size.Known || d.Observation.Size.Bytes != 0 {
			return MetadataCollection{}, fmt.Errorf("%w: collector requires an unknown physical measurement", ErrSourceGap)
		}
		observations = append(observations, d.Observation)
		byID[d.Observation.ID] = d
	}
	// Reuse the unchanged core for duplicate IDs, measurement identities,
	// overlapping extents, protection provenance, copying and deterministic order.
	plan, err := BuildPlan(observations)
	if err != nil {
		return MetadataCollection{}, fmt.Errorf("%w: invalid descriptors: %v", ErrSourceGap, err)
	}
	result := MetadataCollection{Plan: plan, Metadata: make([]FileMetadata, 0, len(descriptors))}
	for _, o := range plan.Inventory.Entries {
		d := byID[o.ID]
		metadata := FileMetadata{ID: o.ID}
		info, reason := explicitMetadata(d.Root, d.Path, lstat)
		metadata.Reason = reason
		if info != nil {
			metadata.Directory = info.IsDir()
			if info.Mode().IsRegular() && info.Size() >= 0 {
				metadata.LogicalBytes = info.Size()
				metadata.LogicalSizeKnown = true
			}
		}
		result.Metadata = append(result.Metadata, metadata)
	}
	if len(descriptors) != 0 {
		return result, ErrSourceGap
	}
	return result, nil
}

func boundedAbsolute(path string) bool {
	if path == "" || strings.ContainsRune(path, '\x00') || !filepath.IsAbs(path) || filepath.Clean(path) != path || filepath.Dir(path) == path {
		return false
	}
	volume := filepath.VolumeName(path)
	// Exclude UNC/device namespaces and Windows alternate data streams. Use
	// portable component restrictions even on Unix to reject ambiguous inputs.
	if strings.HasPrefix(volume, `\\`) {
		return false
	}
	tail := strings.TrimPrefix(path, volume)
	parts := strings.FieldsFunc(tail, func(r rune) bool { return r == '/' || r == '\\' })
	if len(parts) > MaxMetadataObservations {
		return false
	}
	for _, part := range parts {
		if !validKey(part) || !filepath.IsLocal(part) || strings.HasSuffix(part, ".") || strings.ContainsAny(part, ":*?[]") {
			return false
		}
	}
	return true
}

func nestedMetadataRoot(parent, child string) bool {
	rel, err := filepath.Rel(parent, child)
	return err == nil && (rel == "." || filepath.IsLocal(rel))
}

func explicitMetadata(root, path string, lstat func(string) (os.FileInfo, error)) (os.FileInfo, string) {
	// Only explicit descendants are visited; roots' ancestors are caller-owned
	// provenance. Never query descendants after an observed link or bad parent.
	rel, _ := filepath.Rel(root, path)
	components := strings.Split(rel, string(filepath.Separator))
	paths := []string{root}
	for _, component := range components {
		paths = append(paths, filepath.Join(paths[len(paths)-1], component))
	}
	for i, current := range paths {
		info, err := lstat(current)
		if err != nil {
			switch {
			case errors.Is(err, fs.ErrNotExist):
				return nil, "metadata_missing"
			case errors.Is(err, fs.ErrPermission):
				return nil, "metadata_denied"
			default:
				return nil, "metadata_unavailable"
			}
		}
		if info == nil || info.Mode()&(os.ModeSymlink|os.ModeIrregular) != 0 {
			return nil, "symlink_or_irregular_path"
		}
		if i != len(paths)-1 {
			if !info.IsDir() {
				return nil, "parent_not_directory"
			}
			continue
		}
		if info.IsDir() {
			return info, "directory_not_enumerated"
		}
		if !info.Mode().IsRegular() || info.Size() < 0 {
			return nil, "unsupported_file_metadata"
		}
		return info, "physical_extents_and_atomic_containment_unproven"
	}
	return nil, "metadata_unavailable"
}
