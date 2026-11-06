package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// ProbeCode is a bounded, path-free diagnostic in ao.storage.size-probe.v1.
type ProbeCode string

const (
	// ProbeInvalid rejects missing trust evidence, ambiguous paths and bad limits.
	ProbeInvalid ProbeCode = "invalid_request"
	// ProbeUnsupported records an unavailable native source capability.
	ProbeUnsupported ProbeCode = "platform_source_gap"
	// ProbeIO records denied, missing or failed filesystem observations.
	ProbeIO ProbeCode = "permission_or_io_error"
	// ProbeUnsafe rejects links, foreign owners/devices and unsupported file types.
	ProbeUnsafe ProbeCode = "link_owner_device_or_type_mismatch"
	// ProbeAlias rejects ambiguous namespace entries.
	ProbeAlias ProbeCode = "ambiguous_name"
	// ProbeChanged invalidates a scan after an observed metadata/binding change.
	ProbeChanged ProbeCode = "changed_during_observation"
	// ProbeLimit invalidates a scan that exhausts an observation budget.
	ProbeLimit ProbeCode = "resource_limit"
	// ProbeCanceled invalidates a canceled scan.
	ProbeCanceled ProbeCode = "canceled"
	// ProbeExtents retains unknown allocation even after logical measurement.
	ProbeExtents ProbeCode = "allocated_extents_unproven"
)

// SizeProbeRequest is an explicit capability for one AO-owned subtree. Root's
// attestations have the same caller-held, entire-call quiescence contract as
// CollectMetadata. This probe verifies observed metadata, not AO durable
// ownership or an atomic snapshot. No live status caller is bound to it.
// OwnerUID must come from trusted AO ownership evidence, never file metadata.
type SizeProbeRequest struct {
	Root     MetadataRoot
	OwnerUID *uint32
	Limits   SizeProbeLimits
}

// WindowsOwnerSID is a canonical native SID, never a UID, account name or RID.
type WindowsOwnerSID string

// WindowsSizeOwnerScope names the exact current AO ownership binding. Generation
// must be positive; an unknown/legacy zero epoch cannot qualify this seam.
// OwnerID identifies the AO-owned storage record, not an OS account.
type WindowsSizeOwnerScope struct {
	SessionID  domain.SessionID
	ProjectID  domain.ProjectID
	Generation int64
	OwnerID    string
	Root       MetadataRoot
}

// WindowsSizeOwnerClaim must come from independent AO authority, not the target
// handle's owner or an API/UI submission. Scope includes the root qualifications.
type WindowsSizeOwnerClaim struct {
	Scope    WindowsSizeOwnerScope
	OwnerSID WindowsOwnerSID
}

// TrustedWindowsSizeOwnerSource is explicit native caller injection, NOT a wire
// DTO or an issuer-name assertion. The composition owner must independently read
// the current AO session/project/generation/storage-owner/root/SID association,
// return exactly one claim, and hold all Root preconditions for the entire call.
// Missing or ambiguous authority must return no claim or an error. Each call is
// a fresh readback; echoing the requested scope or caching an untrusted claim is
// insufficient. The validator reads twice and compares the complete binding.
//
// No existing AO source currently supplies this SID association. Session and
// workspace records alone, current process tokens, account lookups and target
// metadata cannot issue it. There is deliberately no production binding. A future
// issuer must also independently qualify canonical spelling (including Unicode,
// case and 8.3 aliases), ancestor/link/mount exclusion and whole-call quiescence;
// this seam does not create an issuer, traverse ancestors or enforce those facts.
type TrustedWindowsSizeOwnerSource func(context.Context, WindowsSizeOwnerScope) ([]WindowsSizeOwnerClaim, error)

// WindowsSizeOwnerState describes only principal binding, never size acceptance.
type WindowsSizeOwnerState string

const (
	// WindowsOwnerSourceGap retains unknown ownership when evidence is incomplete.
	WindowsOwnerSourceGap WindowsSizeOwnerState = "SOURCE_GAP"
	// WindowsOwnerBound means the injected binding matched a held native object.
	// It does not qualify a walker, logical/physical bytes or cleanup authority.
	WindowsOwnerBound WindowsSizeOwnerState = "OWNER_BOUND"
)

// WindowsSizeOwnerEvidence is bounded and omits paths, identities and byte counts.
type WindowsSizeOwnerEvidence struct {
	State      WindowsSizeOwnerState
	Diagnostic ProbeCode
}

func windowsOwnerGap(code ProbeCode) (WindowsSizeOwnerEvidence, error) {
	return WindowsSizeOwnerEvidence{State: WindowsOwnerSourceGap, Diagnostic: code}, ErrSourceGap
}

// SizeProbeLimits bounds both passes together. No file contents are read.
// MaxLogicalBytes is an observation budget, not a physical allocation limit.
// Elapsed/cancellation checks surround native calls; they cannot interrupt a
// blocked kernel filesystem operation. This is not a hard wall-clock guarantee.
type SizeProbeLimits struct {
	MaxDepth        int
	MaxEntries      int
	MaxLogicalBytes int64
	MaxElapsed      time.Duration
}

// SizeEvidence keeps logical length separate from allocated bytes. DeviceID is
// the Linux st_dev filesystem identity, NOT a physical disk/extent identity.
// PhysicalBytes is always unknown: stat allocation cannot rule out shared
// extents. Results never confer cleanup eligibility, including a known zero.
// Diagnostics have at most one code; paths and per-entry output are omitted.
type SizeEvidence struct {
	Version              string      `json:"version"`
	State                string      `json:"state"`
	LogicalBytes         *int64      `json:"logicalBytes"`
	PhysicalBytes        *int64      `json:"physicalBytes"`
	DeviceID             *string     `json:"deviceId"`
	Entries              int         `json:"entries"`
	Retain               bool        `json:"retain"`
	ExecutableCandidates int         `json:"executableCandidates"`
	Diagnostics          []ProbeCode `json:"diagnostics"`
}

// ProbeSize reads only explicitly trusted, quiescent fixture/source roots.
// Unsupported platforms fail closed before filesystem access. The seam remains
// unbound to HTTP/status and never supplies a known physical Measurement.
func ProbeSize(ctx context.Context, request SizeProbeRequest) (SizeEvidence, error) {
	return probeSize(ctx, request, openSizeRoot, time.Now)
}

// sizeNode is a no-follow, handle-relative native source. Check must validate
// both the held object and its parent/name binding against opening metadata.
type sizeNode interface {
	metadata() sizeMetadata
	names(int) ([]string, error)
	open(string) (sizeNode, error)
	check() error
	close() error
}

type sizeMetadata struct {
	Device, Inode                                  uint64
	Owner                                          uint32
	Links                                          uint64
	Size                                           int64
	Directory, Regular                             bool
	ModifiedSec, ModifiedNS, ChangedSec, ChangedNS int64
}

type probeFailure struct{ code ProbeCode }

func (e probeFailure) Error() string { return string(e.code) }

func probeSize(ctx context.Context, r SizeProbeRequest, open func(string) (sizeNode, error), now func() time.Time) (SizeEvidence, error) {
	result := SizeEvidence{Version: "ao.storage.size-probe.v1", State: "SOURCE_GAP", Retain: true}
	fail := func(code ProbeCode) (SizeEvidence, error) {
		result.Diagnostics = []ProbeCode{code}
		return result, ErrSourceGap
	}
	l := r.Limits
	if r.OwnerUID == nil || r.Root.Trusted != Yes || r.Root.Canonical != Yes || r.Root.Quiescent != Yes ||
		r.Root.NoReparseOrSymlinks != Yes || r.Root.AliasUnambiguous != Yes || !boundedAbsolute(r.Root.Path) ||
		!probePath(r.Root.Path) || l.MaxDepth < 1 || l.MaxDepth > 64 || l.MaxEntries < 2 || l.MaxEntries > 100000 ||
		l.MaxLogicalBytes < 1 || l.MaxLogicalBytes > 1<<50 || l.MaxElapsed <= 0 || l.MaxElapsed > 30*time.Second {
		return fail(ProbeInvalid)
	}
	start := now()
	entries := 0
	var observedBytes int64
	budget := func() error {
		if ctx.Err() != nil {
			return probeFailure{ProbeCanceled}
		}
		if now().Sub(start) >= l.MaxElapsed {
			return probeFailure{ProbeLimit}
		}
		return nil
	}
	var device uint64
	var digest string
	var total int64
	for pass := 0; pass < 2; pass++ {
		if err := budget(); err != nil {
			return fail(probeErrorCode(err))
		}
		root, err := open(r.Root.Path)
		if err != nil {
			return fail(probeErrorCode(err))
		}
		if pass == 0 {
			device = root.metadata().Device
		}
		hash := sha256.New()
		encoder := json.NewEncoder(hash)
		identities := make(map[[2]uint64]bool)
		var bytes int64
		var walk func(sizeNode, int) error
		walk = func(node sizeNode, depth int) error {
			if err := budget(); err != nil {
				return err
			}
			entries++
			if entries > l.MaxEntries || depth > l.MaxDepth {
				return probeFailure{ProbeLimit}
			}
			m := node.metadata()
			if m.Owner != *r.OwnerUID || m.Device != device || (!m.Directory && !m.Regular) || m.Size < 0 || (m.Regular && m.Links != 1) {
				return probeFailure{ProbeUnsafe}
			}
			identity := [2]uint64{m.Device, m.Inode}
			if identities[identity] {
				return probeFailure{ProbeUnsafe}
			}
			identities[identity] = true
			if err := encoder.Encode(m); err != nil {
				return err
			}
			if m.Regular {
				if m.Size > l.MaxLogicalBytes-observedBytes {
					return probeFailure{ProbeLimit}
				}
				observedBytes += m.Size
				bytes += m.Size
			} else {
				aliases := make(map[string]bool)
				for {
					if err := budget(); err != nil {
						return err
					}
					// Read one name at a time: never materialize an unbounded directory.
					names, readErr := node.names(1)
					if len(names) > 1 {
						return probeFailure{ProbeLimit}
					}
					for _, name := range names {
						if entries >= l.MaxEntries {
							return probeFailure{ProbeLimit}
						}
						key := strings.ToLower(name)
						if !probeName(name) || aliases[key] {
							return probeFailure{ProbeAlias}
						}
						aliases[key] = true
						if err := encoder.Encode(name); err != nil {
							return err
						}
						child, err := node.open(name)
						if err != nil {
							return err
						}
						err = walk(child, depth+1)
						closeErr := child.close()
						if err != nil {
							return err
						}
						if closeErr != nil {
							return closeErr
						}
					}
					if errors.Is(readErr, io.EOF) {
						break
					}
					if readErr != nil {
						return readErr
					}
					if len(names) == 0 {
						return probeFailure{ProbeIO}
					}
				}
			}
			if err := budget(); err != nil {
				return err
			}
			return node.check()
		}
		err = walk(root, 0)
		closeErr := root.close()
		if err != nil {
			return fail(probeErrorCode(err))
		}
		if closeErr != nil {
			return fail(probeErrorCode(closeErr))
		}
		current := hex.EncodeToString(hash.Sum(nil))
		if pass == 0 {
			digest, total = current, bytes
		} else if digest != current || bytes != total {
			return fail(ProbeChanged)
		}
	}
	if err := budget(); err != nil {
		return fail(probeErrorCode(err))
	}
	// Decimal encoding avoids implying a stable globally unique volume UUID.
	deviceID := "linux-st_dev:" + strconv.FormatUint(device, 10)
	result.LogicalBytes, result.DeviceID, result.Entries = &total, &deviceID, entries/2
	return fail(ProbeExtents)
}

func probeErrorCode(err error) ProbeCode {
	var failure probeFailure
	if errors.As(err, &failure) {
		return failure.code
	}
	return ProbeIO
}

func probePath(path string) bool {
	if len(path) > 4096 {
		return false
	}
	for _, part := range strings.FieldsFunc(strings.TrimPrefix(path, filepath.VolumeName(path)), func(r rune) bool { return r == rune(filepath.Separator) }) {
		if !probeName(part) {
			return false
		}
	}
	return true
}

// Reject non-ASCII rather than attempt Unicode normalization/alias inference.
// Windows reserved punctuation, trailing dots/spaces and alternate streams are
// rejected portably even though the native source currently supports Linux only.
func probeName(name string) bool {
	if name == "" || name == "." || name == ".." || len(name) > 255 || strings.TrimSpace(name) != name || strings.HasSuffix(name, ".") {
		return false
	}
	for _, r := range name {
		if r < 32 || r > 126 || strings.ContainsRune(`/\:*?"<>|`, r) {
			return false
		}
	}
	return true
}
