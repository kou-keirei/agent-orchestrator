package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// Fixture nodes exercise source failures without touching production paths or
// depending on permissions, scheduler timing, sparse support or root privileges.
type fixtureSizeNode struct {
	meta                       sizeMetadata
	children                   map[string]*fixtureSizeNode
	order                      []string
	cursor                     int
	readErr, openErr, checkErr error
	onCheck                    func()
	closed                     bool
}

func (n *fixtureSizeNode) metadata() sizeMetadata { return n.meta }
func (n *fixtureSizeNode) names(int) ([]string, error) {
	if n.readErr != nil {
		return nil, n.readErr
	}
	if n.cursor == len(n.order) {
		return nil, io.EOF
	}
	name := n.order[n.cursor]
	n.cursor++
	return []string{name}, nil
}
func (n *fixtureSizeNode) open(name string) (sizeNode, error) {
	if n.openErr != nil {
		return nil, n.openErr
	}
	return n.children[name], nil
}
func (n *fixtureSizeNode) check() error {
	if n.onCheck != nil {
		n.onCheck()
	}
	return n.checkErr
}
func (n *fixtureSizeNode) close() error { n.closed = true; return nil }

func probeRequest(t *testing.T) SizeProbeRequest {
	t.Helper()
	owner := uint32(42)
	return SizeProbeRequest{Root: MetadataRoot{Path: filepath.Join(t.TempDir(), "ao"), Trusted: Yes, Canonical: Yes,
		Quiescent: Yes, NoReparseOrSymlinks: Yes, AliasUnambiguous: Yes}, OwnerUID: &owner,
		Limits: SizeProbeLimits{MaxDepth: 8, MaxEntries: 4096, MaxLogicalBytes: 1 << 30, MaxElapsed: time.Second}}
}

func fixtureTree(count int) *fixtureSizeNode {
	root := &fixtureSizeNode{meta: sizeMetadata{Owner: 42, Device: 7, Inode: 1, Directory: true}, children: map[string]*fixtureSizeNode{}}
	for i := 0; i < count; i++ {
		name := fmt.Sprintf("file-%04d", i)
		root.order = append(root.order, name)
		root.children[name] = &fixtureSizeNode{meta: sizeMetadata{Owner: 42, Device: 7, Inode: uint64(i + 2), Links: 1, Size: 10, Regular: true}}
	}
	return root
}

func fixtureOpen(root *fixtureSizeNode) func(string) (sizeNode, error) {
	return func(string) (sizeNode, error) {
		var reset func(*fixtureSizeNode)
		reset = func(n *fixtureSizeNode) {
			n.cursor = 0
			n.closed = false
			for _, c := range n.children {
				reset(c)
			}
		}
		reset(root)
		return root, nil
	}
}

func requireProbe(t *testing.T, result SizeEvidence, err error, code ProbeCode, logical *int64) {
	t.Helper()
	if !errors.Is(err, ErrSourceGap) || result.Version != "ao.storage.size-probe.v1" || result.State != "SOURCE_GAP" ||
		!result.Retain || result.ExecutableCandidates != 0 || result.PhysicalBytes != nil || len(result.Diagnostics) != 1 || result.Diagnostics[0] != code {
		t.Fatalf("unsafe or unexpected evidence: %+v, %v", result, err)
	}
	if logical == nil {
		if result.LogicalBytes != nil || result.DeviceID != nil || result.Entries != 0 {
			t.Fatalf("partial evidence leaked: %+v", result)
		}
	} else if result.LogicalBytes == nil || *result.LogicalBytes != *logical || result.DeviceID == nil || *result.DeviceID != "linux-st_dev:7" {
		t.Fatalf("measurement mismatch: %+v", result)
	}
	encoded, marshalErr := json.Marshal(result)
	if marshalErr != nil || len(encoded) > 1024 {
		t.Fatalf("output bound: %d, %v", len(encoded), marshalErr)
	}
}

func TestSizeProbeThousandEntries(t *testing.T) {
	r := probeRequest(t)
	root := fixtureTree(1100)
	result, err := probeSize(context.Background(), r, fixtureOpen(root), time.Now)
	logical := int64(11000)
	requireProbe(t, result, err, ProbeExtents, &logical)
	if result.Entries != 1101 || !root.closed {
		t.Fatalf("entry/handle accounting: %+v", result)
	}
	for _, child := range root.children {
		if !child.closed {
			t.Fatal("leaked child")
		}
	}
}

func TestSizeProbeRejectsUntrustedRequestsBeforeIO(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*SizeProbeRequest)
	}{
		{"owner", func(r *SizeProbeRequest) { r.OwnerUID = nil }},
		{"trust", func(r *SizeProbeRequest) { r.Root.Trusted = Unknown }},
		{"live", func(r *SizeProbeRequest) { r.Root.Quiescent = No }},
		{"canonical", func(r *SizeProbeRequest) { r.Root.Canonical = Unknown }},
		{"links", func(r *SizeProbeRequest) { r.Root.NoReparseOrSymlinks = Unknown }},
		{"aliases", func(r *SizeProbeRequest) { r.Root.AliasUnambiguous = Unknown }},
		{"traversal", func(r *SizeProbeRequest) { r.Root.Path += string(filepath.Separator) + ".." }},
		{"unicode", func(r *SizeProbeRequest) { r.Root.Path += "é" }},
		{"alternate-separator", func(r *SizeProbeRequest) { r.Root.Path += "/bad\\name" }},
		{"relative", func(r *SizeProbeRequest) { r.Root.Path = "relative" }},
		{"filesystem-root", func(r *SizeProbeRequest) { r.Root.Path = filepath.VolumeName(r.Root.Path) + string(filepath.Separator) }},
		{"time", func(r *SizeProbeRequest) { r.Limits.MaxElapsed = time.Minute }},
		{"entries", func(r *SizeProbeRequest) { r.Limits.MaxEntries = 100001 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := probeRequest(t)
			test.change(&r)
			result, err := probeSize(context.Background(), r, func(string) (sizeNode, error) { t.Fatal("unexpected filesystem access"); return nil, nil }, time.Now)
			requireProbe(t, result, err, ProbeInvalid, nil)
		})
	}
}

func TestSizeProbeSourceFailures(t *testing.T) {
	for _, test := range []struct {
		name   string
		code   ProbeCode
		change func(*fixtureSizeNode)
	}{
		{"owner", ProbeUnsafe, func(n *fixtureSizeNode) { n.children[n.order[0]].meta.Owner++ }},
		{"device", ProbeUnsafe, func(n *fixtureSizeNode) { n.children[n.order[0]].meta.Device++ }},
		{"hardlink", ProbeUnsafe, func(n *fixtureSizeNode) { n.children[n.order[0]].meta.Links = 2 }},
		{"duplicate-inode", ProbeUnsafe, func(n *fixtureSizeNode) { n.children[n.order[1]].meta.Inode = n.children[n.order[0]].meta.Inode }},
		{"symlink-reparse", ProbeUnsafe, func(n *fixtureSizeNode) { n.children[n.order[0]].meta.Regular = false }},
		{"negative-length", ProbeUnsafe, func(n *fixtureSizeNode) { n.children[n.order[0]].meta.Size = -1 }},
		{"permission", ProbeIO, func(n *fixtureSizeNode) { n.readErr = fs.ErrPermission }},
		{"open-denied", ProbeIO, func(n *fixtureSizeNode) { n.openErr = fs.ErrPermission }},
		{"mutation", ProbeChanged, func(n *fixtureSizeNode) { n.children[n.order[0]].checkErr = probeFailure{ProbeChanged} }},
		{"case-collision", ProbeAlias, func(n *fixtureSizeNode) { n.order = append(n.order, "FILE-0000") }},
		{"unicode-alias", ProbeAlias, func(n *fixtureSizeNode) { n.order = append(n.order, "e\u0301") }},
		{"traversal", ProbeAlias, func(n *fixtureSizeNode) { n.order = append(n.order, "..") }},
		{"stream", ProbeAlias, func(n *fixtureSizeNode) { n.order = append(n.order, "file:stream") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := fixtureTree(2)
			test.change(root)
			result, err := probeSize(context.Background(), probeRequest(t), fixtureOpen(root), time.Now)
			requireProbe(t, result, err, test.code, nil)
			if !root.closed {
				t.Fatal("leaked root")
			}
		})
	}
}

func TestSizeProbeBudgetsAndCancellation(t *testing.T) {
	for _, kind := range []string{"entries", "bytes", "depth", "elapsed", "cancel-before", "cancel-mid"} {
		t.Run(kind, func(t *testing.T) {
			r := probeRequest(t)
			root := fixtureTree(3)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			clock := time.Now
			code := ProbeLimit
			switch kind {
			case "entries":
				r.Limits.MaxEntries = 3
			case "bytes":
				r.Limits.MaxLogicalBytes = 25
			case "depth":
				r.Limits.MaxDepth = 1
				child := fixtureTree(1)
				child.meta.Inode = 999
				root.children[root.order[0]] = child
			case "elapsed":
				now := time.Now()
				clock = func() time.Time { now = now.Add(time.Second); return now }
			case "cancel-before":
				cancel()
				code = ProbeCanceled
			case "cancel-mid":
				root.children[root.order[0]].onCheck = cancel
				code = ProbeCanceled
			}
			result, err := probeSize(ctx, r, fixtureOpen(root), clock)
			requireProbe(t, result, err, code, nil)
		})
	}
}

func TestSizeProbeBetweenPassMutation(t *testing.T) {
	root := fixtureTree(1)
	open := fixtureOpen(root)
	calls := 0
	result, err := probeSize(context.Background(), probeRequest(t), func(path string) (sizeNode, error) {
		calls++
		if calls == 2 {
			root.children[root.order[0]].meta.Inode++
		}
		return open(path)
	}, time.Now)
	requireProbe(t, result, err, ProbeChanged, nil)
}

func TestSizeProbeSparseLogicalAndZero(t *testing.T) {
	for _, size := range []int64{0, 1 << 20} {
		root := fixtureTree(1)
		root.children[root.order[0]].meta.Size = size
		result, err := probeSize(context.Background(), probeRequest(t), fixtureOpen(root), time.Now)
		requireProbe(t, result, err, ProbeExtents, &size)
	}
}

func TestSizeProbeUnsupportedPlatform(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("native Linux adapter has separate fixture tests")
	}
	result, err := ProbeSize(context.Background(), probeRequest(t))
	requireProbe(t, result, err, ProbeUnsupported, nil)
}

func TestSizeProbeCannotChangeRetention(t *testing.T) {
	for _, category := range []Category{Database, WAL, SHM, Snapshot, Worktree, Other} {
		row, err := statusRow(Observation{ID: "protected", Category: category,
			Size: Measurement{MeasurementID: "protected", Extent: []string{"protected"}}}, "")
		if err != nil || !row.Retain || row.LogicalBytes != nil || row.PhysicalBytes != nil {
			t.Fatalf("status contract changed: %+v, %v", row, err)
		}
	}
	plan, err := BuildPlan(nil)
	if err != nil || plan.ExecutableCandidates != 0 {
		t.Fatal("execution capability introduced")
	}
}
