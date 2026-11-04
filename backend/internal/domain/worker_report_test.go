package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

func TestWorkerReportAuthorshipAndDigest(t *testing.T) {
	sum := sha256.Sum256([]byte("authored"))
	r := WorkerReport{ID: "report", Type: "checkpoint", Version: 1, Identity: WorkItemIdentity{
		ID: "work", AttemptID: "attempt", ProjectID: "project", Repository: "repo", ExecutionBaseSHA: strings.Repeat("a", 40),
		OriginalParentSessionID: "parent", OriginalWorkerSessionID: "worker", OwnerSessionID: "worker", OwnerGeneration: 1, WorktreePath: "/work", Branch: "branch"},
		LeaseID: "lease", LeaseRevision: 1, WorkerSessionID: "worker", NativeSessionID: "native", NativeTurnID: "turn", Content: "authored", Digest: hex.EncodeToString(sum[:])}
	for _, kind := range []string{"checkpoint", "needs_input", "stuck", "done", "failure", "artifact"} {
		valid := r
		valid.Type = kind
		if kind == "artifact" {
			valid.Content = ""
			valid.ArtifactReference = "artifact://result"
		}
		if err := valid.Validate(); err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
	}
	for _, mutate := range []func(*WorkerReport){
		func(r *WorkerReport) { r.ID = "" }, func(r *WorkerReport) { r.Version = 2 }, func(r *WorkerReport) { r.Type = "worker_turn_settled" },
		func(r *WorkerReport) { r.LeaseID = "" }, func(r *WorkerReport) { r.LeaseRevision = 0 }, func(r *WorkerReport) { r.WorkerSessionID = "foreign" },
		func(r *WorkerReport) { r.NativeSessionID = "" }, func(r *WorkerReport) { r.NativeTurnID = "" }, func(r *WorkerReport) { r.Content = "changed" },
		func(r *WorkerReport) { r.Content = "" }, func(r *WorkerReport) { r.ArtifactReference = "artifact://also" }, func(r *WorkerReport) { r.Digest = strings.Repeat("g", 64) },
		func(r *WorkerReport) { r.Digest = strings.ToUpper(r.Digest) }, func(r *WorkerReport) { r.Identity.Repository = "bad\xff" },
		func(r *WorkerReport) { r.OriginalNativeSessionID = "bad\xff" },
	} {
		wrong := r
		mutate(&wrong)
		if err := wrong.Validate(); err == nil {
			t.Fatalf("invalid accepted: %+v", wrong)
		}
	}
}
