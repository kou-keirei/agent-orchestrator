package reportactor_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/adapters/reportactor"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// This fixture supplies claims only. It is not an authenticated native caller.
func reportClaim() domain.WorkerReport {
	sum := sha256.Sum256([]byte("checkpoint"))
	return domain.WorkerReport{
		ID: "report", Type: "checkpoint", Version: 1,
		Identity: domain.WorkItemIdentity{
			ID: "work", AttemptID: "attempt", ProjectID: "project", Repository: "repo",
			ExecutionBaseSHA: strings.Repeat("a", 40), OriginalParentSessionID: "parent",
			OriginalWorkerSessionID: "worker", OwnerSessionID: "worker", OwnerGeneration: 1,
			WorktreePath: "/work", Branch: "branch",
		},
		LeaseID: "lease", LeaseRevision: 1, WorkerSessionID: "worker",
		NativeSessionID: "native", NativeTurnID: "invented-turn", OriginalNativeSessionID: "native",
		Content: "checkpoint", Digest: hex.EncodeToString(sum[:]),
	}
}

func requireHold(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, domain.ErrWorkerReportActorBindingUnavailable) {
		t.Fatalf("expected actor binding HOLD, got %v", err)
	}
	var hold domain.WorkerReportActorBindingHold
	if !errors.As(err, &hold) || hold.Error() != "HOLD_WORKER_REPORT_ACTOR_BINDING_UNAVAILABLE" {
		t.Fatalf("lost typed source HOLD: %v", err)
	}
}

func TestUnavailableRejectsAuthoredActorClaims(t *testing.T) {
	var verifier ports.WorkerReportActorVerifier = reportactor.Unavailable{}
	for name, mutate := range map[string]func(*domain.WorkerReport){
		"matching-claims-are-not-proof": func(*domain.WorkerReport) {},
		"spoofed-worker": func(r *domain.WorkerReport) {
			r.WorkerSessionID, r.Identity.OwnerSessionID = "other-worker", "other-worker"
		},
		"wrong-work-item":      func(r *domain.WorkerReport) { r.Identity.ID = "other-work" },
		"wrong-attempt":        func(r *domain.WorkerReport) { r.Identity.AttemptID = "other-attempt" },
		"wrong-generation":     func(r *domain.WorkerReport) { r.Identity.OwnerGeneration++ },
		"wrong-lease":          func(r *domain.WorkerReport) { r.LeaseID = "other-lease" },
		"wrong-lease-revision": func(r *domain.WorkerReport) { r.LeaseRevision++ },
		"wrong-native-session": func(r *domain.WorkerReport) { r.NativeSessionID = "other-native" },
		"fabricated-native-turn": func(r *domain.WorkerReport) {
			r.NativeTurnID = "a-second-invented-turn"
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := reportClaim()
			mutate(&r)
			if err := r.Validate(); err != nil {
				t.Fatalf("expected well-formed untrusted claim: %v", err)
			}
			// A JSON round trip does not add caller provenance.
			data, err := json.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			var decoded domain.WorkerReport
			if err := json.Unmarshal(data, &decoded); err != nil {
				t.Fatal(err)
			}
			requireHold(t, verifier.VerifyWorkerReportActor(context.Background(), decoded))
		})
	}
}

func TestUnavailableHoldSurvivesWrappingAndCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, report := range []domain.WorkerReport{reportClaim(), {}} {
		err := reportactor.Unavailable{}.VerifyWorkerReportActor(ctx, report)
		requireHold(t, fmt.Errorf("verify report: %w", err))
	}
}
