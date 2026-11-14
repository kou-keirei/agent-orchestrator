package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/orchestrationevents"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	chatsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/chat"
)

type parentSemanticStoreFixture struct {
	ports.ExactReportWriter // only Read may run during binding
	receipt                 domain.WorkerReportReceipt
	event                   domain.OrchestrationEvent
	err                     error
}

func (s *parentSemanticStoreFixture) WithExactParentReport(ctx context.Context, _ domain.ProjectID, _ string, fn func(ports.ExactReportWriter) error) error {
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > orchestrationevents.AttemptTimeout {
		return errors.New("owner binding escaped attempt deadline")
	}
	return fn(s)
}

func (s *parentSemanticStoreFixture) Read(context.Context) (domain.WorkerReportReceipt, domain.OrchestrationEvent, error) {
	return s.receipt, s.event, s.err
}

func (*parentSemanticStoreFixture) ReadReportOriginalParent(context.Context, domain.WorkItemID, domain.WorkAttemptID) (ports.ExactReportParent, error) {
	panic("binding must not discover another parent")
}

func TestParentSemanticACKOwnerBindingRejectsInvalidOutbox(t *testing.T) {
	for _, mode := range []string{"valid-unbound", "read-error", "unprepared", "foreign-source", "foreign-report", "foreign-project", "foreign-destination", "never-attempted"} {
		t.Run(mode, func(t *testing.T) {
			sum := sha256.Sum256([]byte("report content"))
			r := domain.WorkerReport{ID: "report", Type: "checkpoint", Version: 1,
				Identity: domain.WorkItemIdentity{ID: "work", AttemptID: "attempt", ProjectID: "project", Repository: "repo",
					ExecutionBaseSHA: strings.Repeat("a", 40), OriginalParentSessionID: "parent", OriginalWorkerSessionID: "worker",
					OwnerSessionID: "worker", OwnerGeneration: 1, WorktreePath: t.TempDir(), Branch: "branch"},
				LeaseID: "lease", LeaseRevision: 1, WorkerSessionID: "worker", NativeSessionID: "worker-native", NativeTurnID: "worker-turn",
				Content: "report content", Digest: hex.EncodeToString(sum[:])}
			b, err := json.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			sum = sha256.Sum256(b)
			m := ports.ExactReportMessage{ReportID: r.ID, EventID: "event", SourceRevision: "worker-report:v1:" + hex.EncodeToString(sum[:]),
				LeaseID: r.LeaseID, LeaseRevision: r.LeaseRevision, Payload: "durable payload",
				Parent: ports.ExactReportParent{WorkItemID: r.Identity.ID, AttemptID: r.Identity.AttemptID, SessionID: "parent",
					NativeSessionID: "parent-native", ControllerGeneration: "generation", ProviderConversationID: "parent-native", ProviderScopeID: "scope"}}
			s := &parentSemanticStoreFixture{receipt: domain.WorkerReportReceipt{Report: r, EventID: m.EventID},
				event: domain.OrchestrationEvent{ID: m.EventID, Kind: domain.OrchestrationWorkerReport, ProjectID: r.Identity.ProjectID,
					WorkerID: r.WorkerSessionID, SourceRevision: m.SourceRevision, LeaseToken: m.Token(), AttemptCount: 1, DestinationSessionID: m.Parent.SessionID}}
			project, reportID := r.Identity.ProjectID, r.ID
			switch mode {
			case "read-error":
				s.err = errors.New("read unavailable")
			case "unprepared":
				s.event.LeaseToken = ""
			case "foreign-source":
				s.event.SourceRevision = "foreign"
			case "foreign-report":
				reportID = "foreign"
			case "foreign-project":
				project = "foreign"
			case "foreign-destination":
				s.event.DestinationSessionID = "foreign"
			case "never-attempted":
				s.event.AttemptCount = 0
			}
			err = bindParentSemanticACK(t.Context(), s, s, &chatsvc.Service{}, project, reportID)
			if mode == "read-error" {
				if !errors.Is(err, s.err) {
					t.Fatal(err)
				}
			} else if !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
				t.Fatal(err)
			}
			if mode == "valid-unbound" && !strings.Contains(err.Error(), "report_native_binding_unbound") {
				t.Fatal(err)
			}
		})
	}
}

func TestParentSemanticACKOwnerBindingRequiresDependencies(t *testing.T) {
	if err := bindParentSemanticACK(t.Context(), nil, nil, nil, "project", "report"); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
		t.Fatal(err)
	}
}
