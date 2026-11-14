package chat_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	chatsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/chat"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

// All tests in this file are tier 1: real Chat/store boundaries with a fixture
// provider. Store reopen is not a native daemon restart or authenticated ACK.
// Native ingress and semantic production remain independent acceptance holds.
type pstackLostResultNative struct{ ports.ExactReportNative }
type pstackLostResultChat struct{ ports.ExactReportChat }

func (n pstackLostResultNative) WithLiveReportParent(ctx context.Context, parent ports.ExactReportParent, fn func(ports.ExactReportParent, ports.ExactReportChat) error) error {
	return n.ExactReportNative.WithLiveReportParent(ctx, parent, func(observed ports.ExactReportParent, chat ports.ExactReportChat) error {
		return fn(observed, pstackLostResultChat{chat})
	})
}

func (c pstackLostResultChat) SendReport(ctx context.Context, id, payload string) (bool, error) {
	admitted, err := c.ExactReportChat.SendReport(ctx, id, payload)
	if err != nil || !admitted {
		return admitted, err
	}
	// Inject only after the genuine durable Chat admission/provider call. The
	// engine now cannot know whether transport succeeded; no ACK is fabricated.
	return false, domain.ErrConversationSendUncertain
}

func TestPStackParentReadbackRestartAndUnknownDelivery(t *testing.T) {
	for _, lost := range []bool{false, true} {
		name := "admitted-awaits-semantics"
		if lost {
			name = "unknown-transport-result"
		}
		t.Run(name, func(t *testing.T) {
			f := newReportParentFixture(t)
			ctx := t.Context()
			identity := f.receipt.Report.Identity
			lease, found, err := f.store.GetWorkItemLease(ctx, identity.ID)
			if err != nil || !found {
				t.Fatalf("lease readback: %v %v", found, err)
			}
			engine := f.engine()
			if lost {
				engine.Native = pstackLostResultNative{f.service}
			}
			err = engine.Deliver(ctx, testProject, f.receipt.Report.ID)
			if !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) || (lost && !errors.Is(err, domain.ErrConversationSendUncertain)) {
				t.Fatalf("transport outcome: %v", err)
			}
			f.settle(t)
			before := f.event(t)
			binding, err := ports.ParseExactReportMessage(before.LeaseToken)
			if err != nil {
				t.Fatal(err)
			}
			if err := binding.ValidateSource(f.receipt, before); err != nil {
				t.Fatal(err)
			}
			wantState := domain.OrchestrationSubmitted
			if lost {
				wantState = domain.OrchestrationLeased
			}
			if before.State != wantState || before.AttemptCount != 1 || before.DestinationSessionID != identity.OriginalParentSessionID ||
				binding.Parent != f.parent || binding.LeaseID != lease.Grant.ID || binding.LeaseRevision != lease.Revision || !before.AcknowledgedAt.IsZero() {
				t.Fatalf("incorrect original-parent binding: %+v", before)
			}
			sent := f.provider.sentMessages()
			if len(sent) != 1 || sent[0].ClientMessageID != binding.ClientMessageID() || sent[0].Text != binding.Payload {
				t.Fatalf("physical send differs from durable binding: %+v", sent)
			}
			// Durable duplicate admission must not make a second provider turn,
			// even when the outbox did not observe the first admission result.
			if err := f.service.WithLiveReportParent(ctx, f.parent, func(_ ports.ExactReportParent, chat ports.ExactReportChat) error {
				admitted, err := chat.SendReport(ctx, binding.ClientMessageID(), binding.Payload)
				if err != nil || !admitted {
					t.Fatalf("duplicate admission: %v %v", admitted, err)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := f.service.Stop(ctx, testSession); err != nil {
				t.Fatal(err)
			}
			if err := f.store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := sqlite.Open(f.dir)
			if err != nil {
				t.Fatal(err)
			}
			f.store = reopened
			f.service = chatsvc.New(chatsvc.Options{Store: reopened, Sessions: reopened, Reader: fullSnapshotReader(reopened)})
			parent, err := reopened.ReadReportOriginalParent(ctx, identity.ID, identity.AttemptID)
			if err != nil || parent != f.parent {
				t.Fatalf("parent changed on reopen: %+v %v", parent, err)
			}
			work, found, err := reopened.GetWorkItem(ctx, identity.ID)
			if err != nil || !found || work.Identity != identity {
				t.Fatalf("work identity changed: %+v %v", work, err)
			}
			afterLease, found, err := reopened.GetWorkItemLease(ctx, identity.ID)
			if err != nil || !found || !reflect.DeepEqual(afterLease, lease) {
				t.Fatalf("lease changed: %+v %v", afterLease, err)
			}
			if err := reopened.WithExactParentReport(ctx, testProject, f.receipt.Report.ID, func(w ports.ExactReportWriter) error {
				receipt, event, err := w.Read(ctx)
				if err != nil || !reflect.DeepEqual(receipt, f.receipt) || event.LeaseToken != before.LeaseToken {
					t.Fatalf("immutable receipt/outbox changed on reopen: %+v %v", receipt, err)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				// Advance beyond the outbox retry lease as well: an unowned
				// parent remains HOLD rather than becoming permission to resume.
				recovery := f.engine()
				recovery.Now = func() time.Time { return before.LeaseExpiresAt.Add(time.Second) }
				if err := recovery.Deliver(ctx, testProject, f.receipt.Report.ID); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
					t.Fatal(err)
				}
				if err := f.engine().Reconcile(ctx, testProject, f.receipt.Report.ID, "worker-authored-ACK-echo"); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
					t.Fatal(err)
				}
			}
			after := f.event(t)
			if after.State != before.State || after.LeaseToken != before.LeaseToken || after.DestinationSessionID != before.DestinationSessionID ||
				after.SourceRevision != before.SourceRevision || after.AttemptCount != 1 || !after.AcknowledgedAt.IsZero() ||
				f.provider.sendCallCount() != 1 || f.starts.Load() != 1 || f.resumes.Load() != 0 {
				t.Fatalf("reopen/unknown result retargeted, resent or ACKed: %+v", after)
			}
		})
	}
}

func TestPStackParentReadbackForeignAndStaleBindings(t *testing.T) {
	for _, change := range []string{"work", "attempt", "parent", "thread", "scope", "controller-generation", "owner-generation", "lease-revision"} {
		t.Run(change, func(t *testing.T) {
			f := newReportParentFixture(t)
			ctx := t.Context()
			parent := f.parent
			switch change {
			case "work":
				parent.WorkItemID = "foreign-work"
			case "attempt":
				parent.AttemptID = "foreign-attempt"
			case "parent":
				parent.SessionID = f.receipt.Report.WorkerSessionID
			case "thread":
				parent.ProviderConversationID = "foreign-thread"
			case "scope":
				parent.ProviderScopeID = "foreign-scope"
			case "controller-generation":
				if err := f.store.ClaimChatControllerGeneration(ctx, testSession, "replacement-generation"); err != nil {
					t.Fatal(err)
				}
			case "owner-generation":
				work, found, err := f.store.GetWorkItem(ctx, f.receipt.Report.Identity.ID)
				if err != nil || !found {
					t.Fatalf("work: %v %v", found, err)
				}
				if _, err := f.store.ClaimWorkItemOwner(ctx, work, testSession); err != nil {
					t.Fatal(err)
				}
			case "lease-revision":
				if _, err := f.store.RenewWorkItemLease(ctx, f.receipt.Report.Identity, f.lease, f.lease.ExpiresAt.Add(time.Minute)); err != nil {
					t.Fatal(err)
				}
			}
			var err error
			if change == "owner-generation" || change == "lease-revision" {
				err = f.engine().Deliver(ctx, testProject, f.receipt.Report.ID)
			} else {
				called := false
				err = f.service.WithLiveReportParent(ctx, parent, func(ports.ExactReportParent, ports.ExactReportChat) error { called = true; return nil })
				if called {
					t.Fatal("foreign/stale parent reached send capability")
				}
			}
			var hold *ports.ReportDeliveryHold
			if !errors.As(err, &hold) || hold.Reason == "" {
				t.Fatalf("missing typed HOLD: %v", err)
			}
			if f.provider.sendCallCount() != 0 || f.starts.Load() != 1 || f.resumes.Load() != 0 {
				t.Fatal("refusal sent or replaced provider work")
			}
			event := f.event(t)
			if event.AttemptCount != 0 || event.LeaseToken != "" || !event.SubmittedAt.IsZero() || !event.AcknowledgedAt.IsZero() {
				t.Fatalf("refusal advanced outbox: %+v", event)
			}
		})
	}
}

func TestPStackParentReadbackSemanticProducerAndResumeHold(t *testing.T) {
	f := newReportParentFixture(t)
	err := f.engine().Deliver(t.Context(), testProject, f.receipt.Report.ID)
	if !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) {
		t.Fatal(err)
	}
	f.settle(t)
	for _, reference := range []string{"ACK", "turn-completed", "foreign-native-event", f.receipt.EventID} {
		err := f.service.WithLiveReportParent(t.Context(), f.parent, func(_ ports.ExactReportParent, chat ports.ExactReportChat) error {
			ack, err := chat.ReadReportReconciliation(t.Context(), reference)
			var hold *ports.ReportDeliveryHold
			if !errors.As(err, &hold) || hold.Reason != "report_parent_semantic_producer_unbound" || ack != (ports.ExactReportReconciliation{}) {
				t.Fatalf("unauthenticated reference produced semantic evidence: %+v %v", ack, err)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	called := false
	err = f.service.WithResumedReportParent(t.Context(), f.parent, func(ports.ExactReportParent, ports.ExactReportChat) error { called = true; return nil })
	var hold *ports.ReportDeliveryHold
	if !errors.As(err, &hold) || hold.Reason != "report_resume_generation_continuity_unavailable" || called {
		t.Fatalf("unproven continuity resumed: %v", err)
	}
	if event := f.event(t); !event.AcknowledgedAt.IsZero() || event.State != domain.OrchestrationSubmitted || f.provider.sendCallCount() != 1 || f.resumes.Load() != 0 {
		t.Fatalf("semantic/continuity HOLD changed delivery: %+v", event)
	}
}
