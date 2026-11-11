package orchestrationevents

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

type runtimeReportSource struct {
	ids   []string
	err   error
	calls int
}

func (s *runtimeReportSource) ListDueExactParentReports(_ context.Context, _ domain.ProjectID, _ time.Time, after string, limit int) ([]string, error) {
	s.calls++
	var ids []string
	for _, id := range s.ids {
		if id > after && len(ids) < limit {
			ids = append(ids, id)
		}
	}
	return ids, s.err
}

func TestRuntimeReportScanHeldPageCannotStarveNextSingleton(t *testing.T) {
	f := newExactReportFixture()
	source := &runtimeReportSource{}
	// These IDs deliberately mismatch readback and must remain held. The valid
	// singleton after a whole held page must still get its original-parent turn.
	for i := range MaxBatchEvents {
		source.ids = append(source.ids, fmt.Sprintf("%03d", i))
	}
	source.ids = append(source.ids, f.receipt.Report.ID)
	d := f.engine()
	d.Source = source
	if err := d.DispatchProject(t.Context(), "project"); err != nil {
		t.Fatal(err)
	}
	if source.calls != 2 || f.sends != 1 || f.semanticReads != 0 || f.resolutions != 0 {
		t.Fatalf("calls=%d sends=%d semantics=%d", source.calls, f.sends, f.semanticReads)
	}
}

func TestRuntimeReportSourceErrorPreservesLegacyDelivery(t *testing.T) {
	now := time.Now()
	want := errors.New("report source unavailable")
	source := &runtimeReportSource{err: want}
	store := &fakeStore{
		reportsHeld: true,
		sessions: []domain.SessionRecord{{ID: "o", ProjectID: "p", Kind: domain.KindOrchestrator, Mode: domain.SessionModeChat,
			Activity: domain.Activity{State: domain.ActivityIdle}, FirstSignalAt: now}},
		events: []domain.OrchestrationEvent{{ID: "legacy", ProjectID: "p", WorkerID: "w", Kind: domain.OrchestrationWorkerTerminated,
			SourceRevision: "r", EnqueuedAt: now}},
	}
	d := &Dispatcher{Store: store, Reports: &ExactParentReports{Source: source},
		Transport: &fakeTransport{result: Submission{Submitted: true, Acknowledged: true}}}
	if err := d.DispatchProject(t.Context(), "p"); !errors.Is(err, want) || errors.Is(err, ErrReportDeliveryHeld) {
		t.Fatalf("source failure masked: %v", err)
	}
	if len(store.acked) != 1 {
		t.Fatal("report source error starved independent legacy work")
	}
}
