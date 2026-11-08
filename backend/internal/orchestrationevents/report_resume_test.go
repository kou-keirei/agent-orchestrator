package orchestrationevents

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// A disposable contract fixture, not evidence that a provider can Resume.
type reportResumeFixture struct {
	f       *exactReportFixture
	calls   int
	err     error
	missing bool
}

func (r *reportResumeFixture) WithResumedReportParent(_ context.Context, expected ports.ExactReportParent, fn func(ports.ExactReportParent, ports.ExactReportChat) error) error {
	r.calls++
	if expected != r.f.parent {
		return errors.New("fixture wrong resume target")
	}
	if r.err != nil || r.missing {
		return r.err
	}
	return fn(r.f.observed, r.f)
}

func TestReportResumeOnlyAfterPositiveExitAndExactHistory(t *testing.T) {
	for _, name := range []string{"exact", "unbound", "busy", "probe_failed", "history_mismatch", "foreign_owner", "foreign_scope", "missing_callback", "submitted", "stale_lease", "not_due", "reconcile"} {
		t.Run(name, func(t *testing.T) {
			f := newExactReportFixture()
			f.liveErr = ports.ErrExactReportParentExited
			r := &reportResumeFixture{f: f}
			d := f.engine()
			d.Resume = r
			switch name {
			case "unbound":
				d.Resume = nil
			case "busy":
				f.liveErr = &ports.ReportDeliveryHold{Reason: "busy"}
			case "probe_failed":
				f.liveErr = errors.New("unknown controller")
			case "history_mismatch":
				r.err = errors.New("history mismatch")
			case "foreign_owner":
				f.observed.ControllerGeneration = "replacement"
			case "foreign_scope":
				f.observed.ProviderScopeID = "foreign"
			case "missing_callback":
				r.missing = true
			case "submitted", "stale_lease", "reconcile":
				f.liveErr = nil
				_ = d.Deliver(t.Context(), "project", "report")
				f.liveErr = ports.ErrExactReportParentExited
				if name == "stale_lease" {
					f.event.State, f.event.SubmittedAt = domain.OrchestrationLeased, time.Time{}
				}
			case "not_due":
				f.event.NextAttemptAt = f.now.Add(time.Minute)
			}
			beforeSends, beforePrepares := f.sends, f.prepares
			var err error
			if name == "reconcile" {
				err = d.Reconcile(t.Context(), "project", "report", "event-reference")
			} else {
				err = d.Deliver(t.Context(), "project", "report")
			}
			wantResumes, wantSends := 0, 0
			switch name {
			case "exact":
				wantResumes, wantSends = 1, 1
			case "history_mismatch", "foreign_owner", "foreign_scope", "missing_callback":
				wantResumes = 1
			}
			if !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) || r.calls != wantResumes ||
				f.sends != beforeSends+wantSends || f.prepares != beforePrepares+wantSends ||
				f.resolutions != 0 || !f.event.AcknowledgedAt.IsZero() {
				t.Fatalf("err=%v resumes=%d fixture=%+v", err, r.calls, f)
			}
			if name == "exact" {
				// Admission is still not semantics; a new Deliver never resumes again.
				_ = d.Deliver(t.Context(), "project", "report")
				if r.calls != 1 || f.sends != 1 || f.event.State != domain.OrchestrationSubmitted {
					t.Fatalf("repeat wake: resumes=%d fixture=%+v", r.calls, f)
				}
			}
		})
	}
}

func TestReportRequestIdentityPrecedesNativeAccess(t *testing.T) {
	for _, name := range []string{"project", "report"} {
		f := newExactReportFixture()
		project, reportID := domain.ProjectID("project"), "report"
		if name == "project" {
			project = "foreign"
		} else {
			reportID = "foreign"
		}
		if err := f.engine().Deliver(t.Context(), project, reportID); !errors.Is(err, ports.ErrWorkerReportDeliveryHeld) ||
			f.parentReads != 0 || f.liveCalls != 0 || f.prepares != 0 || f.sends != 0 {
			t.Fatalf("%s request crossed boundary: err=%v fixture=%+v", name, err, f)
		}
	}
}

func TestReportSemanticReplayAuthenticatesWithoutSecondTransition(t *testing.T) {
	f := newExactReportFixture()
	d := f.engine()
	_ = d.Deliver(t.Context(), "project", "report")
	f.semanticEvent(t)
	if err := d.Reconcile(t.Context(), "project", "report", f.ack.NativeEventID); err != nil {
		t.Fatal(err)
	}
	acknowledged := f.event
	for _, foreign := range []bool{true, false} {
		f.semanticErr = nil
		if foreign {
			f.semanticErr = errors.New("unauthenticated replay")
		}
		err := d.Reconcile(t.Context(), "project", "report", f.ack.NativeEventID)
		if (foreign && !errors.Is(err, ports.ErrWorkerReportDeliveryHeld)) || (!foreign && err != nil) ||
			f.event != acknowledged || f.resolutions != 1 || f.sends != 1 {
			t.Fatalf("foreign=%v err=%v fixture=%+v", foreign, err, f)
		}
	}
}
