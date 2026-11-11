package orchestrationevents

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// ExactParentReports is an opt-in, singleton worker-report delivery engine.
// The daemon binds it to the existing dispatcher, immutable reader and Chat
// ownership gate. Missing semantic capabilities continue to durably HOLD.
type ExactParentReports struct {
	Store  ports.ExactReportStore
	Source interface {
		ListDueExactParentReports(context.Context, domain.ProjectID, time.Time, string, int) ([]string, error)
	}
	Parents ports.ExactReportParentReader
	Native  ports.ExactReportNative
	Resume  ports.ExactReportResumer
	Now     func() time.Time
}

// DispatchProject consumes a bounded durable scan, never caller-asserted routes.
// Selection is advisory: Deliver re-reads the receipt, parent and lease under
// the existing project gate. Submitted reports await a genuine semantic event.
func (d *ExactParentReports) DispatchProject(ctx context.Context, project domain.ProjectID) error {
	if d.Source == nil {
		return &ports.ReportDeliveryHold{Reason: "report_source_unbound"}
	}
	now := d.now()
	after := ""
	var failures []error
	for {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(failures, err)...)
		}
		ids, err := d.Source.ListDueExactParentReports(ctx, project, now, after, MaxBatchEvents)
		if err != nil {
			return errors.Join(append(failures, err)...)
		}
		for _, id := range ids {
			// Keyset progress also protects against a malformed source looping.
			if id <= after {
				return errors.New("report source did not advance")
			}
			after = id
			if err := d.Deliver(ctx, project, id); err != nil && !errors.Is(err, ErrReportDeliveryHeld) && !errors.Is(err, ErrDispatchHeld) {
				failures = append(failures, err)
			}
		}
		if len(ids) < MaxBatchEvents {
			return errors.Join(failures...)
		}
	}
}

func (d *ExactParentReports) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now().UTC()
}

// Deliver makes at most one native Send with a durable client ID and payload.
// Even admitted transport returns HOLD until separate semantic reconciliation.
// A submitted row awaits semantics without repeatedly waking the parent.
func (d *ExactParentReports) Deliver(ctx context.Context, project domain.ProjectID, reportID string) error {
	return d.run(ctx, project, reportID, false, "")
}

// Reconcile consumes a separately authenticated parent-native semantic event.
// nativeEventID is only a reference for the trusted native reader, never an
// asserted ACK body. This method does not send, wake or discover any parent.
func (d *ExactParentReports) Reconcile(ctx context.Context, project domain.ProjectID, reportID, nativeEventID string) error {
	return d.run(ctx, project, reportID, true, nativeEventID)
}

func (d *ExactParentReports) run(ctx context.Context, project domain.ProjectID, reportID string, semantic bool, nativeEventID string) error {
	if d.Store == nil {
		return &ports.ReportDeliveryHold{Reason: "report_store_unbound"}
	}
	now := d.now()
	return d.Store.WithExactParentReport(ctx, project, reportID, func(w ports.ExactReportWriter) error {
		holdCtx := ctx
		hold := func(reason string, cause error) error {
			return errors.Join(&ports.ReportDeliveryHold{Reason: reason}, cause, w.Hold(holdCtx, now))
		}
		receipt, event, err := w.Read(ctx)
		if err != nil {
			return hold("report_read_unavailable", err)
		}
		if receipt.Report.ID != reportID || receipt.Report.Identity.ProjectID != project || event.ProjectID != project {
			return hold("report_request_mismatch", nil)
		}
		// Do not treat an old, generically ACKed report as semantically resolved.
		// Only this engine's exact durable binding can have that meaning.
		var message ports.ExactReportMessage
		resolved := false
		if event.LeaseToken != "" {
			message, err = ports.ParseExactReportMessage(event.LeaseToken)
			if err != nil {
				return hold("report_binding_unavailable", err)
			}
			if err := message.ValidateSource(receipt, event); err != nil || event.DestinationSessionID != message.Parent.SessionID {
				return hold("report_binding_mismatch", err)
			}
			if event.State == domain.OrchestrationAcknowledged {
				if event.AcknowledgedAt.IsZero() || event.AttemptCount == 0 {
					return hold("report_incomplete_semantic_state", nil)
				}
				if !semantic {
					return nil
				}
				resolved = true
			}
		} else if event.State != domain.OrchestrationPending || event.AttemptCount != 0 ||
			event.DestinationSessionID != "" || !event.SubmittedAt.IsZero() || !event.AcknowledgedAt.IsZero() {
			return hold("report_incomplete_binding", nil)
		}
		if d.Parents == nil || d.Native == nil {
			return hold("report_native_binding_unbound", nil)
		}
		parent, err := d.Parents.ReadReportOriginalParent(ctx, receipt.Report.Identity.ID, receipt.Report.Identity.AttemptID)
		if err != nil {
			return hold("report_original_parent_unavailable", err)
		}
		if event.LeaseToken == "" {
			if semantic {
				return hold("report_never_prepared", nil)
			}
			message = ports.ExactReportMessage{ReportID: receipt.Report.ID, EventID: receipt.EventID,
				SourceRevision: event.SourceRevision, LeaseID: receipt.Report.LeaseID,
				LeaseRevision: receipt.Report.LeaseRevision, Parent: parent}
			// Send only bounded AO identifiers. The parent reads report content via
			// immutable report readback. Report prose is not an instruction source.
			body, err := json.Marshal(message)
			if err != nil {
				return hold("report_payload_invalid", err)
			}
			message.Payload = "[AO worker report] Read the immutable report and explicitly reconcile this exact event within your existing authority. Transport admission is not semantic reconciliation. Binding: " + string(body)
		}
		if message.Parent != parent {
			return hold("report_original_parent_changed", nil)
		}
		if err := message.ValidateSource(receipt, event); err != nil {
			return hold("report_source_mismatch", err)
		}
		if event.State == domain.OrchestrationDeadLetter && !semantic {
			return hold("report_dead_letter", nil)
		}
		// Waiting for an ACK never resumes or wakes a parent. Retry admission is
		// checked before native access and again by the durable singleton CAS.
		if !semantic {
			if event.State == domain.OrchestrationSubmitted || !event.SubmittedAt.IsZero() {
				return hold("report_awaiting_semantics", nil)
			}
			if event.AttemptCount >= chatRetryAttempts || now.Before(event.NextAttemptAt) ||
				(event.State == domain.OrchestrationLeased && now.Before(event.LeaseExpiresAt)) {
				return hold("report_retry_held", nil)
			}
		}
		attemptCtx, cancel := context.WithTimeout(ctx, AttemptTimeout)
		defer cancel()
		// Failure bookkeeping must not outlive the native ownership gate's
		// attempt either, including when a writer is still contended.
		holdCtx = attemptCtx
		called := false
		consume := func(observed ports.ExactReportParent, chat ports.ExactReportChat) error {
			if called {
				return &ports.ReportDeliveryHold{Reason: "report_native_callback_replayed"}
			}
			called = true
			if err := attemptCtx.Err(); err != nil {
				return err
			}
			if observed != message.Parent || chat == nil {
				return &ports.ReportDeliveryHold{Reason: "report_live_parent_mismatch"}
			}
			if semantic {
				if nativeEventID == "" || event.LeaseToken == "" {
					return &ports.ReportDeliveryHold{Reason: "report_semantic_event_missing"}
				}
				ack, err := chat.ReadReportReconciliation(attemptCtx, nativeEventID)
				if err != nil {
					return err
				}
				if ack.NativeEventID != nativeEventID || ack.Message != message || ack.ClientMessageID != message.ClientMessageID() {
					return &ports.ReportDeliveryHold{Reason: "report_semantic_event_mismatch"}
				}
				if resolved {
					return nil // authenticated replay, no second semantic transition
				}
				// A lost transport result may leave a leased/pending row even
				// though the exact native semantic event exists. Startup reclaim may
				// also dead-letter that prepared row. Neither case needs a resend.
				return w.Resolve(attemptCtx, message, d.now)
			}
			if err := w.Prepare(attemptCtx, message, event, d.now); err != nil {
				return err
			}
			admitted, sendErr := chat.SendReport(attemptCtx, message.ClientMessageID(), message.Payload)
			if admitted {
				if err := w.Admit(attemptCtx, message, d.now); err != nil {
					return errors.Join(sendErr, err)
				}
			}
			// Uncertain Send retains its exact durable binding for replay. A
			// native admission, with or without an error, never resolves it.
			return errors.Join(&ports.ReportDeliveryHold{Reason: "report_awaiting_semantics"}, sendErr)
		}
		err = d.Native.WithLiveReportParent(attemptCtx, parent, consume)
		// Only an explicit positive exit before callback admission permits one
		// bounded Resume. Reconcile is read-only with respect to native lifecycle.
		if !semantic && !called && attemptCtx.Err() == nil && errors.Is(err, ports.ErrExactReportParentExited) && d.Resume != nil {
			err = d.Resume.WithResumedReportParent(attemptCtx, parent, consume)
		}
		if err != nil {
			if resolved {
				return errors.Join(&ports.ReportDeliveryHold{Reason: "report_replay_unverified"}, err)
			}
			return hold("report_unresolved", err)
		}
		if !called {
			return hold("report_native_callback_missing", nil)
		}
		return nil
	})
}
