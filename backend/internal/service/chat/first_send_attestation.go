package chat

import (
	"context"
	"errors"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// FirstSendIdentity must come from the trusted AO creation/dispatch owner, with
// the original client ID frozen before dispatch. Never construct it from worker
// claims or expose the binder through a public route. Matching these facts is
// necessary for readback, but does not itself prove an original send attempt.
type FirstSendIdentity struct {
	ProjectID       domain.ProjectID
	SessionID       domain.SessionID
	CreatedAt       time.Time
	Owner           domain.SessionControllerOwner
	ConversationID  string
	BranchID        string
	ClientMessageID string
}

// FirstSendDisposition deliberately has no safe-to-resend/reuse value. The
// existing store cannot attest an immutable first admission across all reads.
type FirstSendDisposition string

const (
	// FirstSendUnknown indicates native first-send delivery is not proven.
	FirstSendUnknown FirstSendDisposition = "UNKNOWN"
	// FirstSendHold indicates the readback cannot safely qualify this identity.
	FirstSendHold FirstSendDisposition = "HOLD"
)

// FirstSendReason identifies a specific fail-closed first-send evidence gap.
type FirstSendReason string

const (
	// FirstSendIdentityMismatch rejects a changed or foreign session identity.
	FirstSendIdentityMismatch FirstSendReason = "identity_mismatch"
	// FirstSendReadUnavailable means the required durable readback failed.
	FirstSendReadUnavailable FirstSendReason = "read_unavailable"
	// FirstSendNoAdmission means no authoritative first admission is available.
	FirstSendNoAdmission FirstSendReason = "no_authoritative_admission"
	// FirstSendSnapshotChanged marks conflicting observations across reads.
	FirstSendSnapshotChanged FirstSendReason = "snapshot_changed"
	// FirstSendAttemptUncertain means the provider call may already have occurred.
	FirstSendAttemptUncertain FirstSendReason = "send_may_have_occurred"
	// FirstSendAdmissionUnbound indicates the original request binding is unavailable.
	FirstSendAdmissionUnbound FirstSendReason = "immutable_first_admission_unbound"
	// FirstSendAcceptanceUnbound means native provider acceptance is unproven.
	FirstSendAcceptanceUnbound FirstSendReason = "native_acceptance_unbound"
)

// FirstSendAttestation is diagnostic readback, not a send grant, provider ACK,
// recovery instruction, or authorization. JournalState is only an observation;
// it does not prove this is the first immutable request or an atomic snapshot.
type FirstSendAttestation struct {
	Disposition  FirstSendDisposition
	Reason       FirstSendReason
	JournalState domain.ConversationSendState
}

// ErrFirstSendIdentity rejects a readback whose original AO-owned identity cannot be proven.
var ErrFirstSendIdentity = errors.New("first-send readback identity mismatch")

// FirstSendReader is fixed to one identity and one service controller. It never
// follows replacement controllers, restores a session, or calls a provider.
type FirstSendReader struct {
	service    *Service
	controller *Controller
	identity   FirstSendIdentity
}

// BindFirstSendReadback is an opt-in, internal read-only seam. It is intentionally
// not wired to CLI/HTTP or automatic recovery. Possession grants only readback
// for this fixed identity; it grants no input lease or dispatch authority.
func (s *Service) BindFirstSendReadback(ctx context.Context, i FirstSendIdentity) (*FirstSendReader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if i.SessionID == "" || i.CreatedAt.IsZero() || i.ConversationID == "" ||
		i.BranchID == "" || i.ClientMessageID == "" || i.Owner.ControllerGeneration == "" ||
		i.Owner.ProviderConversationID == "" || i.Owner.Mode != domain.SessionModeChat || i.Owner.IsTerminated {
		return nil, ErrFirstSendIdentity
	}
	c, err := s.Controller(i.SessionID)
	if err != nil {
		return nil, ErrFirstSendIdentity
	}
	r := &FirstSendReader{service: s, controller: c, identity: i}
	if _, err := r.session(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *FirstSendReader) session(ctx context.Context) (domain.SessionRecord, error) {
	if r == nil || r.service == nil || r.controller == nil {
		return domain.SessionRecord{}, ErrFirstSendIdentity
	}
	s, c, i := r.service, r.controller, r.identity
	current, err := s.Controller(i.SessionID)
	if err != nil || current != c || c.Generation() != i.Owner.ControllerGeneration ||
		c.ProviderConversationID() != i.Owner.ProviderConversationID || c.ConversationID() != i.ConversationID {
		return domain.SessionRecord{}, ErrFirstSendIdentity
	}
	if s.sessions == nil {
		return domain.SessionRecord{}, ErrFirstSendIdentity
	}
	rec, found, err := s.sessions.GetSession(ctx, i.SessionID)
	if err != nil {
		return domain.SessionRecord{}, err
	}
	if !found || rec.ID != i.SessionID || rec.ProjectID != i.ProjectID ||
		!rec.CreatedAt.Equal(i.CreatedAt) || rec.ControllerOwner() != i.Owner {
		return domain.SessionRecord{}, ErrFirstSendIdentity
	}
	return rec, nil
}

// Read never upgrades absence, pending work, a reservation, HTTP success, idle,
// or a local provider reference into definitive delivery evidence. In particular:
//   - pending has no immutable original request/admission-generation binding;
//   - reserved includes crashes before AND after native send;
//   - bound can precede StartDeferredTurn and hence native acceptance.
//
// The missing producer boundary is an immutable original admission binding and
// native acceptance correlated to it, readable with owner/branch/journal facts
// at a common durable snapshot boundary. This lane does not invent that contract.
func (r *FirstSendReader) Read(ctx context.Context) (FirstSendAttestation, error) {
	unknown := FirstSendAttestation{Disposition: FirstSendUnknown, Reason: FirstSendReadUnavailable}
	if err := ctx.Err(); err != nil {
		return unknown, err
	}
	before, err := r.session(ctx)
	if err != nil {
		if errors.Is(err, ErrFirstSendIdentity) {
			return FirstSendAttestation{Disposition: FirstSendHold, Reason: FirstSendIdentityMismatch}, nil
		}
		return unknown, err
	}
	s, i := r.service, r.identity
	conversation, err := s.store.ConversationForSession(ctx, i.SessionID)
	if err != nil {
		return unknown, err
	}
	if conversation.ID != i.ConversationID || conversation.ActiveBranchID != i.BranchID ||
		conversation.SessionID != i.SessionID || conversation.ProjectID != i.ProjectID {
		return FirstSendAttestation{Disposition: FirstSendHold, Reason: FirstSendIdentityMismatch}, nil
	}
	branch, err := s.store.ConversationBranch(ctx, i.ConversationID, i.BranchID)
	if err != nil {
		return unknown, err
	}
	if branch.ID != i.BranchID || branch.ConversationID != i.ConversationID ||
		branch.ProviderConversationID != i.Owner.ProviderConversationID {
		return FirstSendAttestation{Disposition: FirstSendHold, Reason: FirstSendIdentityMismatch}, nil
	}
	message, found, err := s.store.ConversationMessageByClientID(ctx, i.ConversationID, i.ClientMessageID)
	if err != nil {
		return unknown, err
	}
	if !found {
		return FirstSendAttestation{Disposition: FirstSendUnknown, Reason: FirstSendNoAdmission}, nil
	}
	if message.ClientMessageID != i.ClientMessageID || message.ConversationID != i.ConversationID {
		return FirstSendAttestation{Disposition: FirstSendHold, Reason: FirstSendIdentityMismatch}, nil
	}
	attempt, err := s.store.OrdinarySendAttempt(ctx, message.TurnID)
	if err != nil {
		return unknown, err
	}
	if attempt.SessionID != i.SessionID || attempt.ConversationID != i.ConversationID ||
		attempt.BranchID != i.BranchID || attempt.TurnID != message.TurnID || attempt.ClientMessageID != i.ClientMessageID ||
		(attempt.ControllerGeneration != "" && attempt.ControllerGeneration != i.Owner.ControllerGeneration) {
		return FirstSendAttestation{Disposition: FirstSendHold, Reason: FirstSendIdentityMismatch}, nil
	}
	after, err := r.session(ctx)
	if err != nil {
		if errors.Is(err, ErrFirstSendIdentity) {
			return FirstSendAttestation{Disposition: FirstSendHold, Reason: FirstSendIdentityMismatch}, nil
		}
		return unknown, err
	}
	if before.Revision != after.Revision {
		return FirstSendAttestation{Disposition: FirstSendHold, Reason: FirstSendSnapshotChanged}, nil
	}
	result := FirstSendAttestation{Disposition: FirstSendUnknown, JournalState: attempt.State, Reason: FirstSendAttemptUncertain}
	switch attempt.State {
	case domain.ConversationSendPending:
		result.Reason = FirstSendAdmissionUnbound
	case domain.ConversationSendBound:
		result.Reason = FirstSendAcceptanceUnbound
	}
	return result, ctx.Err()
}
