package sessionmanager

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// workItemSpawnAuthorization is an internal authorization snapshot, never an
// ingress DTO. BLOCKS_PRODUCTION: selecting the AO producer authorized to mint
// work/attempt IDs and approve the frozen base requires a product decision.
// There is deliberately no exported constructor, dependency or production caller.
// That producer must capture parent at authorization time using Chat's
// OriginalParentIssuer; it must never resolve today's coordinator at launch.
// Owner and generation are NOT inputs: first issuance belongs to the new worker
// at generation one. Native worker identity is unknown before provider birth.
type workItemSpawnAuthorization struct {
	work    domain.WorkItemID
	attempt domain.WorkAttemptID
	project domain.ProjectID
	parent  domain.SessionID
	base    string
	issuer  ports.WorkItemParentIssuer
	grant   domain.WorkLeaseGrant
}

type workItemSpawnStore interface {
	ports.WorkItemParentReader
	ports.WorkItemLeaseStore
	ports.WorkItemAdmissionStore
}

// spawnWorkItem consumes only a trusted AO authorization. A fresh Spawn always
// allocates a fresh worker; immutable WorkItem issuance rejects its reuse for an
// already issued attempt. A durable lease additionally rejects retries BEFORE
// allocating a seed. Neither exact reservation retries nor expiry permit launch.
func (m *Manager) spawnWorkItem(ctx context.Context, cfg ports.SpawnConfig, a *workItemSpawnAuthorization) (domain.SessionRecord, int, int, error) {
	if a == nil || a.issuer == nil {
		return domain.SessionRecord{}, 0, 0, &domain.WorkItemParentHold{Reason: domain.WorkItemParentIssuanceUnbound}
	}
	snapshot := *a
	snapshot.grant.AllowedEffects = slices.Clone(a.grant.AllowedEffects)
	a = &snapshot
	if cfg.Kind != domain.KindWorker || cfg.ProjectID != a.project || cfg.ParentSessionID != a.parent ||
		(cfg.ExecutionBaseSHA != "" && cfg.ExecutionBaseSHA != a.base) {
		return domain.SessionRecord{}, 0, 0, domain.ErrWorkIdentityMismatch
	}
	// Validate the authorization's shape before allocating anything; all worker
	// identity fields below are replaced with AO's materialized facts at issuance.
	i := a.identity("unallocated", ports.WorkspaceInfo{Path: "unallocated", Branch: "unallocated"})
	if err := i.Validate(); err != nil {
		return domain.SessionRecord{}, 0, 0, err
	}
	if err := a.grant.Validate(); err != nil {
		return domain.SessionRecord{}, 0, 0, err
	}
	s, ok := m.store.(workItemSpawnStore)
	if !ok {
		return domain.SessionRecord{}, 0, 0, domain.ErrWorkAdmissionHeld
	}
	if _, found, err := s.GetWorkItemLease(ctx, a.work); err != nil || found {
		return domain.SessionRecord{}, 0, 0, errors.Join(domain.ErrWorkAdmissionHeld, err)
	}
	cfg.ExecutionBaseSHA = a.base
	return m.spawn(ctx, cfg, a)
}

func (a *workItemSpawnAuthorization) identity(worker domain.SessionID, ws ports.WorkspaceInfo) domain.WorkItemIdentity {
	return domain.WorkItemIdentity{ID: a.work, AttemptID: a.attempt, ProjectID: a.project,
		Repository: domain.RootWorkspaceRepoName, ExecutionBaseSHA: a.base,
		OriginalParentSessionID: a.parent, OriginalWorkerSessionID: worker,
		OwnerSessionID: worker, OwnerGeneration: 1, WorktreePath: ws.Path, Branch: ws.Branch}
}

// admitWorkItemSpawn runs once, immediately before StartChat/runtime.Create.
// After entering this boundary, errors preserve the seed/worktree and any
// committed work/lease/reservation for explicit reconciliation. No release,
// supersession, provider permission change, send or wake is performed here.
func (m *Manager) admitWorkItemSpawn(ctx context.Context, a *workItemSpawnAuthorization, rec domain.SessionRecord, ws ports.WorkspaceInfo) error {
	if a == nil {
		return nil
	}
	s, ok := m.store.(workItemSpawnStore)
	if !ok || ws.SessionID != rec.ID || ws.ProjectID != a.project || ws.ExecutionBaseSHA != a.base {
		return domain.ErrWorkAdmissionHeld
	}
	i := a.identity(rec.ID, ws)
	if err := i.Validate(); err != nil {
		return err
	}
	if err := m.store.UpsertSessionWorktree(ctx, domain.SessionWorktreeRecord{
		SessionID: rec.ID, RepoName: i.Repository, WorktreePath: ws.Path,
		Branch: ws.Branch, BaseSHA: a.base, BaseRef: ws.BaseRef, State: "active",
	}); err != nil {
		return err
	}
	issuer, ok := a.issuer.(ports.WorkItemSpawnIssuer)
	if !ok {
		return &domain.WorkItemParentHold{Reason: domain.WorkItemParentIssuanceUnbound}
	}
	receipt, err := issuer.IssueWorkItemSpawn(ctx, i, "", a.grant)
	if err != nil {
		return err
	}
	target, lease := receipt.Parent, receipt.Lease
	if err := target.ValidateFor(i); err != nil {
		return err
	}
	readback, err := s.GetWorkItemOriginalParent(ctx, i.ID, i.AttemptID)
	if err != nil || readback != target {
		return errors.Join(domain.ErrWorkItemParentHeld, err)
	}
	current, found, err := s.GetWorkItemLease(ctx, i.ID)
	if err != nil || !found || current.WorkItemID != lease.WorkItemID || current.OwnerGeneration != lease.OwnerGeneration ||
		current.Revision != lease.Revision || current.State != lease.State || !current.ExpiresAt.Equal(lease.ExpiresAt) ||
		!sameSpawnWorkGrant(current.Grant, lease.Grant) || lease.State != domain.WorkLeaseActive ||
		lease.WorkItemID != i.ID || lease.OwnerGeneration != i.OwnerGeneration || !sameSpawnWorkGrant(lease.Grant, a.grant) {
		return errors.Join(domain.ErrWorkLeaseConflict, err)
	}
	// Revalidate the captured live controller, never acquire a replacement.
	if latest, err := a.issuer.IssueWorkItemFromOriginalParent(ctx, i, ""); err != nil || latest != target {
		return errors.Join(domain.ErrWorkItemParentHeld, err)
	}
	wanted := domain.WorkItemAdmission{WorkItemID: i.ID, AttemptID: i.AttemptID,
		OwnerSessionID: i.OwnerSessionID, OwnerGeneration: i.OwnerGeneration,
		LeaseID: lease.Grant.ID, LeaseRevision: lease.Revision, State: domain.WorkAdmissionReserved}
	if receipt.Admission != wanted {
		return domain.ErrWorkAdmissionHeld
	}
	// Reserve performs fresh transactional owner/lease checks and durable
	// readback of the atomic receipt. It cannot grant a second launch.
	// It is the final store operation, not a cached permission token.
	got, err := s.ReserveWorkItemAdmission(ctx, i, lease)
	if err != nil || got != wanted {
		return errors.Join(domain.ErrWorkAdmissionHeld, err)
	}
	return ctx.Err()
}

func sameSpawnWorkGrant(a, b domain.WorkLeaseGrant) bool {
	return a.ID == b.ID && a.Duration == b.Duration && slices.Equal(a.AllowedEffects, b.AllowedEffects)
}

func workItemSpawnHeld(id domain.SessionID, err error) error {
	return fmt.Errorf("work item spawn %s: reconciliation required: %w", id, errors.Join(domain.ErrWorkAdmissionHeld, err))
}
