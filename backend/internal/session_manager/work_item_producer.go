package sessionmanager

import (
	"context"
	"slices"

	"github.com/google/uuid"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// prepareWorkItemSpawn mints identity once for an already authorized AO command.
// The caller must capture issuer from the original live parent at authorization
// time and supply the explicitly approved frozen base and lease/effect grant.
// It must retain the returned authorization across retries, never prepare another
// identity to retry an uncertain birth. This helper does not authorize a command,
// resolve a coordinator, derive effects from provider permissions, or launch work.
//
// Only the internal native command below consumes this helper; ordinary
// Spawn/DelegateTask DTOs are not accepted authorization sources.
func prepareWorkItemSpawn(project domain.ProjectID, parent domain.SessionID, base string, grant domain.WorkLeaseGrant, issuer ports.WorkItemParentIssuer) (*workItemSpawnAuthorization, error) {
	if _, ok := issuer.(ports.WorkItemSpawnIssuer); !ok {
		return nil, &domain.WorkItemParentHold{Reason: domain.WorkItemParentIssuanceUnbound}
	}
	if err := grant.Validate(); err != nil {
		return nil, err
	}
	a := &workItemSpawnAuthorization{
		work: domain.WorkItemID(uuid.NewString()), attempt: domain.WorkAttemptID(uuid.NewString()),
		project: project, parent: parent, base: base, issuer: issuer, grant: grant,
	}
	a.grant.AllowedEffects = slices.Clone(grant.AllowedEffects)
	// Worker/workspace identity comes only from manager materialization later.
	if err := a.identity("unallocated", ports.WorkspaceInfo{Path: "unallocated", Branch: "unallocated"}).Validate(); err != nil {
		return nil, err
	}
	return a, nil
}

// NativeWorkItemSpec is an already authorized AO command, never a wire DTO.
// The trusted owner supplies the frozen base and explicit lease/effect grant.
// Provider permissions and task text cannot supply these authorities.
// HUMAN_DECISION_REQUIRED: the production command owner is not mounted here.
type NativeWorkItemSpec struct {
	Project domain.ProjectID
	Parent  domain.SessionID
	Base    string
	Harness domain.AgentHarness
	Prompt  string
	Grant   domain.WorkLeaseGrant
}

// NativeWorkItemCommand retains one authorization across launch retries. Its
// fields are private so a caller cannot substitute IDs, parent, base or grant.
// Durable admission, rather than this object, owns crash/retry exclusion.
type NativeWorkItemCommand struct {
	manager       *Manager
	authorization *workItemSpawnAuthorization
	config        ports.SpawnConfig
}

// PrepareNativeWorkItem is for the daemon's trusted producer only. issuer must
// have been captured from OriginalParentIssuer at authorization time. No HTTP,
// CLI, model-output parser or generic spawn route may call this from claims.
func (m *Manager) PrepareNativeWorkItem(spec NativeWorkItemSpec, issuer ports.WorkItemParentIssuer) (*NativeWorkItemCommand, error) {
	a, err := prepareWorkItemSpawn(spec.Project, spec.Parent, spec.Base, spec.Grant, issuer)
	if err != nil {
		return nil, err
	}
	if spec.Harness == "" {
		return nil, ErrMissingHarness
	}
	return &NativeWorkItemCommand{manager: m, authorization: a, config: ports.SpawnConfig{
		ProjectID: spec.Project, ParentSessionID: spec.Parent, ExecutionBaseSHA: spec.Base,
		Kind: domain.KindWorker, RequestedMode: domain.SessionModeChat, Harness: spec.Harness, Prompt: spec.Prompt,
	}}, nil
}

// WorkItemID is readback identity, never a report credential or launch permit.
func (c *NativeWorkItemCommand) WorkItemID() domain.WorkItemID {
	if c == nil || c.authorization == nil {
		return ""
	}
	return c.authorization.work
}

// Spawn materializes the command using its retained authorization across retries.
func (c *NativeWorkItemCommand) Spawn(ctx context.Context) (domain.SessionRecord, error) {
	if c == nil || c.manager == nil || c.authorization == nil {
		return domain.SessionRecord{}, domain.ErrWorkAdmissionHeld
	}
	return c.manager.spawnWorkItem(ctx, c.config, c.authorization)
}
