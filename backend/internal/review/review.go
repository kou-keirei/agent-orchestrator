// Package review holds the core code-review logic: triggering a reviewer over a
// worker's worktree, recording review runs, and accepting submitted results.
//
// It is independent of any transport. The daemon's HTTP service
// (internal/service/review) is a thin boundary over this engine today, and the
// same engine can back an in-process CLI trigger later without going through the
// API. Transport-specific concerns (DTOs, error→status mapping) stay in the
// service/controller layers; the orchestration and run-id generation live here.
package review

import (
	stdctx "context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// ErrInvalid and ErrNotFound let the transport layer map failures to 422/404.
var (
	ErrInvalid  = errors.New("review: invalid input")
	ErrNotFound = errors.New("review: not found")
)

// Store is the persistence surface the engine needs. *sqlite.Store satisfies it
// in production; tests use a fake.
type Store interface {
	UpsertReview(ctx stdctx.Context, r domain.Review) error
	SetSessionReviewerConfig(ctx stdctx.Context, id domain.SessionID, harness domain.ReviewerHarness, config domain.AgentConfig, updatedAt time.Time) (bool, error)
	GetReviewBySession(ctx stdctx.Context, id domain.SessionID) (domain.Review, bool, error)
	ClearReviewerHandle(ctx stdctx.Context, id domain.SessionID) error
	GetReviewBySessionAndHarness(ctx stdctx.Context, id domain.SessionID, harness domain.ReviewerHarness) (domain.Review, bool, error)
	ListReviewsBySession(ctx stdctx.Context, id domain.SessionID) ([]domain.Review, error)
	ClearReviewerHandleByHarness(ctx stdctx.Context, id domain.SessionID, harness domain.ReviewerHarness) error
	InsertReviewRun(ctx stdctx.Context, r domain.ReviewRun) error
	UpdateReviewRunResult(ctx stdctx.Context, id string, status domain.ReviewRunStatus, verdict domain.ReviewVerdict, body, githubReviewID string, autoInjectReview bool) (bool, error)
	UpdateReviewAgentSessionID(ctx stdctx.Context, id, agentSessionID string) (bool, error)
	SupersedeStaleRunningReviewRuns(ctx stdctx.Context, sessionID domain.SessionID, prURL, targetSHA, body string) (int64, error)
	CancelRunningReviewRunsBySession(ctx stdctx.Context, sessionID domain.SessionID, body string) (int64, error)
	CancelRunningReviewRunsBySessionAndHarness(ctx stdctx.Context, sessionID domain.SessionID, harness domain.ReviewerHarness, body string) (int64, error)
	GetReviewRun(ctx stdctx.Context, id string) (domain.ReviewRun, bool, error)
	GetReviewRunBySessionPRAndSHA(ctx stdctx.Context, id domain.SessionID, prURL, targetSHA string) (domain.ReviewRun, bool, error)
	GetReviewRunBySessionPRSHAAndHarness(ctx stdctx.Context, id domain.SessionID, prURL, targetSHA string, harness domain.ReviewerHarness) (domain.ReviewRun, bool, error)
	ListReviewRunsBySession(ctx stdctx.Context, id domain.SessionID) ([]domain.ReviewRun, error)
	ListRunningReviewRunsBySession(ctx stdctx.Context, id domain.SessionID) ([]domain.ReviewRun, error)
}

// ReviewRunNativeEvidenceAppender is the optional persistence capability used
// for provider-native reviewer hook facts. Keeping it optional preserves the
// transport-neutral test/store boundary while production SQLite can enforce
// launch-generation and append-only semantics in one statement.
type ReviewRunNativeEvidenceAppender interface {
	AppendReviewRunNativeEvidence(ctx stdctx.Context, id, launchID, runSessionID, nativeSessionID, firstTurnID, completionID string, observedAt time.Time) (bool, error)
}

// ReviewerCleanupObligationStore is the optional durable surface for a runtime
// handle that may have escaped before a ReviewRun could be attributed. Keeping
// it optional preserves small transport-neutral fakes; production SQLite must
// implement it so this cleanup identity cannot exist only in an error value.
type ReviewerCleanupObligationStore interface {
	InsertReviewerCleanupObligation(stdctx.Context, domain.ReviewerCleanupObligation) error
	ListUnresolvedReviewerCleanupObligations(stdctx.Context, domain.SessionID) ([]domain.ReviewerCleanupObligation, error)
	ResolveReviewerCleanupObligation(stdctx.Context, string, time.Time) (bool, error)
}

// ReviewerCleanupMarkerStore clears the exact ReviewRun cleanup marker after
// the runtime destroy has succeeded. It is optional so transport-neutral fakes
// can remain small; production SQLite implements the compare-and-clear.
type ReviewerCleanupMarkerStore interface {
	ResolveReviewRunCleanupMarker(stdctx.Context, string, string, string) (bool, error)
}

// ReviewerReplacementAcceptor is the optional durable transition used when a
// live reviewer generation is replaced. Implementations must verify the exact
// incumbent and predecessor snapshots, then commit the replacement review row
// and predecessor terminalization atomically with any retired-host cleanup
// obligation. Keeping this capability optional
// preserves the transport-neutral test/store boundary; production SQLite
// provides the transaction.
type ReviewerReplacementAcceptor interface {
	AcceptReviewerReplacement(ctx stdctx.Context, incumbent, replacement domain.Review, predecessors []domain.ReviewRun, clearAgentSession bool, retiredCleanup domain.ReviewerCleanupObligation) error
}

const reviewerCleanupPendingPrefix = "[ao reviewer cleanup pending handle: "

func reviewerCleanupPendingMarker(handleID string) string {
	return reviewerCleanupPendingPrefix + strings.TrimSpace(handleID) + "]"
}

func reviewerCleanupPendingMarkerForGeneration(handleID, launchID string) string {
	marker := reviewerCleanupPendingPrefix + strings.TrimSpace(handleID)
	if launchID != "" {
		marker += " generation: " + strings.TrimSpace(launchID)
	}
	return marker + "]"
}

func reviewerCleanupPendingIdentity(body string) (marker, handleID, launchID string, ok bool) {
	for {
		start := strings.Index(body, reviewerCleanupPendingPrefix)
		if start < 0 {
			return
		}
		value := body[start:]
		end := strings.IndexByte(value, ']')
		if end < 0 {
			return
		}
		// A malformed earlier marker must not consume a later valid marker.
		if next := strings.Index(value[len(reviewerCleanupPendingPrefix):], reviewerCleanupPendingPrefix); next >= 0 && next+len(reviewerCleanupPendingPrefix) < end {
			body = value[next+len(reviewerCleanupPendingPrefix):]
			continue
		}
		candidate := value[:end+1]
		body = value[end+1:]
		content := strings.TrimSpace(candidate[len(reviewerCleanupPendingPrefix) : len(candidate)-1])
		candidateHandle, candidateLaunch := content, ""
		const generationPrefix = " generation:"
		if index := strings.LastIndex(content, generationPrefix); index >= 0 {
			candidateHandle = strings.TrimSpace(content[:index])
			candidateLaunch = strings.TrimSpace(content[index+len(generationPrefix):])
		}
		if candidateHandle == "" {
			continue
		}
		// Prefer exact generation evidence over an earlier unbound marker.
		if candidateLaunch != "" {
			return candidate, candidateHandle, candidateLaunch, true
		}
		if !ok {
			marker, handleID, launchID, ok = candidate, candidateHandle, candidateLaunch, true
		}
	}
}

func reviewerCleanupPendingHandle(body string) string {
	_, handleID, _, _ := reviewerCleanupPendingIdentity(body)
	return handleID
}

func reviewerCleanupPendingGeneration(body string) string {
	_, _, launchID, _ := reviewerCleanupPendingIdentity(body)
	return launchID
}

// Sessions resolves the worker session under review.
type Sessions interface {
	GetSession(ctx stdctx.Context, id domain.SessionID) (domain.SessionRecord, bool, error)
}

// PRs resolves the PR a worker owns.
type PRs interface {
	ListPRsBySession(ctx stdctx.Context, id domain.SessionID) ([]domain.PullRequest, error)
}

// Projects resolves the per-project reviewer config.
type Projects interface {
	GetProject(ctx stdctx.Context, id string) (domain.ProjectRecord, bool, error)
}

// Deps wires the engine.
type Deps struct {
	Store    Store
	Sessions Sessions
	PRs      PRs
	Projects Projects
	Launcher Launcher
	// Workspace is the AO-owned read-only observation surface. Production wiring
	// supplies the workspace router; nil is a deliberate fail-closed error when
	// a new run would otherwise lack a target binding.
	Workspace ports.WorkspaceObserver

	// Clock and NewID are injectable for deterministic tests.
	Clock func() time.Time
	NewID func() string
}

// Engine is the core code-review engine.
type Engine struct {
	store     Store
	sessions  Sessions
	prs       PRs
	projects  Projects
	launcher  Launcher
	workspace ports.WorkspaceObserver
	clock     func() time.Time
	newID     func() string

	// triggerMu guards triggerLocks; triggerLocks holds one mutex per worker
	// session so concurrent Trigger calls for the same worker serialise (see
	// lockWorker). Distinct workers never contend.
	triggerMu    sync.Mutex
	triggerLocks map[domain.SessionID]*sync.Mutex
}

const autoReviewFailedRetryLimit = 3

// New wires an Engine from its dependencies, defaulting the clock and id source.
func New(d Deps) *Engine {
	clock := d.Clock
	if clock == nil {
		clock = func() time.Time { return time.Now().UTC() }
	}
	newID := d.NewID
	if newID == nil {
		newID = uuid.NewString
	}
	return &Engine{
		store:        d.Store,
		sessions:     d.Sessions,
		prs:          d.PRs,
		projects:     d.Projects,
		launcher:     d.Launcher,
		workspace:    d.Workspace,
		clock:        clock,
		newID:        newID,
		triggerLocks: make(map[domain.SessionID]*sync.Mutex),
	}
}

// lockWorker serialises Trigger calls for a single worker session and returns
// the unlock func. Without it, two concurrent triggers for the same worker can
// both pass the per-commit idempotency check and each spawn a reviewer against
// the same deterministic handle, leaving two running runs for one commit (#242).
//
// The per-worker mutex is created on first use and kept for the lifetime of the
// engine; the entry is a single pointer, so the unbounded-by-session-count map
// is a negligible, bounded-in-practice cost.
func (e *Engine) lockWorker(id domain.SessionID) func() {
	e.triggerMu.Lock()
	mu, ok := e.triggerLocks[id]
	if !ok {
		mu = &sync.Mutex{}
		e.triggerLocks[id] = mu
	}
	e.triggerMu.Unlock()
	mu.Lock()
	return mu.Unlock
}

// TriggerResult is the outcome of a trigger: the (new or existing) run, the live
// reviewer pane's handle so the UI can attach its terminal, and whether a new
// pass was started (false when an existing run for the same commit was reused).
type TriggerResult struct {
	Run              domain.ReviewRun
	ReviewerHandleID string
	Created          bool
	Reviews          []PRReviewState
	Runs             []domain.ReviewRun
	CreatedRuns      []domain.ReviewRun
	// SkipReason is set only for a normal automatic-trigger policy race, such
	// as the worker becoming active after the coordinator's initial read.
	SkipReason string
}

// SessionReviews is a worker's review state: the live reviewer handle plus its
// recorded passes, newest first.
type SessionReviews struct {
	ReviewerHandleID      string
	ReviewerHarness       domain.ReviewerHarness
	ReviewerActivityState domain.ActivityState
	Runs                  []domain.ReviewRun
	Reviews               []PRReviewState
}

// CancelResult is the review state after a reviewer pane cancellation.
type CancelResult struct {
	ReviewerHandleID string
	Reviews          []PRReviewState
	CancelledRuns    []domain.ReviewRun
}

// TerminateResult reports the reviewer pane hard-teardown performed because
// the owning worker session is leaving its live lifecycle.
type TerminateResult struct {
	ReviewerHandleID string
	CancelledRuns    []domain.ReviewRun
}

// RestoreReviewerResult reports an idle reviewer pane restored alongside its
// worker session.
type RestoreReviewerResult struct {
	ReviewerHandleID string
	Restored         bool
}

// Trigger starts reviews for every PR on the worker session that needs review.
// It reuses running/up-to-date runs, retries failed/current changes-requested
// heads, and uses one reviewer pane for every new run in the batch. Automatic
// retries against the same PR head stop after three failed auto-review runs.
//
// An empty override keeps the project's configured reviewer. A known one runs
// this pass under it without editing project config, so picking a reviewer for
// one session cannot change what any other session in the project runs. The
// harness-change path below already handles the swap by respawning the pane.
func (e *Engine) Trigger(ctx stdctx.Context, workerID domain.SessionID, override domain.ReviewerHarness, config domain.AgentConfig) (TriggerResult, error) {
	return e.TriggerWithSource(ctx, workerID, override, config, domain.ReviewTriggerManual)
}

// TriggerWithSource starts a review and records who initiated the pass.
func (e *Engine) TriggerWithSource(ctx stdctx.Context, workerID domain.SessionID, override domain.ReviewerHarness, overrideConfig domain.AgentConfig, source domain.ReviewTriggerSource) (TriggerResult, error) {
	if workerID == "" {
		return TriggerResult{}, fmt.Errorf("%w: worker session id is required", ErrInvalid)
	}
	if override != "" && !override.IsKnown() {
		return TriggerResult{}, fmt.Errorf("%w: unknown reviewer harness %q", ErrInvalid, override)
	}
	if source != domain.ReviewTriggerManual && source != domain.ReviewTriggerAuto {
		return TriggerResult{}, fmt.Errorf("%w: unknown review trigger source %q", ErrInvalid, source)
	}

	// Serialise concurrent triggers for this worker so the idempotency check
	// below (and the reviewer spawn that follows it) can't be raced into a
	// double-spawn. Held across the spawn deliberately: the loser then re-reads
	// the freshly-recorded run and short-circuits to Created:false.
	unlock := e.lockWorker(workerID)
	defer unlock()

	worker, ok, err := e.sessions.GetSession(ctx, workerID)
	if err != nil {
		return TriggerResult{}, err
	}
	if !ok {
		return TriggerResult{}, fmt.Errorf("%w: worker session %q", ErrNotFound, workerID)
	}
	if source == domain.ReviewTriggerAuto {
		if reason := autoReviewSessionReason(worker, e.clock()); reason != "" {
			return TriggerResult{SkipReason: reason}, nil
		}
	}
	if worker.IsTerminated {
		return TriggerResult{}, fmt.Errorf("%w: worker session %q is terminated", ErrInvalid, workerID)
	}
	if worker.Metadata.WorkspacePath == "" {
		return TriggerResult{}, fmt.Errorf("%w: worker session %q has no workspace to review", ErrInvalid, workerID)
	}

	prs, err := e.prs.ListPRsBySession(ctx, workerID)
	if err != nil {
		return TriggerResult{}, err
	}
	if len(prs) == 0 {
		return TriggerResult{}, fmt.Errorf("%w: worker %q has no PR to review", ErrInvalid, workerID)
	}
	runs, err := e.store.ListReviewRunsBySession(ctx, workerID)
	if err != nil {
		return TriggerResult{}, err
	}
	if err := e.retryPendingReviewerCleanup(ctx, workerID, runs); err != nil {
		return TriggerResult{}, err
	}

	if err := overrideConfig.Validate(); err != nil {
		return TriggerResult{}, fmt.Errorf("%w: reviewer config: %w", ErrInvalid, err)
	}

	harness, config, err := e.reviewerSelection(ctx, worker)
	if err != nil {
		return TriggerResult{}, err
	}
	resolvedHarness := harness
	resolvedConfig := config
	hasHarnessOverride := override != ""
	hasConfigOverride := false
	if hasHarnessOverride {
		harness = override
		if override == resolvedHarness {
			config = mergeReviewerAgentConfig(resolvedConfig, overrideConfig)
			hasConfigOverride = config != resolvedConfig
		} else if !overrideConfig.IsZero() {
			config = mergeReviewerAgentConfig(domain.AgentConfig{}, overrideConfig)
		} else {
			config = domain.AgentConfig{}
		}
	} else if !overrideConfig.IsZero() {
		config = mergeReviewerAgentConfig(config, overrideConfig)
		hasConfigOverride = config != resolvedConfig
	}
	reviewRows, err := e.store.ListReviewsBySession(ctx, workerID)
	if err != nil {
		return TriggerResult{}, err
	}
	if err := e.destroyOtherReviewerHandles(ctx, workerID, harness, reviewRows); err != nil {
		return TriggerResult{}, err
	}
	reviewRow, hasReview, err := e.store.GetReviewBySessionAndHarness(ctx, workerID, harness)
	if err != nil {
		return TriggerResult{}, err
	}
	if stale, err := e.cancelStaleRunningRuns(ctx, workerID, reviewRow, hasReview, runs); err != nil {
		return TriggerResult{}, err
	} else if stale {
		runs, err = e.store.ListReviewRunsBySession(ctx, workerID)
		if err != nil {
			return TriggerResult{}, err
		}
	}
	hadRunningReviewer := reviewRunsContainRunningForHarness(runs, harness)
	reviews := Plan(prs, runs)
	if source == domain.ReviewTriggerAuto {
		reviews = Plan(prs, reviewRunsForHarness(runs, harness))
		// Automatic sweeps may discover another eligible PR while this harness is
		// already reviewing one. Reconciliation above has proved the reviewer
		// terminal is still alive, so do not append work to its active batch. A
		// later sweep can start the remaining PR after the current run terminates.
		if hadRunningReviewer {
			return TriggerResult{
				Run:              firstReusableRun(reviews),
				ReviewerHandleID: reviewRow.ReviewerHandleID,
				Created:          false,
				Reviews:          reviews,
				Runs:             runs,
			}, nil
		}
	}

	now := e.clock()
	if !hasReview {
		reviewRow, err = e.upsertReview(ctx, worker, harness, "", "", "", "", now)
		if err != nil {
			return TriggerResult{}, err
		}
	}
	// Decide the reviewer generation before inserting any pass. The generation
	// is part of the immutable run binding, so a run can never be retrofitted
	// with the launch that happened to follow it.
	reuseHandle := false
	launchID := ""
	if !hasConfigOverride && reviewRow.ReviewerHandleID != "" && reviewerPaneReusable(reviewRow, hadRunningReviewer) {
		alive, aliveErr := e.launcher.Alive(ctx, reviewRow.ReviewerHandleID)
		if aliveErr != nil {
			return TriggerResult{}, aliveErr
		}
		reuseHandle = alive
		if reuseHandle {
			launchID = reviewRow.ReviewerLaunchID
		}
	}
	if launchID == "" {
		launchID = e.newID()
	}
	var created []domain.ReviewRun
	type staleSupersede struct {
		prURL     string
		targetSHA string
	}
	var pendingSupersedes []staleSupersede
	var runningToReplace []domain.ReviewRun
	batchID := ""
	failRuns := func(start int, cause error) error {
		// Refusal/cancellation must settle every inserted pass, including when
		// another settlement fails. Keep the original cause and all write errors.
		cleanupCtx := stdctx.WithoutCancel(ctx)
		resultErr := cause
		for _, run := range created[start:] {
			updated, updateErr := e.store.UpdateReviewRunResult(cleanupCtx, run.ID, domain.ReviewRunFailed, domain.VerdictNone, reviewerFailureBodyForLaunch(cause, run.LaunchID), "", run.AutoInjectReview)
			if updateErr != nil {
				resultErr = errors.Join(resultErr, fmt.Errorf("settle unlaunched review run %q: %w", run.ID, updateErr))
			} else if !updated {
				resultErr = errors.Join(resultErr, fmt.Errorf("settle unlaunched review run %q: running row was not updated", run.ID))
			}
		}
		return resultErr
	}
	type preparedReview struct {
		state       PRReviewState
		observation ports.WorkspaceObservation
	}
	var prepared []preparedReview
	for _, reviewState := range reviews {
		// A PR that is already up to date has nothing due — unless the caller asked
		// for a different reviewer than the one that produced that verdict. Picking
		// another agent is precisely a request for a second opinion on this commit,
		// so refusing it makes the reviewer choice inert exactly when it is most
		// useful. Ineligible PRs stay excluded: nothing can review those.
		eligible := reviewState.Status == ReviewStateNeedsReview || (source == domain.ReviewTriggerManual && reviewState.Status == ReviewStateChangesRequested)
		if source == domain.ReviewTriggerAuto && autoReviewHeadBlocked(runs, reviewState.PRURL, reviewState.TargetSHA, harness) {
			eligible = false
		}
		if !eligible && !secondOpinionWanted(reviewState, hasHarnessOverride, hasConfigOverride, harness) {
			continue
		}
		workspaceObservation, observeErr := e.observeReviewWorkspace(ctx, worker, reviewState.TargetSHA)
		if observeErr != nil {
			return TriggerResult{}, observeErr
		}
		if strings.TrimSpace(workspaceObservation.HeadSHA) != strings.TrimSpace(reviewState.TargetSHA) {
			return TriggerResult{}, fmt.Errorf("%w: observed workspace HEAD %q does not match review target %q", ErrInvalid, workspaceObservation.HeadSHA, reviewState.TargetSHA)
		}
		prepared = append(prepared, preparedReview{state: reviewState, observation: workspaceObservation})
	}
	// Validate the entire batch before inserting running rows or superseding
	// incumbents: one checkout cannot satisfy different PR heads at once.
	for _, item := range prepared {
		reviewState, workspaceObservation := item.state, item.observation
		if hasConfigOverride {
			pendingSupersedes = append(pendingSupersedes, staleSupersede{prURL: reviewState.PRURL, targetSHA: reviewState.TargetSHA})
		} else if _, err := e.store.SupersedeStaleRunningReviewRuns(ctx, workerID, reviewState.PRURL, reviewState.TargetSHA, "superseded by a review trigger for a newer commit"); err != nil {
			return TriggerResult{}, failRuns(0, err)
		}
		if batchID == "" {
			batchID = e.newID()
		}
		run := domain.ReviewRun{
			ID:            e.newID(),
			ReviewID:      reviewRow.ID,
			SessionID:     workerID,
			BatchID:       batchID,
			Harness:       harness,
			TriggerSource: source,
			PRURL:         reviewState.PRURL,
			TargetSHA:     reviewState.TargetSHA,
			TargetBinding: reviewTargetBinding(worker, reviewState.TargetSHA, workspaceObservation, now),
			Provenance:    reviewProvenance(override, overrideConfig, resolvedHarness, resolvedConfig, harness, config, hasHarnessOverride || hasConfigOverride),
			LaunchID:      launchID,
			Status:        domain.ReviewRunRunning,
			Verdict:       domain.VerdictNone,
			CreatedAt:     now,
			// Completion refreshes this snapshot before delivery. Keeping the
			// trigger-time value also makes a running pass truthful in the API.
			AutoInjectReview: worker.AutoInjectReview,
		}
		if err := e.store.InsertReviewRun(ctx, run); err != nil {
			if errors.Is(err, domain.ErrDuplicateReviewRun) {
				if existing, ok, getErr := e.store.GetReviewRunBySessionPRSHAAndHarness(ctx, workerID, reviewState.PRURL, reviewState.TargetSHA, harness); getErr != nil {
					return TriggerResult{}, failRuns(0, getErr)
				} else if ok {
					reviews = replaceReviewLatestRun(reviews, reviewState.PRURL, reviewState.TargetSHA, existing)
					continue
				}
			}
			return TriggerResult{}, failRuns(0, err)
		}
		created = append(created, run)
		reviews = replaceReviewLatestRun(reviews, reviewState.PRURL, reviewState.TargetSHA, run)
		if predecessor, ok := runningReviewRunForReplacement(runs, workerID, reviewState, reviewRow.ID, harness, reviewRow.ReviewerLaunchID); ok {
			// A replacement gets a fresh launch generation. Close the exact
			// predecessor after the replacement is ready and before the old pane is
			// destroyed, regardless of whether the old row predates launch binding
			// or already carries one. Stale rows for another target are handled by
			// the existing supersession query below; same-target rows never use a
			// broad cancellation. Record it only after the replacement run was
			// actually inserted so a duplicate-writer race cannot cancel an
			// unpaired predecessor.
			runningToReplace = append(runningToReplace, predecessor)
		}
	}
	if len(created) == 0 {
		return TriggerResult{Run: firstReusableRun(reviews), ReviewerHandleID: reviewRow.ReviewerHandleID, Created: false, Reviews: reviews, Runs: runs}, nil
	}
	queueRuns := append([]domain.ReviewRun{}, created...)
	queue := reviewQueue(queueRuns)
	launchRun := queueRuns[0]
	incumbentReview := reviewRow
	previousHandleID := incumbentReview.ReviewerHandleID
	launchAgentSessionID := reviewRow.AgentSessionID
	if hasConfigOverride {
		launchAgentSessionID = ""
	}
	persistedAgentSessionID := launchAgentSessionID
	var retiredCleanup domain.ReviewerCleanupObligation
	handleID := ""
	if !hasConfigOverride && reviewRow.ReviewerHandleID != "" && reviewerPaneReusable(reviewRow, hadRunningReviewer) {
		alive, err := e.launcher.Alive(ctx, reviewRow.ReviewerHandleID)
		if err != nil {
			return TriggerResult{}, failRuns(0, err)
		}
		if alive {
			handleID = reviewRow.ReviewerHandleID
		}
	}
	if !reuseHandle {
		// Each pass gets a fresh reviewer process on the same stable terminal
		// handle when there is no resumable live agent session to notify.
		if err := e.launcher.Preflight(ctx, harness, worker.Metadata.WorkspacePath); err != nil {
			return TriggerResult{}, failRuns(0, fmt.Errorf("reviewer preflight: %w", err))
		}
		launchSpec := reviewLaunchSpec(worker, harness, config, launchRun, queue, 0, launchAgentSessionID, launchID)
		launchSpec.PreviousHandleID = previousHandleID
		launchSpec.PreviousLaunchID = strings.TrimSpace(incumbentReview.ReviewerLaunchID)
		launch, err := e.launcher.Spawn(ctx, launchSpec)
		if err != nil {
			return TriggerResult{}, failRuns(0, fmt.Errorf("launch reviewer: %w", err))
		}
		handleID = launch.HandleID
		if strings.TrimSpace(handleID) == "" {
			return TriggerResult{}, failRuns(0, errors.New("launch reviewer: launcher returned an empty handle"))
		}
		effectiveLaunchID := launchID
		if returnedLaunchID := strings.TrimSpace(launch.LaunchID); returnedLaunchID != "" {
			if returnedLaunchID != launchID {
				launchErr := fmt.Errorf("launch reviewer returned launch generation %q, expected %q", returnedLaunchID, launchID)
				if destroyErr := e.launcher.Destroy(stdctx.WithoutCancel(ctx), handleID); destroyErr != nil {
					launchErr = errors.Join(launchErr, reviewerCleanupEvidence(destroyErr, handleID))
				}
				return TriggerResult{}, failRuns(0, launchErr)
			}
			effectiveLaunchID = returnedLaunchID
		}
		if launch.AgentSessionID != "" {
			persistedAgentSessionID = launch.AgentSessionID
		}
		replacementReview := incumbentReview
		replacementReview.ReviewerHandleID = handleID
		replacementReview.AgentSessionID = persistedAgentSessionID
		replacementReview.ReviewerLaunchID = effectiveLaunchID
		replacementReview.UpdatedAt = now
		if previousHandleID != "" && previousHandleID != handleID {
			retiredCleanup = e.reviewerCleanupObligation(workerID, incumbentReview, true, []domain.ReviewRun{launchRun}, incumbentReview.Harness, incumbentReview.ReviewerLaunchID, incumbentReview.AgentSessionID, previousHandleID)
		}
		if err := e.acceptReviewerReplacement(ctx, incumbentReview, replacementReview, runningToReplace, hasConfigOverride && persistedAgentSessionID == "", retiredCleanup); err != nil {
			acceptErr := fmt.Errorf("accept reviewer replacement: %w", err)
			if destroyErr := e.launcher.Destroy(stdctx.WithoutCancel(ctx), handleID); destroyErr != nil {
				acceptErr = errors.Join(acceptErr, reviewerCleanupEvidence(destroyErr, handleID))
			}
			return TriggerResult{}, failRuns(0, acceptErr)
		}
		reviewRow = replacementReview
	} else {
		if err := e.launcher.Notify(ctx, handleID, reviewLaunchSpec(worker, harness, config, launchRun, queue, 0, reviewRow.AgentSessionID, reviewRow.ReviewerLaunchID)); err != nil {
			return TriggerResult{}, failRuns(0, fmt.Errorf("notify reviewer: %w", err))
		}
	}
	for _, stale := range pendingSupersedes {
		if _, err := e.store.SupersedeStaleRunningReviewRuns(ctx, workerID, stale.prURL, stale.targetSHA, "superseded by a review trigger for a newer commit"); err != nil {
			return TriggerResult{}, fmt.Errorf("reviewer replacement accepted but stale-run supersession failed: %w", err)
		}
	}
	if previousHandleID != "" && previousHandleID != handleID {
		if err := e.launcher.Destroy(stdctx.WithoutCancel(ctx), previousHandleID); err != nil {
			teardownErr := fmt.Errorf("destroy previous reviewer %q after replacement acceptance: %w", previousHandleID, reviewerCleanupEvidence(err, previousHandleID))
			// Acceptance already committed this obligation. Reaffirm the same
			// identity idempotently; a storage refusal cannot erase that commit.
			if cleanupErr := e.persistReviewerCleanupObligation(ctx, retiredCleanup); cleanupErr != nil {
				teardownErr = errors.Join(teardownErr, cleanupErr)
			}
			return TriggerResult{}, teardownErr
		}
		if retiredCleanup.ID != "" {
			store, ok := e.store.(ReviewerCleanupObligationStore)
			if !ok {
				return TriggerResult{}, errors.New("durable reviewer cleanup obligations are unavailable")
			}
			cleared, err := store.ResolveReviewerCleanupObligation(stdctx.WithoutCancel(ctx), retiredCleanup.ID, e.clock())
			if err != nil {
				return TriggerResult{}, fmt.Errorf("clear retired reviewer cleanup obligation %q: %w", retiredCleanup.ID, err)
			}
			if !cleared {
				return TriggerResult{}, fmt.Errorf("retired reviewer cleanup obligation %q was not unresolved", retiredCleanup.ID)
			}
		}
	}
	for i := range created {
		created[i].ReviewID = reviewRow.ID
	}
	triggerRuns := append([]domain.ReviewRun{}, created...)
	triggerRuns = append(triggerRuns, runs...)
	resultRun := launchRun
	createdFlag := len(created) > 0
	return TriggerResult{Run: resultRun, ReviewerHandleID: handleID, Created: createdFlag, Reviews: reviews, Runs: triggerRuns, CreatedRuns: created}, nil
}

func (e *Engine) observeReviewWorkspace(ctx stdctx.Context, worker domain.SessionRecord, targetSHA string) (ports.WorkspaceObservation, error) {
	if e.workspace == nil {
		return ports.WorkspaceObservation{}, fmt.Errorf("%w: AO workspace observer is unavailable", ErrInvalid)
	}
	workspacePath := strings.TrimSpace(worker.Metadata.WorkspacePath)
	executionBase := strings.TrimSpace(worker.Metadata.ExecutionBaseSHA)
	if workspacePath == "" || executionBase == "" {
		return ports.WorkspaceObservation{}, fmt.Errorf("%w: reviewer target binding requires workspace path and execution base SHA", ErrInvalid)
	}
	observation, err := e.workspace.ObserveWorkspace(ctx, ports.WorkspaceInfo{
		Path:             workspacePath,
		Branch:           worker.Metadata.Branch,
		TargetSHA:        targetSHA,
		BaseRef:          worker.Metadata.DiffBaseRef,
		ExecutionBaseSHA: executionBase,
		SessionID:        worker.ID,
		ProjectID:        worker.ProjectID,
	})
	if err != nil {
		return ports.WorkspaceObservation{}, fmt.Errorf("%w: observe reviewer workspace: %v", ErrInvalid, err)
	}
	if strings.TrimSpace(observation.Path) != workspacePath || strings.TrimSpace(observation.HeadSHA) == "" {
		return ports.WorkspaceObservation{}, fmt.Errorf("%w: reviewer target binding observation is incomplete", ErrInvalid)
	}
	return observation, nil
}

func reviewTargetBinding(worker domain.SessionRecord, targetSHA string, observation ports.WorkspaceObservation, observedAt time.Time) domain.ReviewTargetBinding {
	return domain.ReviewTargetBinding{
		TargetSHA:           targetSHA,
		ExecutionBaseSHA:    worker.Metadata.ExecutionBaseSHA,
		DiffBaseSHA:         worker.Metadata.DiffBaseSHA,
		DiffBaseRef:         worker.Metadata.DiffBaseRef,
		WorkspacePath:       worker.Metadata.WorkspacePath,
		ObservedHeadSHA:     observation.HeadSHA,
		ObservedBranch:      observation.Branch,
		WorkspaceDirty:      observation.Dirty,
		WorkspaceDirtyKnown: true,
		IndexStaged:         observation.Staged,
		IndexStagedKnown:    true,
		ObservationPhase:    "review_run_creation",
		ObservedAt:          observedAt,
	}
}

func reviewProvenance(requestedHarness domain.ReviewerHarness, requestedConfig domain.AgentConfig, configuredHarness domain.ReviewerHarness, configuredConfig domain.AgentConfig, resolvedHarness domain.ReviewerHarness, resolvedConfig domain.AgentConfig, requested bool) domain.ReviewerProvenance {
	return domain.ReviewerProvenance{
		Requested:  domain.ReviewerSettings{Harness: requestedHarness, Model: requestedConfig.Model, Effort: requestedConfig.Effort, Mode: requestedConfig.Mode, Permission: requestedConfig.Permissions, Observed: requested},
		Configured: domain.ReviewerSettings{Harness: configuredHarness, Model: configuredConfig.Model, Effort: configuredConfig.Effort, Mode: configuredConfig.Mode, Permission: configuredConfig.Permissions, Observed: true},
		AOResolved: domain.ReviewerSettings{Harness: resolvedHarness, Model: resolvedConfig.Model, Effort: resolvedConfig.Effort, Mode: resolvedConfig.Mode, Permission: resolvedConfig.Permissions, Observed: true},
	}
}

func autoReviewSessionReason(worker domain.SessionRecord, now time.Time) string {
	switch {
	case !worker.AutoReviewEnabled:
		return "disabled"
	case worker.Kind != domain.KindWorker:
		return "not_worker"
	case worker.IsTerminated:
		return "terminated"
	case worker.Activity.State != domain.ActivityIdle:
		return "not_idle"
	case worker.Activity.LastActivityAt.IsZero() || now.Sub(worker.Activity.LastActivityAt) < time.Minute:
		return "idle_threshold_not_met"
	default:
		return ""
	}
}

// SwitchReviewer serializes reviewer preference changes with trigger/restore
// and returns the authoritative post-switch review state.
func (e *Engine) SwitchReviewer(
	ctx stdctx.Context,
	workerID domain.SessionID,
	harness domain.ReviewerHarness,
	config domain.AgentConfig,
) (SessionReviews, error) {
	if workerID == "" {
		return SessionReviews{}, fmt.Errorf("%w: worker session id is required", ErrInvalid)
	}
	if harness != "" && !harness.IsKnown() {
		return SessionReviews{}, fmt.Errorf("%w: unknown reviewer harness %q", ErrInvalid, harness)
	}
	if err := config.Validate(); err != nil {
		return SessionReviews{}, fmt.Errorf("%w: reviewer config: %w", ErrInvalid, err)
	}
	unlock := e.lockWorker(workerID)
	defer unlock()

	worker, ok, err := e.sessions.GetSession(ctx, workerID)
	if err != nil {
		return SessionReviews{}, err
	}
	if !ok {
		return SessionReviews{}, fmt.Errorf("%w: worker session %q", ErrNotFound, workerID)
	}
	previousSelected, previousConfig, err := e.reviewerSelection(ctx, worker)
	if err != nil {
		return SessionReviews{}, err
	}
	if ok, err := e.store.SetSessionReviewerConfig(ctx, workerID, harness, config, e.clock()); err != nil {
		return SessionReviews{}, err
	} else if !ok {
		return SessionReviews{}, fmt.Errorf("%w: worker session %q", ErrNotFound, workerID)
	}
	worker.ReviewerHarness = harness
	worker.ReviewerConfig = config
	selected, selectedConfig, err := e.reviewerSelection(ctx, worker)
	if err != nil {
		return SessionReviews{}, err
	}
	reviewRows, err := e.store.ListReviewsBySession(ctx, workerID)
	if err != nil {
		return SessionReviews{}, err
	}
	if err := e.destroyOtherReviewerHandles(ctx, workerID, selected, reviewRows); err != nil {
		return SessionReviews{}, err
	}
	if previousSelected == selected && previousConfig != selectedConfig {
		if err := e.resetReviewerRuntimeLocked(ctx, workerID, selected); err != nil {
			return SessionReviews{}, err
		}
	}
	if _, err := e.restoreReviewerLocked(ctx, workerID, worker, selected, selectedConfig); err != nil {
		return SessionReviews{}, err
	}
	return e.listLocked(ctx, workerID, selected)
}

func reviewerPaneReusable(reviewRow domain.Review, hadRunningReviewer bool) bool {
	if strings.TrimSpace(reviewRow.AgentSessionID) != "" {
		return true
	}
	return hadRunningReviewer
}

func reviewRunsContainRunningForHarness(runs []domain.ReviewRun, harness domain.ReviewerHarness) bool {
	for _, run := range runs {
		if (run.Harness == harness || run.Harness == "") && run.Status == domain.ReviewRunRunning {
			return true
		}
	}
	return false
}

func (e *Engine) destroyOtherReviewerHandles(ctx stdctx.Context, workerID domain.SessionID, selected domain.ReviewerHarness, reviews []domain.Review) error {
	for _, review := range reviews {
		if review.Harness == selected || review.ReviewerHandleID == "" {
			continue
		}
		if err := e.launcher.Destroy(ctx, review.ReviewerHandleID); err != nil {
			return err
		}
		if err := e.store.ClearReviewerHandleByHarness(ctx, workerID, review.Harness); err != nil {
			return err
		}
		if _, err := e.store.CancelRunningReviewRunsBySessionAndHarness(ctx, workerID, review.Harness, "cancelled because reviewer agent was switched"); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) resetReviewerRuntimeLocked(ctx stdctx.Context, workerID domain.SessionID, harness domain.ReviewerHarness) error {
	review, ok, err := e.store.GetReviewBySessionAndHarness(ctx, workerID, harness)
	if err != nil || !ok {
		return err
	}
	if review.ReviewerHandleID != "" {
		if err := e.launcher.Destroy(ctx, review.ReviewerHandleID); err != nil {
			return err
		}
		if err := e.store.ClearReviewerHandleByHarness(ctx, workerID, harness); err != nil {
			return err
		}
	}
	if _, err := e.store.UpdateReviewAgentSessionID(ctx, review.ID, ""); err != nil {
		return err
	}
	return nil
}

// RestoreReviewer relaunches the reviewer terminal for a restored worker when
// that worker already has review history. It does not create review_run rows or
// start a review; explicit trigger remains the only path that starts review
// work.
func (e *Engine) RestoreReviewer(ctx stdctx.Context, workerID domain.SessionID) (RestoreReviewerResult, error) {
	if workerID == "" {
		return RestoreReviewerResult{}, fmt.Errorf("%w: worker session id is required", ErrInvalid)
	}
	unlock := e.lockWorker(workerID)
	defer unlock()
	worker, ok, err := e.sessions.GetSession(ctx, workerID)
	if err != nil {
		return RestoreReviewerResult{}, err
	}
	if !ok {
		return RestoreReviewerResult{}, fmt.Errorf("%w: worker session %q", ErrNotFound, workerID)
	}
	if worker.IsTerminated || worker.Metadata.WorkspacePath == "" {
		return RestoreReviewerResult{}, nil
	}
	harness, config, err := e.reviewerSelection(ctx, worker)
	if err != nil {
		return RestoreReviewerResult{}, err
	}
	return e.restoreReviewerLocked(ctx, workerID, worker, harness, config)
}

// ReconcileReviewerCleanup retries durable reviewer cleanup independently of
// reviewer restoration. Startup calls this even when the worker runtime is
// adopted in place, so a live-session adoption cannot skip cleanup obligations
// left by a prior crash.
func (e *Engine) ReconcileReviewerCleanup(ctx stdctx.Context, workerID domain.SessionID) error {
	if workerID == "" {
		return fmt.Errorf("%w: worker session id is required", ErrInvalid)
	}
	if _, ok := e.store.(ReviewerCleanupObligationStore); !ok {
		return errors.New("durable reviewer cleanup obligations are unavailable")
	}
	unlock := e.lockWorker(workerID)
	defer unlock()
	runs, err := e.store.ListReviewRunsBySession(ctx, workerID)
	if err != nil {
		return err
	}
	return e.retryPendingReviewerCleanup(ctx, workerID, runs)
}

func (e *Engine) restoreReviewerLocked(
	ctx stdctx.Context,
	workerID domain.SessionID,
	worker domain.SessionRecord,
	harness domain.ReviewerHarness,
	config domain.AgentConfig,
) (RestoreReviewerResult, error) {
	reviewRows, err := e.store.ListReviewsBySession(ctx, workerID)
	if err != nil {
		return RestoreReviewerResult{}, err
	}
	if err := e.destroyOtherReviewerHandles(ctx, workerID, harness, reviewRows); err != nil {
		return RestoreReviewerResult{}, err
	}
	reviewRow, hasReview, err := e.store.GetReviewBySessionAndHarness(ctx, workerID, harness)
	if err != nil {
		return RestoreReviewerResult{}, err
	}
	runs, err := e.store.ListReviewRunsBySession(ctx, workerID)
	if err != nil {
		return RestoreReviewerResult{}, err
	}
	if err := e.retryPendingReviewerCleanup(ctx, workerID, runs); err != nil {
		return RestoreReviewerResult{}, err
	}
	previousRuns := reviewRunsForHarness(runs, harness)
	if !hasReview && len(previousRuns) == 0 {
		return RestoreReviewerResult{}, nil
	}
	if hasReview && reviewRow.ReviewerHandleID != "" {
		alive, err := e.launcher.Alive(ctx, reviewRow.ReviewerHandleID)
		if err != nil {
			return RestoreReviewerResult{}, err
		}
		if alive {
			return RestoreReviewerResult{ReviewerHandleID: reviewRow.ReviewerHandleID, Restored: false}, nil
		}
	}
	priorReview := reviewRow
	previousLaunchID := strings.TrimSpace(reviewRow.ReviewerLaunchID)
	agentSessionID := ""
	if hasReview {
		agentSessionID = reviewRow.AgentSessionID
	} else {
		// Keep the first restore attempt in memory. There is no durable
		// generation to fence until a live handle has been accepted.
		reviewRow = domain.Review{
			ID:                    e.newID(),
			SessionID:             worker.ID,
			ProjectID:             worker.ProjectID,
			Harness:               harness,
			ReviewerActivityState: domain.ActivityIdle,
			CreatedAt:             e.clock(),
			UpdatedAt:             e.clock(),
		}
	}
	// A native restore continues the generation already bound to its running
	// pass. A restore with no uniquely attributable running pass gets a fresh
	// idle generation; restore itself never rewrites a ReviewRun binding.
	restoreRuns, launchID, restoreRunID, attributionRejected := restoreRunAttribution(previousRuns, reviewRow, harness)
	if attributionRejected {
		return RestoreReviewerResult{}, fmt.Errorf("%w: reviewer restore run attribution is ambiguous or has a mismatched launch generation", ErrInvalid)
	}
	if launchID == "" {
		launchID = e.newID()
	}
	if hasReview {
		// Claim the generation before provider hooks can arrive, but retain the
		// exact prior row so any failed restore can restore the old authority.
		reviewRow, err = e.upsertReview(ctx, worker, harness, reviewRow.ReviewerHandleID, agentSessionID, launchID, "", e.clock())
		if err != nil {
			return RestoreReviewerResult{}, err
		}
	} else {
		reviewRow.ReviewerLaunchID = launchID
	}
	launch, err := e.launcher.RestoreTerminal(ctx, LaunchSpec{
		RunID:                restoreRunID,
		ReviewSessionID:      reviewRow.ID,
		LaunchID:             launchID,
		WorkerID:             worker.ID,
		ProjectID:            worker.ProjectID,
		Harness:              harness,
		AgentConfig:          config,
		WorkspacePath:        worker.Metadata.WorkspacePath,
		AgentSessionID:       agentSessionID,
		PreviousLaunchID:     previousLaunchID,
		RequireNativeHistory: harness == domain.ReviewerCodex,
		PreviousRuns:         previousRuns,
	})
	if err != nil {
		restoreErr := fmt.Errorf("restore reviewer: %w", err)
		if err := e.recordPossibleReviewerCleanup(ctx, worker.ID, reviewRow, hasReview, restoreRuns, harness, launchID, agentSessionID, err); err != nil {
			restoreErr = errors.Join(restoreErr, err)
		}
		if rollbackErr := e.rollbackRestoreReview(ctx, hasReview, priorReview, "", false); rollbackErr != nil {
			restoreErr = errors.Join(restoreErr, rollbackErr)
		}
		if failErr := e.failRunningRestoredRuns(ctx, restoreRuns, restoreErr, launchID); failErr != nil {
			return RestoreReviewerResult{}, errors.Join(restoreErr, failErr)
		}
		return RestoreReviewerResult{}, restoreErr
	}
	if len(restoreRuns) > 0 && !launch.NativeResumed {
		const reason = "reviewer native conversation was unavailable during restore; retry the review"
		if strings.TrimSpace(launch.HandleID) == "" {
			failureErr := errors.New(reason + "; restore returned an empty runtime handle")
			if rollbackErr := e.rollbackRestoreReview(ctx, hasReview, priorReview, "", false); rollbackErr != nil {
				failureErr = errors.Join(failureErr, rollbackErr)
			}
			if failErr := e.failRunningRestoredRuns(ctx, restoreRuns, failureErr, launchID); failErr != nil {
				return RestoreReviewerResult{}, errors.Join(failureErr, failErr)
			}
			return RestoreReviewerResult{}, failureErr
		}
		cleanupErr := e.cleanupRestoredReviewer(ctx, worker.ID, reviewRow, hasReview, restoreRuns, harness, launch.LaunchID, agentSessionID, launch.HandleID)
		failureErr := error(errors.New(reason))
		if cleanupErr != nil {
			failureErr = errors.Join(failureErr, cleanupErr)
		}
		rollbackErr := e.rollbackRestoreReview(ctx, hasReview, priorReview, launch.HandleID, cleanupErr == nil)
		if rollbackErr != nil {
			failureErr = errors.Join(failureErr, rollbackErr)
		}
		if err := e.failRunningRestoredRuns(ctx, restoreRuns, failureErr, launch.LaunchID); err != nil {
			return RestoreReviewerResult{}, errors.Join(failureErr, err)
		}
		if cleanupErr != nil || rollbackErr != nil {
			return RestoreReviewerResult{}, failureErr
		}
		// The fresh fallback was destroyed and is never an authoritative
		// reviewer handle. Restore reports the durable run failure only.
		return RestoreReviewerResult{}, nil
	}
	if strings.TrimSpace(launch.HandleID) == "" || strings.TrimSpace(launch.LaunchID) == "" {
		restoreErr := errors.New("restore reviewer returned incomplete runtime authority")
		if strings.TrimSpace(launch.HandleID) != "" {
			restoreErr = errors.Join(restoreErr, e.cleanupRestoredReviewer(ctx, worker.ID, reviewRow, hasReview, restoreRuns, harness, launch.LaunchID, agentSessionID, launch.HandleID))
		}
		if rollbackErr := e.rollbackRestoreReview(ctx, hasReview, priorReview, launch.HandleID, false); rollbackErr != nil {
			restoreErr = errors.Join(restoreErr, rollbackErr)
		}
		if failErr := e.failRunningRestoredRuns(ctx, restoreRuns, restoreErr, launch.LaunchID); failErr != nil {
			return RestoreReviewerResult{}, errors.Join(restoreErr, failErr)
		}
		return RestoreReviewerResult{}, restoreErr
	}
	if launch.AgentSessionID != "" {
		agentSessionID = launch.AgentSessionID
	}
	if _, err := e.persistRestoredReview(ctx, worker, harness, hasReview, reviewRow, launch.HandleID, agentSessionID, launch.LaunchID); err != nil {
		persistErr := fmt.Errorf("persist restored reviewer: %w", err)
		cleanupErr := e.cleanupRestoredReviewer(ctx, worker.ID, reviewRow, hasReview, restoreRuns, harness, launch.LaunchID, agentSessionID, launch.HandleID)
		if cleanupErr != nil {
			persistErr = errors.Join(persistErr, cleanupErr)
		}
		if rollbackErr := e.rollbackRestoreReview(ctx, hasReview, priorReview, launch.HandleID, cleanupErr == nil); rollbackErr != nil {
			persistErr = errors.Join(persistErr, rollbackErr)
		}
		if failErr := e.failRunningRestoredRuns(ctx, restoreRuns, persistErr, launch.LaunchID); failErr != nil {
			return RestoreReviewerResult{}, errors.Join(persistErr, failErr)
		}
		return RestoreReviewerResult{}, persistErr
	}
	return RestoreReviewerResult{ReviewerHandleID: launch.HandleID, Restored: true}, nil
}

func (e *Engine) persistRestoredReview(
	ctx stdctx.Context,
	worker domain.SessionRecord,
	harness domain.ReviewerHarness,
	hasReview bool,
	review domain.Review,
	handleID, agentSessionID, launchID string,
) (domain.Review, error) {
	if hasReview {
		return e.upsertReview(ctx, worker, harness, handleID, agentSessionID, launchID, "", e.clock())
	}
	review.ReviewerHandleID = strings.TrimSpace(handleID)
	review.AgentSessionID = strings.TrimSpace(agentSessionID)
	review.ReviewerLaunchID = strings.TrimSpace(launchID)
	review.UpdatedAt = e.clock()
	if err := e.store.UpsertReview(ctx, review); err != nil {
		return domain.Review{}, err
	}
	return review, nil
}

func reviewerCleanupHandleFromError(err error) (string, bool) {
	var effectErr ports.RuntimeEffectError
	if !errors.As(err, &effectErr) || effectErr.CleanupOutcome() == ports.RuntimeCleanupSucceeded {
		return "", false
	}
	handleID := strings.TrimSpace(effectErr.PossibleHandle().ID)
	return handleID, handleID != ""
}

type reviewerCleanupGenerationProvider interface {
	CleanupGeneration() string
}

type reviewerCleanupGenerationBinding interface {
	CleanupGenerationBound() bool
}

func reviewerCleanupGenerationFromError(err error) string {
	var provider reviewerCleanupGenerationProvider
	if errors.As(err, &provider) {
		return strings.TrimSpace(provider.CleanupGeneration())
	}
	return ""
}

func (e *Engine) recordReviewerCleanupObligation(
	ctx stdctx.Context,
	workerID domain.SessionID,
	review domain.Review,
	hasReview bool,
	runs []domain.ReviewRun,
	harness domain.ReviewerHarness,
	launchID, agentSessionID, handleID string,
) error {
	return e.persistReviewerCleanupObligation(ctx, e.reviewerCleanupObligation(workerID, review, hasReview, runs, harness, launchID, agentSessionID, handleID))
}

func (e *Engine) reviewerCleanupObligation(
	workerID domain.SessionID,
	review domain.Review,
	hasReview bool,
	runs []domain.ReviewRun,
	harness domain.ReviewerHarness,
	launchID, agentSessionID, handleID string,
) domain.ReviewerCleanupObligation {
	obligation := domain.ReviewerCleanupObligation{
		ID:             e.newID(),
		SessionID:      workerID,
		Harness:        harness,
		HandleID:       strings.TrimSpace(handleID),
		LaunchID:       strings.TrimSpace(launchID),
		AgentSessionID: strings.TrimSpace(agentSessionID),
		Status:         domain.ReviewerCleanupUnresolved,
		CreatedAt:      e.clock(),
		UpdatedAt:      e.clock(),
	}
	if hasReview {
		obligation.ReviewID = strings.TrimSpace(review.ID)
	}
	if len(runs) == 1 {
		obligation.RunID = strings.TrimSpace(runs[0].ID)
	}
	return obligation
}

func (e *Engine) persistReviewerCleanupObligation(ctx stdctx.Context, obligation domain.ReviewerCleanupObligation) error {
	store, ok := e.store.(ReviewerCleanupObligationStore)
	if !ok {
		return errors.New("durable reviewer cleanup obligations are unavailable")
	}
	if obligation.HandleID == "" {
		return errors.New("durable reviewer cleanup obligation has an empty handle")
	}
	if err := store.InsertReviewerCleanupObligation(stdctx.WithoutCancel(ctx), obligation); err != nil {
		return fmt.Errorf("persist reviewer cleanup obligation for handle %q: %w", obligation.HandleID, err)
	}
	return nil
}

// recordPossibleReviewerCleanup handles the launcher's own failed cleanup path.
// When a ReviewRun is attributable, its existing marker remains the sole
// cleanup authority; otherwise this path persists the exact possible handle.
func (e *Engine) recordPossibleReviewerCleanup(
	ctx stdctx.Context,
	workerID domain.SessionID,
	review domain.Review,
	hasReview bool,
	runs []domain.ReviewRun,
	harness domain.ReviewerHarness,
	launchID, agentSessionID string,
	launchErr error,
) error {
	if len(runs) != 0 {
		return nil
	}
	handleID, ok := reviewerCleanupHandleFromError(launchErr)
	if !ok {
		return nil
	}
	var binding reviewerCleanupGenerationBinding
	if errors.As(launchErr, &binding) && binding.CleanupGenerationBound() {
		launchID = reviewerCleanupGenerationFromError(launchErr)
	} else if cleanupLaunchID := reviewerCleanupGenerationFromError(launchErr); cleanupLaunchID != "" {
		launchID = cleanupLaunchID
	}
	return e.recordReviewerCleanupObligation(ctx, workerID, review, hasReview, runs, harness, launchID, agentSessionID, handleID)
}

// cleanupRestoredReviewer records the obligation before attempting destruction
// when no ReviewRun can carry it. A successful destroy then clears that exact
// durable row; a failed destroy leaves the row unresolved for restart retry.
func (e *Engine) cleanupRestoredReviewer(
	ctx stdctx.Context,
	workerID domain.SessionID,
	review domain.Review,
	hasReview bool,
	runs []domain.ReviewRun,
	harness domain.ReviewerHarness,
	launchID, agentSessionID, handleID string,
) error {
	handleID = strings.TrimSpace(handleID)
	if handleID == "" {
		return nil
	}
	cleanupCtx := stdctx.WithoutCancel(ctx)
	var obligationStore ReviewerCleanupObligationStore
	var obligationID string
	var recordErr error
	if len(runs) == 0 {
		obligationStore, _ = e.store.(ReviewerCleanupObligationStore)
		if obligationStore == nil {
			recordErr = errors.New("durable reviewer cleanup obligations are unavailable")
		} else {
			obligation := domain.ReviewerCleanupObligation{
				ID:             e.newID(),
				SessionID:      workerID,
				Harness:        harness,
				HandleID:       handleID,
				LaunchID:       strings.TrimSpace(launchID),
				AgentSessionID: strings.TrimSpace(agentSessionID),
				Status:         domain.ReviewerCleanupUnresolved,
				CreatedAt:      e.clock(),
				UpdatedAt:      e.clock(),
			}
			if hasReview {
				obligation.ReviewID = strings.TrimSpace(review.ID)
			}
			obligationID = obligation.ID
			if err := obligationStore.InsertReviewerCleanupObligation(cleanupCtx, obligation); err != nil {
				recordErr = fmt.Errorf("persist reviewer cleanup obligation for handle %q: %w", handleID, err)
			}
		}
	}
	if recordErr != nil {
		return recordErr
	}
	authorities, err := e.store.ListReviewsBySession(cleanupCtx, workerID)
	if err != nil {
		return fmt.Errorf("list reviewer cleanup authorities for %q: %w", workerID, err)
	}
	if !reviewerCleanupAuthorityMatches(authorities, workerID, harness, review.ID, launchID, handleID) {
		return fmt.Errorf("reviewer cleanup handle %q rejected by launch-generation fence", handleID)
	}

	destroyErr := e.launcher.Destroy(cleanupCtx, handleID)
	if destroyErr != nil {
		destroyErr = reviewerCleanupEvidence(destroyErr, handleID)
		return destroyErr
	}
	if obligationStore != nil && obligationID != "" {
		if cleared, err := obligationStore.ResolveReviewerCleanupObligation(cleanupCtx, obligationID, e.clock()); err != nil {
			return errors.Join(recordErr, fmt.Errorf("clear reviewer cleanup obligation %q: %w", obligationID, err))
		} else if !cleared {
			return fmt.Errorf("reviewer cleanup obligation %q was not unresolved", obligationID)
		}
	}
	return nil
}

func (e *Engine) rollbackRestoreReview(ctx stdctx.Context, hadReview bool, prior domain.Review, attemptedHandle string, cleanupProven bool) error {
	if !hadReview {
		return nil
	}
	if cleanupProven && strings.TrimSpace(attemptedHandle) != "" && strings.TrimSpace(prior.ReviewerHandleID) == strings.TrimSpace(attemptedHandle) {
		prior.ReviewerHandleID = ""
	}
	if err := e.store.UpsertReview(ctx, prior); err != nil {
		return fmt.Errorf("restore prior reviewer authority: %w", err)
	}
	return nil
}

func (e *Engine) failRunningRestoredRuns(ctx stdctx.Context, runs []domain.ReviewRun, cause error, cleanupLaunchID string) error {
	for _, run := range runs {
		if run.Status != domain.ReviewRunRunning {
			continue
		}
		generation := strings.TrimSpace(cleanupLaunchID)
		if generation == "" {
			generation = run.LaunchID
		}
		body := reviewerFailureBodyForLaunch(cause, generation)
		if _, err := e.store.UpdateReviewRunResult(ctx, run.ID, domain.ReviewRunFailed, domain.VerdictNone, body, "", run.AutoInjectReview); err != nil {
			return fmt.Errorf("fail review run %q after reviewer restore: %w", run.ID, err)
		}
	}
	return nil
}

// restoreRunAttribution returns only the running passes that can be truthfully
// tied to this restore attempt. A durable reviewer generation is stronger than
// same-session/same-harness coincidence; a lone legacy row without a launch id
// remains attributable, but ambiguous rows and non-empty generation mismatches
// are rejected rather than reinterpreted as legacy.
func restoreRunAttribution(runs []domain.ReviewRun, review domain.Review, harness domain.ReviewerHarness) (attributable []domain.ReviewRun, launchID, runID string, rejected bool) {
	preferExactReviewID := false
	if strings.TrimSpace(review.ID) != "" {
		for _, run := range runs {
			if run.Status == domain.ReviewRunRunning && run.SessionID == review.SessionID && run.Harness == harness && run.ReviewID == review.ID {
				preferExactReviewID = true
				break
			}
		}
	}
	candidates := make([]domain.ReviewRun, 0, len(runs))
	for _, run := range runs {
		if run.Status != domain.ReviewRunRunning || run.SessionID != review.SessionID || run.Harness != harness {
			continue
		}
		if preferExactReviewID && run.ReviewID != review.ID {
			continue
		}
		if strings.TrimSpace(review.ID) != "" && strings.TrimSpace(run.ReviewID) != "" && run.ReviewID != review.ID {
			continue
		}
		candidates = append(candidates, run)
	}
	if len(candidates) == 0 {
		return nil, "", "", false
	}

	expectedLaunchID := strings.TrimSpace(review.ReviewerLaunchID)
	if expectedLaunchID != "" {
		matching := make([]domain.ReviewRun, 0, len(candidates))
		mismatched := false
		for _, run := range candidates {
			runLaunchID := strings.TrimSpace(run.LaunchID)
			if runLaunchID == expectedLaunchID {
				matching = append(matching, run)
			} else if runLaunchID != "" {
				mismatched = true
			}
		}
		if len(matching) > 0 {
			if len(matching) == 1 {
				return matching, expectedLaunchID, matching[0].ID, false
			}
			return matching, expectedLaunchID, "", false
		}
		if mismatched {
			return nil, "", "", true
		}
	}

	uniqueLaunchID := ""
	emptyLaunchCount := 0
	for _, run := range candidates {
		current := strings.TrimSpace(run.LaunchID)
		if current == "" {
			emptyLaunchCount++
			continue
		}
		if uniqueLaunchID == "" {
			uniqueLaunchID = current
		} else if uniqueLaunchID != current {
			return nil, "", "", true
		}
	}
	if uniqueLaunchID != "" && emptyLaunchCount == 0 {
		if len(candidates) == 1 {
			return candidates, uniqueLaunchID, candidates[0].ID, false
		}
		return candidates, uniqueLaunchID, "", false
	}
	if len(candidates) == 1 {
		// Keep an existing durable generation for a lone legacy predecessor so
		// restore does not make its native hooks stale before the launch returns.
		return candidates, expectedLaunchID, candidates[0].ID, false
	}
	return nil, "", "", true
}

// TeardownReviewerTerminal destroys reviewer panes before shutdown removes the
// worker worktree, but preserves review rows and native agent session ids so
// worker restore can recreate the reviewer terminal later.
func (e *Engine) TeardownReviewerTerminal(ctx stdctx.Context, workerID domain.SessionID) error {
	if workerID == "" {
		return fmt.Errorf("%w: worker session id is required", ErrInvalid)
	}
	unlock := e.lockWorker(workerID)
	defer unlock()
	reviews, err := e.store.ListReviewsBySession(ctx, workerID)
	if err != nil {
		return err
	}
	for _, review := range reviews {
		if review.ReviewerHandleID == "" {
			continue
		}
		if err := e.launcher.Destroy(ctx, review.ReviewerHandleID); err != nil {
			return err
		}
	}
	if len(reviews) > 0 {
		if err := e.store.ClearReviewerHandle(ctx, workerID); err != nil {
			return err
		}
	}
	return nil
}

func autoReviewHeadBlocked(runs []domain.ReviewRun, prURL, targetSHA string, harness domain.ReviewerHarness) bool {
	failedAutoRuns := 0
	for _, run := range runs {
		if run.PRURL != prURL || run.TargetSHA != targetSHA || (run.Harness != harness && run.Harness != "") {
			continue
		}
		if run.Status == domain.ReviewRunRunning || run.Status == domain.ReviewRunCancelled || run.Verdict == domain.VerdictApproved || run.Verdict == domain.VerdictChangesRequested {
			return true
		}
		if run.Status == domain.ReviewRunFailed && run.TriggerSource == domain.ReviewTriggerAuto {
			failedAutoRuns++
			if failedAutoRuns >= autoReviewFailedRetryLimit {
				return true
			}
		}
	}
	return false
}

func reviewRunsForHarness(runs []domain.ReviewRun, harness domain.ReviewerHarness) []domain.ReviewRun {
	out := make([]domain.ReviewRun, 0, len(runs))
	for _, run := range runs {
		if run.Harness == harness {
			out = append(out, run)
		}
	}
	return out
}

func (e *Engine) cancelStaleRunningRuns(ctx stdctx.Context, workerID domain.SessionID, reviewRow domain.Review, hasReview bool, runs []domain.ReviewRun) (bool, error) {
	hasRunning := false
	for _, run := range runs {
		if run.SessionID == workerID && run.Status == domain.ReviewRunRunning && run.Verdict == domain.VerdictNone {
			hasRunning = true
			break
		}
	}
	if !hasRunning {
		return false, nil
	}
	if !hasReview || reviewRow.ReviewerHandleID == "" {
		if _, err := e.store.CancelRunningReviewRunsBySession(ctx, workerID, "cancelled because reviewer terminal is unavailable"); err != nil {
			return false, err
		}
		return true, nil
	}
	alive, err := e.launcher.Alive(ctx, reviewRow.ReviewerHandleID)
	if err != nil {
		return false, err
	}
	if alive {
		return false, nil
	}
	if _, err := e.store.CancelRunningReviewRunsBySession(ctx, workerID, "cancelled because reviewer terminal is unavailable"); err != nil {
		return false, err
	}
	return true, nil
}

func reviewLaunchSpec(
	worker domain.SessionRecord,
	harness domain.ReviewerHarness,
	config domain.AgentConfig,
	run domain.ReviewRun,
	queue []ports.ReviewTask,
	index int,
	agentSessionID string,
	launchID string,
) LaunchSpec {
	return LaunchSpec{
		RunID:           run.ID,
		BatchID:         run.BatchID,
		ReviewSessionID: run.ReviewID,
		LaunchID:        launchID,
		WorkerID:        worker.ID,
		ProjectID:       worker.ProjectID,
		Harness:         harness,
		AgentConfig:     config,
		WorkspacePath:   worker.Metadata.WorkspacePath,
		AgentSessionID:  agentSessionID,
		PreviousRuns:    nil,
		PRURL:           run.PRURL,
		TargetSHA:       run.TargetSHA,
		ReviewQueue:     queue,
		ReviewIndex:     index,
	}
}

func reviewQueue(runs []domain.ReviewRun) []ports.ReviewTask {
	queue := make([]ports.ReviewTask, 0, len(runs))
	for _, run := range runs {
		queue = append(queue, ports.ReviewTask{
			RunID:     run.ID,
			PRURL:     run.PRURL,
			TargetSHA: run.TargetSHA,
		})
	}
	return queue
}

// secondOpinionWanted reports whether an explicit manual override makes an
// otherwise up-to-date PR worth running again. A config-only override (for
// example, picking a model under the same harness) always requests another
// pass. A harness-only override only does so when it actually changes the
// reviewer whose pass is currently authoritative; re-picking the same harness
// must still reuse. That remains true even while another harness is already
// running: a different reviewer is a second opinion, not a restart. Legacy
// runs may have an empty harness, which means "the same resolved harness as
// today" for this comparison.
func secondOpinionWanted(state PRReviewState, hasHarnessOverride, hasConfigOverride bool, harness domain.ReviewerHarness) bool {
	if state.Status == ReviewStateIneligible {
		return false
	}
	if state.LatestRun == nil {
		return false
	}
	if hasConfigOverride {
		return true
	}
	if !hasHarnessOverride {
		return false
	}
	latestHarness := state.LatestRun.Harness
	if latestHarness == "" {
		latestHarness = harness
	}
	return latestHarness != harness
}

func runningReviewRunForReplacement(runs []domain.ReviewRun, sessionID domain.SessionID, state PRReviewState, reviewID string, harness domain.ReviewerHarness, reviewerLaunchID string) (domain.ReviewRun, bool) {
	reviewerLaunchID = strings.TrimSpace(reviewerLaunchID)
	preferExactReviewID := false
	if strings.TrimSpace(reviewID) != "" {
		for _, run := range runs {
			if run.SessionID == sessionID && run.PRURL == state.PRURL && run.TargetSHA == state.TargetSHA && run.Status == domain.ReviewRunRunning && run.Harness == harness && run.ReviewID == reviewID {
				preferExactReviewID = true
				break
			}
		}
	}
	var predecessor domain.ReviewRun
	var legacyPredecessor domain.ReviewRun
	found := false
	legacyFound := false
	legacyAmbiguous := false
	for _, run := range runs {
		if run.SessionID != sessionID || run.PRURL != state.PRURL || run.TargetSHA != state.TargetSHA || run.Status != domain.ReviewRunRunning {
			continue
		}
		if run.Harness != harness && run.Harness != "" {
			continue
		}
		if preferExactReviewID && run.ReviewID != reviewID {
			continue
		}
		if strings.TrimSpace(reviewID) != "" && strings.TrimSpace(run.ReviewID) != "" && run.ReviewID != reviewID {
			continue
		}
		runLaunchID := strings.TrimSpace(run.LaunchID)
		if runLaunchID == "" {
			if !legacyFound {
				legacyPredecessor = run
				legacyFound = true
			} else {
				legacyAmbiguous = true
			}
			continue
		}
		if runLaunchID != reviewerLaunchID {
			continue
		}
		if !found || run.CreatedAt.After(predecessor.CreatedAt) || (run.CreatedAt.Equal(predecessor.CreatedAt) && run.ID > predecessor.ID) {
			predecessor = run
			found = true
		}
	}
	if found {
		return predecessor, true
	}
	if legacyFound && !legacyAmbiguous {
		return legacyPredecessor, true
	}
	return domain.ReviewRun{}, false
}

func replaceReviewLatestRun(reviews []PRReviewState, prURL, targetSHA string, run domain.ReviewRun) []PRReviewState {
	for i := range reviews {
		if reviews[i].PRURL == prURL && reviews[i].TargetSHA == targetSHA {
			reviews[i].LatestRun = &run
			if run.Status == domain.ReviewRunRunning {
				reviews[i].Status = ReviewStateRunning
			}
			break
		}
	}
	return reviews
}

func firstReusableRun(reviews []PRReviewState) domain.ReviewRun {
	// Legacy compatibility only: in the multi-PR model the authoritative state
	// is Reviews. When no run is created, this field is just a best-effort
	// non-empty run for older clients.
	for _, review := range reviews {
		if review.LatestRun != nil {
			return *review.LatestRun
		}
	}
	return domain.ReviewRun{}
}

// List returns a worker's review state: the live reviewer handle and its passes.
func (e *Engine) List(ctx stdctx.Context, workerID domain.SessionID) (SessionReviews, error) {
	if workerID == "" {
		return SessionReviews{}, fmt.Errorf("%w: worker session id is required", ErrInvalid)
	}
	worker, ok, err := e.sessions.GetSession(ctx, workerID)
	if err != nil {
		return SessionReviews{}, err
	}
	if !ok {
		return SessionReviews{}, fmt.Errorf("%w: worker session %q", ErrNotFound, workerID)
	}
	selectedHarness, _, err := e.reviewerSelection(ctx, worker)
	if err != nil {
		return SessionReviews{}, err
	}
	return e.listLocked(ctx, workerID, selectedHarness)
}

func (e *Engine) listLocked(ctx stdctx.Context, workerID domain.SessionID, selectedHarness domain.ReviewerHarness) (SessionReviews, error) {
	runs, err := e.store.ListReviewRunsBySession(ctx, workerID)
	if err != nil {
		return SessionReviews{}, err
	}
	var handle string
	reviewerHarness := selectedHarness
	var activityState domain.ActivityState
	if review, ok, err := e.store.GetReviewBySessionAndHarness(ctx, workerID, selectedHarness); err != nil {
		return SessionReviews{}, err
	} else if ok && review.ReviewerHandleID != "" {
		handle = review.ReviewerHandleID
		reviewerHarness = review.Harness
		activityState = review.ReviewerActivityState
	} else if review, ok, err := e.store.GetReviewBySession(ctx, workerID); err != nil {
		return SessionReviews{}, err
	} else if ok && review.ReviewerHandleID != "" {
		handle = review.ReviewerHandleID
		reviewerHarness = review.Harness
		activityState = review.ReviewerActivityState
	} else if review, ok, err := e.store.GetReviewBySessionAndHarness(ctx, workerID, selectedHarness); err != nil {
		return SessionReviews{}, err
	} else if ok {
		activityState = review.ReviewerActivityState
	}
	prs, err := e.prs.ListPRsBySession(ctx, workerID)
	if err != nil {
		return SessionReviews{}, err
	}
	return SessionReviews{
		ReviewerHandleID:      handle,
		ReviewerHarness:       reviewerHarness,
		ReviewerActivityState: activityState,
		Runs:                  runs,
		Reviews:               Plan(prs, runs),
	}, nil
}

// Cancel interrupts the live reviewer pane for a worker and marks running
// review runs as cancelled so they no longer block a fresh trigger.
func (e *Engine) Cancel(ctx stdctx.Context, workerID domain.SessionID) (CancelResult, error) {
	if workerID == "" {
		return CancelResult{}, fmt.Errorf("%w: worker session id is required", ErrInvalid)
	}
	worker, ok, err := e.sessions.GetSession(ctx, workerID)
	if err != nil {
		return CancelResult{}, err
	}
	if !ok {
		return CancelResult{}, fmt.Errorf("%w: worker session %q", ErrNotFound, workerID)
	}
	harness, _, err := e.reviewerSelection(ctx, worker)
	if err != nil {
		return CancelResult{}, err
	}
	running, err := e.store.ListRunningReviewRunsBySession(ctx, workerID)
	if err != nil {
		return CancelResult{}, err
	}
	if len(running) == 0 {
		review, ok, err := e.currentReviewForSession(ctx, workerID, harness)
		if err != nil {
			return CancelResult{}, err
		}
		handle := ""
		if ok {
			handle = review.ReviewerHandleID
		}
		prs, err := e.prs.ListPRsBySession(ctx, workerID)
		if err != nil {
			return CancelResult{}, err
		}
		runs, err := e.store.ListReviewRunsBySession(ctx, workerID)
		if err != nil {
			return CancelResult{}, err
		}
		return CancelResult{ReviewerHandleID: handle, Reviews: Plan(prs, runs)}, nil
	}
	review, ok, err := e.currentReviewForCancel(ctx, workerID, harness, running)
	if err != nil {
		return CancelResult{}, err
	}
	if !ok || review.ReviewerHandleID == "" {
		return CancelResult{}, fmt.Errorf("%w: reviewer for worker session %q", ErrNotFound, workerID)
	}
	if err := e.launcher.Cancel(ctx, review.ReviewerHandleID, review.Harness); err != nil {
		alive, aliveErr := e.launcher.Alive(ctx, review.ReviewerHandleID)
		if aliveErr != nil {
			return CancelResult{}, err
		}
		if alive {
			return CancelResult{}, err
		}
	}
	if _, err := e.store.CancelRunningReviewRunsBySessionAndHarness(ctx, workerID, review.Harness, "cancelled by user"); err != nil {
		return CancelResult{}, err
	}
	cancelled := make([]domain.ReviewRun, 0, len(running))
	for _, run := range running {
		if run.Harness != review.Harness && run.Harness != "" {
			continue
		}
		run.Status = domain.ReviewRunCancelled
		run.Verdict = domain.VerdictNone
		run.Body = "cancelled by user"
		run.GithubReviewID = ""
		cancelled = append(cancelled, run)
	}
	prs, err := e.prs.ListPRsBySession(ctx, workerID)
	if err != nil {
		return CancelResult{}, err
	}
	runs, err := e.store.ListReviewRunsBySession(ctx, workerID)
	if err != nil {
		return CancelResult{}, err
	}
	return CancelResult{ReviewerHandleID: review.ReviewerHandleID, Reviews: Plan(prs, runs), CancelledRuns: cancelled}, nil
}

func (e *Engine) currentReviewForCancel(ctx stdctx.Context, workerID domain.SessionID, selected domain.ReviewerHarness, running []domain.ReviewRun) (domain.Review, bool, error) {
	if len(running) > 0 {
		if selected != "" {
			for _, run := range running {
				if run.Harness != selected && run.Harness != "" {
					continue
				}
				review, ok, err := e.store.GetReviewBySessionAndHarness(ctx, workerID, selected)
				if err != nil || (ok && review.ReviewerHandleID != "") {
					return review, ok, err
				}
				break
			}
		}
		for _, run := range running {
			if run.Harness == "" || run.Harness == selected {
				continue
			}
			review, ok, err := e.store.GetReviewBySessionAndHarness(ctx, workerID, run.Harness)
			if err != nil || (ok && review.ReviewerHandleID != "") {
				return review, ok, err
			}
		}
	}
	return e.currentReviewForSession(ctx, workerID, selected)
}

func (e *Engine) currentReviewForSession(ctx stdctx.Context, workerID domain.SessionID, selected domain.ReviewerHarness) (domain.Review, bool, error) {
	if selected != "" {
		review, ok, err := e.store.GetReviewBySessionAndHarness(ctx, workerID, selected)
		if err != nil || ok {
			return review, ok, err
		}
	}
	return e.store.GetReviewBySession(ctx, workerID)
}

// TerminateReviewer destroys the live reviewer pane for a worker and cancels
// any running review runs. Unlike Cancel, this does not ask the reviewer
// adapter for a graceful interrupt sequence: worker termination/restore must
// remove the terminal pane itself.
func (e *Engine) TerminateReviewer(ctx stdctx.Context, workerID domain.SessionID, body string) (TerminateResult, error) {
	if workerID == "" {
		return TerminateResult{}, fmt.Errorf("%w: worker session id is required", ErrInvalid)
	}
	unlock := e.lockWorker(workerID)
	defer unlock()
	reviews, err := e.store.ListReviewsBySession(ctx, workerID)
	if err != nil {
		return TerminateResult{}, err
	}
	running, err := e.store.ListRunningReviewRunsBySession(ctx, workerID)
	if err != nil {
		return TerminateResult{}, err
	}
	destroyedHandle := ""
	for _, review := range reviews {
		if review.ReviewerHandleID == "" {
			continue
		}
		if err := e.launcher.Destroy(ctx, review.ReviewerHandleID); err != nil {
			return TerminateResult{}, err
		}
		if destroyedHandle == "" {
			destroyedHandle = review.ReviewerHandleID
		}
	}
	if len(reviews) > 0 {
		if err := e.store.ClearReviewerHandle(ctx, workerID); err != nil {
			return TerminateResult{}, err
		}
	}
	if body == "" {
		body = "cancelled by worker session lifecycle"
	}
	if _, err := e.store.CancelRunningReviewRunsBySession(ctx, workerID, body); err != nil {
		return TerminateResult{}, err
	}
	cancelled := make([]domain.ReviewRun, 0, len(running))
	for _, run := range running {
		run.Status = domain.ReviewRunCancelled
		run.Verdict = domain.VerdictNone
		run.Body = body
		run.GithubReviewID = ""
		cancelled = append(cancelled, run)
	}
	return TerminateResult{ReviewerHandleID: destroyedHandle, CancelledRuns: cancelled}, nil
}

// reviewerHarness resolves which harness reviews the worker's PR: a persisted
// session preference wins, then project configuration, then the worker's own
// harness when supported, otherwise claude-code.
func (e *Engine) reviewerSelection(
	ctx stdctx.Context,
	worker domain.SessionRecord,
) (domain.ReviewerHarness, domain.AgentConfig, error) {
	if worker.ReviewerHarness != "" || !worker.ReviewerConfig.IsZero() {
		harness := worker.ReviewerHarness
		projectHarness, projectConfig, err := e.projectReviewerSelection(ctx, worker)
		if err != nil {
			return "", domain.AgentConfig{}, err
		}
		if harness == "" {
			harness = projectHarness
		}
		baseConfig := domain.AgentConfig{}
		if harness == projectHarness {
			baseConfig = projectConfig
		}
		return harness, mergeReviewerAgentConfig(baseConfig, worker.ReviewerConfig), nil
	}
	return e.projectReviewerSelection(ctx, worker)
}

func mergeReviewerAgentConfig(base, override domain.AgentConfig) domain.AgentConfig {
	if override.Model != "" {
		base.Model = override.Model
	}
	if override.Effort != "" {
		base.Effort = override.Effort
	}
	if override.Mode != "" {
		base.Mode = override.Mode
	}
	if override.Permissions != "" {
		base.Permissions = override.Permissions
	}
	return base
}

func (e *Engine) projectReviewerSelection(
	ctx stdctx.Context,
	worker domain.SessionRecord,
) (domain.ReviewerHarness, domain.AgentConfig, error) {
	var cfg domain.ProjectConfig
	if e.projects != nil {
		if proj, ok, err := e.projects.GetProject(ctx, string(worker.ProjectID)); err != nil {
			return "", domain.AgentConfig{}, err
		} else if ok {
			cfg = proj.Config
		}
	}
	if len(cfg.Reviewers) > 0 {
		return cfg.Reviewers[0].Harness, cfg.Reviewers[0].AgentConfig, nil
	}
	return cfg.ResolveReviewerHarness(worker.Harness), domain.AgentConfig{}, nil
}

func (e *Engine) upsertReview(ctx stdctx.Context, worker domain.SessionRecord, harness domain.ReviewerHarness, handleID, agentSessionID, reviewerLaunchID string, activityState domain.ActivityState, now time.Time) (domain.Review, error) {
	existing, ok, err := e.store.GetReviewBySessionAndHarness(ctx, worker.ID, harness)
	if err != nil {
		return domain.Review{}, err
	}
	agentSessionID = strings.TrimSpace(agentSessionID)
	review := domain.Review{
		ID:                    e.newID(),
		SessionID:             worker.ID,
		ProjectID:             worker.ProjectID,
		Harness:               harness,
		PRURL:                 "",
		ReviewerHandleID:      handleID,
		AgentSessionID:        agentSessionID,
		ReviewerLaunchID:      strings.TrimSpace(reviewerLaunchID),
		ReviewerActivityState: activityState,
		CreatedAt:             now,
		UpdatedAt:             now,
	}
	if ok {
		// Reuse the existing row's identity and creation time; UpsertReview
		// refreshes harness/pr_url/reviewer_handle_id/updated_at.
		review.ID = existing.ID
		review.CreatedAt = existing.CreatedAt
		if review.ReviewerLaunchID == "" {
			review.ReviewerLaunchID = existing.ReviewerLaunchID
		}
		if review.ReviewerActivityState == "" {
			review.ReviewerActivityState = existing.ReviewerActivityState
		}
	}
	if err := e.store.UpsertReview(ctx, review); err != nil {
		return domain.Review{}, err
	}
	return review, nil
}

func (e *Engine) acceptReviewerReplacement(ctx stdctx.Context, incumbent, replacement domain.Review, predecessors []domain.ReviewRun, clearAgentSession bool, retiredCleanup domain.ReviewerCleanupObligation) error {
	if acceptor, ok := e.store.(ReviewerReplacementAcceptor); ok {
		return acceptor.AcceptReviewerReplacement(ctx, incumbent, replacement, predecessors, clearAgentSession, retiredCleanup)
	}
	return errors.New("atomic reviewer replacement persistence is unavailable")
}

func reviewerFailureBody(err error) string {
	if err == nil {
		return ""
	}
	body := err.Error()
	var effectErr ports.RuntimeEffectError
	if errors.As(err, &effectErr) && effectErr.CleanupOutcome() != ports.RuntimeCleanupSucceeded {
		if handleID := strings.TrimSpace(effectErr.PossibleHandle().ID); handleID != "" && !strings.Contains(body, reviewerCleanupPendingPrefix) {
			marker := reviewerCleanupPendingMarker(handleID)
			body += "\n" + marker
		}
	}
	return body
}

func (e *Engine) retryPendingReviewerCleanup(ctx stdctx.Context, sessionID domain.SessionID, runs []domain.ReviewRun) error {
	var cleanupErr error
	reviews, err := e.store.ListReviewsBySession(ctx, sessionID)
	if err != nil {
		return fmt.Errorf("list reviewer cleanup authorities for %q: %w", sessionID, err)
	}
	if store, ok := e.store.(ReviewerCleanupObligationStore); ok {
		obligations, err := store.ListUnresolvedReviewerCleanupObligations(ctx, sessionID)
		if err != nil {
			return fmt.Errorf("list reviewer cleanup obligations for %q: %w", sessionID, err)
		}
		for _, obligation := range obligations {
			authorityMatches := reviewerCleanupAuthorityMatches(reviews, obligation.SessionID, obligation.Harness, obligation.ReviewID, obligation.LaunchID, obligation.HandleID)
			newerGenerations := reviewerCleanupNewerRunningGenerations(runs, obligation.SessionID, obligation.Harness, obligation.ReviewID, obligation.LaunchID)
			destroy, resolve, decisionErr := e.reviewerCleanupDecision(ctx, authorityMatches, obligation.HandleID, obligation.LaunchID, newerGenerations)
			if decisionErr != nil {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("reviewer cleanup obligation %q: %w", obligation.ID, decisionErr))
				continue
			}
			if destroy {
				if err := e.launcher.Destroy(stdctx.WithoutCancel(ctx), obligation.HandleID); err != nil {
					cleanupErr = errors.Join(cleanupErr, fmt.Errorf("retry reviewer cleanup obligation %q handle %q: %w", obligation.ID, obligation.HandleID, err))
					continue
				}
			}
			if resolve {
				if cleared, err := store.ResolveReviewerCleanupObligation(stdctx.WithoutCancel(ctx), obligation.ID, e.clock()); err != nil {
					cleanupErr = errors.Join(cleanupErr, fmt.Errorf("clear reviewer cleanup obligation %q: %w", obligation.ID, err))
				} else if !cleared {
					cleanupErr = errors.Join(cleanupErr, fmt.Errorf("reviewer cleanup obligation %q was not unresolved", obligation.ID))
				}
			}
		}
	}
	markerStore, hasMarkerStore := e.store.(ReviewerCleanupMarkerStore)
	seen := make(map[string]struct{})
	for _, run := range runs {
		pendingBody := run.Body
		for {
			marker, handleID, launchID, ok := reviewerCleanupPendingIdentity(pendingBody)
			if !ok {
				if strings.Contains(pendingBody, reviewerCleanupPendingPrefix) {
					cleanupErr = errors.Join(cleanupErr, fmt.Errorf("review run %q malformed cleanup marker retained because identity is unavailable", run.ID))
				}
				break
			}
			pendingBody = strings.ReplaceAll(pendingBody, marker, "")
			key := run.ID + "\x00" + marker
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			if !hasMarkerStore {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("review run %q cleanup marker cannot be reconciled without durable marker storage", run.ID))
				continue
			}
			authorityMatches := reviewerCleanupAuthorityMatches(reviews, run.SessionID, run.Harness, run.ReviewID, launchID, handleID)
			newerGenerations := reviewerCleanupNewerRunningGenerations(runs, run.SessionID, run.Harness, run.ReviewID, launchID)
			destroy, resolve, decisionErr := e.reviewerCleanupDecision(ctx, authorityMatches, handleID, launchID, newerGenerations)
			if decisionErr != nil {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("review run %q cleanup marker: %w", run.ID, decisionErr))
				continue
			}
			if destroy {
				if err := e.launcher.Destroy(stdctx.WithoutCancel(ctx), handleID); err != nil {
					cleanupErr = errors.Join(cleanupErr, fmt.Errorf("retry reviewer cleanup for run %q handle %q: %w", run.ID, handleID, err))
					continue
				}
			}
			if resolve {
				clearedBody := strings.ReplaceAll(run.Body, marker, "")
				if launchID != "" {
					// These legacy duplicates refer to the same handle. Clear them
					// only after its generation-bound cleanup is proven released.
					legacy := reviewerCleanupPendingMarker(handleID)
					clearedBody = strings.ReplaceAll(clearedBody, legacy, "")
					pendingBody = strings.ReplaceAll(pendingBody, legacy, "")
				}
				clearedBody = strings.TrimSpace(clearedBody)
				if cleared, err := markerStore.ResolveReviewRunCleanupMarker(stdctx.WithoutCancel(ctx), run.ID, marker, clearedBody); err != nil {
					cleanupErr = errors.Join(cleanupErr, fmt.Errorf("clear reviewer cleanup marker for run %q: %w", run.ID, err))
				} else if !cleared {
					cleanupErr = errors.Join(cleanupErr, fmt.Errorf("clear reviewer cleanup marker for run %q: marker was not unresolved", run.ID))
				} else {
					run.Body = clearedBody
				}
			}
			if !destroy && !resolve {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("review run %q cleanup marker retained because generation authority is unavailable", run.ID))
			}
		}
	}
	return cleanupErr
}

// reviewerCleanupDecision establishes exact runtime-generation authority before
// permitting any destructive cleanup. A newer running review generation is
// probed first: while it is live but not yet durably adopted, cleanup remains
// unresolved; once the durable review authority advances, the old obligation
// can clear without destroying the reused handle. A dead exact generation is
// already gone. Every other uncertainty remains retryable and fail-closed.
func (e *Engine) reviewerCleanupDecision(ctx stdctx.Context, authorityMatches bool, handleID, launchID string, newerGenerations []string) (destroy, resolve bool, err error) {
	prober, ok := e.launcher.(ReviewerGenerationProber)
	if !ok {
		return false, false, errors.New("exact reviewer runtime-generation authority is unavailable")
	}
	for _, newerGeneration := range newerGenerations {
		probe := prober.ProbeReviewerGeneration(ctx, handleID, newerGeneration)
		if probe.Liveness != ports.FencedAlive || probe.Reason != ports.FencedReasonExactMatch {
			continue
		}
		if authorityMatches {
			return false, false, fmt.Errorf("launch-generation fence: newer reviewer generation %q is live but not durably adopted", newerGeneration)
		}
		return false, true, nil
	}
	probe := prober.ProbeReviewerGeneration(ctx, handleID, launchID)
	switch {
	case authorityMatches && probe.Liveness == ports.FencedAlive && probe.Reason == ports.FencedReasonExactMatch:
		return true, true, nil
	case probe.Liveness == ports.FencedDead && probe.Reason == ports.FencedReasonExactAbsent:
		return false, true, nil
	case !authorityMatches && probe.Liveness == ports.FencedUnknown && probe.Reason == ports.FencedReasonGenerationMismatch:
		return false, true, nil
	default:
		if !authorityMatches {
			return false, false, fmt.Errorf("launch-generation fence: generation authority unavailable (durable_match=%t liveness=%s reason=%s)", authorityMatches, probe.Liveness, probe.Reason)
		}
		return false, false, fmt.Errorf("generation authority unavailable (durable_match=%t liveness=%s reason=%s)", authorityMatches, probe.Liveness, probe.Reason)
	}
}

func reviewerCleanupNewerRunningGenerations(runs []domain.ReviewRun, sessionID domain.SessionID, harness domain.ReviewerHarness, reviewID, launchID string) []string {
	if strings.TrimSpace(reviewID) == "" {
		return nil
	}
	seen := make(map[string]struct{})
	var generations []string
	for _, run := range runs {
		generation := strings.TrimSpace(run.LaunchID)
		if run.SessionID != sessionID || run.Harness != harness || run.ReviewID != reviewID || run.Status != domain.ReviewRunRunning || generation == "" || generation == strings.TrimSpace(launchID) {
			continue
		}
		if _, ok := seen[generation]; ok {
			continue
		}
		seen[generation] = struct{}{}
		generations = append(generations, generation)
	}
	return generations
}

func reviewerCleanupAuthorityMatches(reviews []domain.Review, sessionID domain.SessionID, harness domain.ReviewerHarness, reviewID, launchID, handleID string) bool {
	if strings.TrimSpace(launchID) == "" || strings.TrimSpace(handleID) == "" {
		return false
	}
	for _, review := range reviews {
		if review.SessionID != sessionID || (harness != "" && review.Harness != harness) {
			continue
		}
		if reviewID != "" {
			if review.ID != reviewID {
				continue
			}
		} else if strings.TrimSpace(review.ReviewerHandleID) != strings.TrimSpace(handleID) {
			continue
		}
		return strings.TrimSpace(review.ReviewerLaunchID) == strings.TrimSpace(launchID)
	}
	return false
}

func reviewerFailureBodyForLaunch(err error, launchID string) string {
	body := reviewerFailureBody(err)
	handleID, ok := reviewerCleanupHandleFromError(err)
	if !ok || strings.TrimSpace(launchID) == "" {
		return body
	}
	var binding reviewerCleanupGenerationBinding
	if errors.As(err, &binding) && binding.CleanupGenerationBound() {
		return body
	}
	legacy := reviewerCleanupPendingMarker(handleID)
	generation := reviewerCleanupPendingMarkerForGeneration(handleID, launchID)
	return strings.Replace(body, legacy, generation, 1)
}
