# AO Bootstrap

**Status:** Temporary operational runbook  
**Date:** 2026-09-18  
**Repository:** `kou-keirei/agent-orchestrator`  
**Intended destination:** `docs/runbooks/ao-bootstrap.md`

## Purpose

This runbook governs repair and port-forward work while the currently installed AO control plane is not trusted to supervise its own repair.

The bootstrap condition exists because observed AO failures occurred in the same orchestration surfaces being used to repair AO:

- parent orchestrator did not reliably re-engage after child completion;
- worker/reviewer evidence was misclassified;
- missing control-plane evidence triggered false review failure;
- false review failure risked unnecessary implementation repair;
- worker-local environment observations were treated as authoritative even when deterministic host evidence existed;
- long-lived AO Boss prompts retained stale acceptance rules.

Therefore, until the exit criteria in this document are met:

> **AO Boss is not a trusted policy/controller for AO-fork repair.**

AO may still provide worker execution primitives and worktree isolation.

---

## Bootstrap Control Model

Temporary control flow:

```text
Human / ChatGPT
        ↓
explicit bounded task specification
        ↓
independent AO worker
        ↓
worker result + Git/runtime evidence
        ↓
Human / ChatGPT deterministic reconciliation
        ↓
next bounded task
```

During bootstrap, DO NOT use AO Boss to independently:

- decide whether a reviewer failure implies a source defect;
- decide whether to spawn a repair worker;
- terminate an otherwise valid worker because evidence is missing;
- reinterpret known environment caveats;
- widen scope;
- elevate model/effort;
- authorize remote/destructive effects.

AO Boss may remain quarantined/terminated while fork repair is active.

---

## Authority Model

The bootstrap lane may perform only actions explicitly approved by the user.

### Local read-only inspection

Allowed when scoped:

- inspect Git state;
- inspect worktrees;
- inspect AO sessions;
- inspect branches/refs;
- inspect upstream/fork PRs/issues/commits;
- compare source;
- read logs/evidence.

### Local cleanup

Requires an approved cleanup scope.

Cleanup MUST distinguish:

```text
clean redundant state
vs.
dirty/historical evidence
```

Dirty or ambiguous worktrees are preservation objects, not garbage.

### Local mutation

Implementation workers may mutate only their explicitly assigned AO-created worktree and source scope.

### Commit

Commit only when specifically authorized for that bounded implementation lane and after required validation.

### Remote mutation

No push, PR mutation, merge, rebase, history rewrite, release publication, or other remote mutation unless separately authorized.

### Installation/cutover

No install/restart/reboot/cutover merely because a candidate builds.

Cutover requires a fresh explicit gate.

---

## Phase 1 — Reconcile and Clean Existing AO State

Before spawning new reconstruction/recon workers:

1. snapshot live AO sessions;
2. snapshot Git worktrees;
3. record clean/dirty state;
4. record HEAD and branch for each relevant worktree;
5. record protected/archive refs;
6. identify the latest accepted integration candidate;
7. create archive refs for accepted commits not already durably named, when authorized;
8. terminate/quarantine AO Boss;
9. terminate completed/idle clean sessions that are no longer needed, when authorized;
10. remove only redundant clean worktrees, when authorized;
11. preserve dirty or historically unique worktrees;
12. preserve conversations/evidence needed for reconstruction.

### Preservation rule

DO NOT clean, reset, restore, delete, or reuse a dirty worktree merely because its branch is old.

Preserve first. Reconcile later.

A durable commit/archive ref is preferred before deleting any clean redundant worktree.

---

## Phase 2 — Establish Permanent Maintained Fork Checkout

Canonical target:

```text
C:\Users\Bez Wong\workspace\projects\agent-orchestrator
```

Expected remotes:

```text
origin   → kou-keirei/agent-orchestrator
upstream → Untrivial-ai/agent-orchestrator
```

Before using the checkout:

- verify the fork relationship;
- verify remote URLs;
- verify default branch;
- fetch current upstream/fork refs;
- verify clean Git state;
- verify exact current SHAs;
- preserve the old temporary checkout until reconciliation is complete.

Do not silently replace or delete the historical checkout.

---

## Phase 3 — Independent Port-Forward Recon

Recon workers are independent workers, not AO Boss subordinates.

They are read-only. They do not mutate source. They do not commit. They do not push.

They produce evidence-backed classifications and recommendations.

### Worker A — Lifecycle Recon

**Model:** GPT-5.6 Luna  
**Effort:** high  
**Authority:** read-only

Primary upstream targets:

- durable orchestration event outbox;
- durable worker report delivery;
- historical worker-idle→orchestrator delivery;
- historical idle-orchestrator re-engagement;
- revert/negative prior art for duplicate re-engagement;
- interrupted orchestrator spawn recovery;
- orchestrator dead-probe / continuity fixes.

For each candidate, classify:

```text
UPSTREAM_EQUIVALENT
PARTIAL_OVERLAP
DOWNSTREAM_ONLY
UPSTREAM_SUPERSEDES
NEGATIVE_PRIOR_ART
DEPENDENCY
```

Worker A must determine:

- what upstream already solves;
- what upstream intentionally does not solve;
- what was previously reverted and why;
- what dependencies are required;
- what our downstream fork still needs;
- safest port/reconcile order;
- lifecycle acceptance tests required afterward.

Worker A MUST NOT assume “idle” equals “task complete.”

Worker A MUST preserve the AO invariant that durable lifecycle truth and provider-turn wakeups are distinct concerns.

---

### Worker B — Execution Contract / Permission Recon

**Model:** GPT-5.6 Luna  
**Effort:** high  
**Authority:** read-only

Primary upstream targets include:

- preventive read-only permission support;
- permission-mode issue/requirements;
- canonical AO executable work;
- Codex model/effort selection;
- current effort-propagation work;
- current downstream maintained candidate and accepted repair slices.

Worker B must trace:

```text
manual UI selection
        ↓
frontend typed permission/model/effort
        ↓
spawn/task payload
        ↓
backend domain state
        ↓
delegation inheritance/capping
        ↓
provider launch
        ↓
provider-confirmed effective state
        ↓
turn update
        ↓
resume/reconnect/restore
        ↓
provider/interface transition
```

Worker B must explicitly audit:

### Permission

- omitted child permission inheritance;
- non-broadening;
- fixed read-only;
- provider-confirmed effective permission;
- stale evidence invalidation;
- reconnect/resume/restore;
- Chat↔TUI;
- provider switch;
- queued turn/handoff behavior;
- manual-start UI visibility and fidelity.

### Model / effort

- omitted vs explicit-empty vs explicit-nonempty;
- frontend/API/storage fidelity;
- delegation;
- provider launch;
- first-turn binding;
- resume/restore;
- persistent-host behavior.

### Runtime identity

- canonical AO executable;
- login-shell/PATH override resistance;
- workspace identity;
- environment/toolchain evidence.

For each relevant downstream change, classify:

```text
KEEP_DOWNSTREAM
DROP_FOR_UPSTREAM
PORT_UPSTREAM
RECONCILE
SUPERSEDED
NEEDS_NEW_IMPLEMENTATION
```

---

## Phase 4 — Human/ChatGPT Reconciliation

AO Boss does not normalize recon results.

Human/ChatGPT compares Worker A and Worker B evidence and decides the implementation plan.

The normalization output should state:

- exact upstream commits/PRs to port or reproduce;
- exact downstream commits/semantics to keep;
- conflicts/overlaps;
- negative prior art to preserve as tests;
- dependency order;
- first bounded implementation slice;
- acceptance tests for that slice.

No implementation worker should be dispatched from a generic reviewer `FAIL`.

A concrete source/runtime defect must be named.

---

## Phase 5 — Bounded Independent Implementation

Use independent AO workers for bounded implementation.

Default implementer:

```text
GPT-5.6 Luna
```

Use the lowest reliable effort appropriate to the slice.

Each worker receives:

- exact base SHA;
- exact worktree;
- exact source scope;
- exact upstream/downstream inputs;
- exact invariants;
- exact validation;
- commit authority status;
- prohibited effects.

AO Boss remains outside the control loop.

---

## Review During Bootstrap

Independent reviewers inspect:

- exact candidate SHA;
- exact scoped diff;
- source semantics;
- relevant supplied runtime evidence;
- provenance;
- non-mutation state.

Reviewers do not self-attest control-plane facts.

Negative review outcomes must be normalized before repair:

```text
SOURCE_DEFECT
EVIDENCE_INCOMPLETE
ENVIRONMENT_BLOCKED
CONTROL_PLANE_BLOCKED
AUTHORITY_BLOCKED
REVIEW_PROTOCOL_FAILURE
```

Only a concrete `SOURCE_DEFECT` justifies a new source-repair lane.

---

## Known Caveat Handling

A previously classified environmental caveat must not silently become a source blocker in a later worker/reviewer.

Examples may include:

- Windows fixtures written for POSIX-only paths;
- missing optional local dependency installation;
- fixture assumptions requiring Git Bash/AO context;
- tests requiring an installed app/runtime not present in an isolated worktree.

A later worker may challenge a caveat only with new contradictory evidence.

During bootstrap, caveats are recorded by Human/ChatGPT until ECP provides the deterministic registry.

---

## Exit Criteria — When AO Boss May Be Trusted Again

Bootstrap mode ends only after the maintained AO fork is installed and proves the relevant AO invariants natively.

### Execution truth

- model/effort native evidence works;
- permission/sandbox evidence works;
- session/thread/turn correlation works;
- canonical executable/runtime identity works.

### Permission

- parent→child non-broadening passes;
- omitted child permission inherits safely;
- fixed read-only cannot broaden;
- resume/reconnect/restore passes;
- manual-start UI exposes and correctly launches read-only;
- UI reflects effective/fixed permission.

### Lifecycle

- child report/checkpoint persists;
- child completion persists;
- parent re-engages without manual `ao send`;
- duplicate delivery is idempotent/coalesced;
- daemon restart does not silently lose accepted lifecycle facts.

### Git/worktree

- exact base/worktree lease integrity passes;
- unsafe rebind/reuse fails closed.

### Review

- reviewers consume centrally/native supplied evidence;
- missing reviewer-local evidence cannot become a source defect automatically;
- typed result semantics survive child→parent transport.

### Cold-start acceptance

After installation:

1. cold start AO;
2. verify serving health separately from provider readiness;
3. verify provider readiness;
4. verify model/effort dispatch;
5. verify preventive read-only;
6. run native late-child-completion regression;
7. verify parent automatically re-engages and consumes the result idempotently.

Only after these checks should AO Boss become a trusted orchestrator/controller again.

---

## Relationship to ECP

This bootstrap runbook is temporary.

ECP will later own deterministic authorization, gate evaluation, evidence completeness, failure normalization/routing, repair admission, known-caveat registry, retries/budgets, audit receipts, and remote/destructive-effect constraints.

The bootstrap process manually approximates only the minimum control needed to repair AO safely.

Once ECP is operational, this manual scaffolding should shrink substantially.

---

## Canonical Bootstrap Rule

> **Until AO proves its own invariants, use independent AO workers as execution units and keep orchestration judgment outside AO Boss. Preserve evidence, port proven upstream work first, and never convert missing control-plane evidence into an automatic source-repair loop.**
