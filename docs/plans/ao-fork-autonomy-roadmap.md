# AO Fork Autonomy Capability Roadmap

## Purpose

This roadmap gives the AO fork one durable altitude gauge for the work that matters most: trustworthy unattended orchestration over long-running and overnight execution.

It exists to answer four questions after every FULLWAVE:

1. What can AO reliably do now?
2. Which capability moved in this wave?
3. How far are we from the next meaningful milestone?
4. Which hidden plumbing actually changed a user-level or system-level capability?

This is a fork-local planning and qualification document. It does not redefine upstream Agent Orchestrator product truth. Runtime/API/source truth still comes from the code, generated contracts, tests, review evidence, and integrated repository state.

The machine-readable companion is [`ao-autonomy-readiness.json`](ao-autonomy-readiness.json).

## Primary readiness numbers

Track two percentages.

### Qualified Reach

**Qualified Reach** answers:

> How much of the overnight-autonomy capability stack is on the intended current/canonical path and has crossed the proof gates required for its current maturity stage?

Qualified Reach is the number used for milestone progress.

### Build Reach

**Build Reach** answers:

> How much capability has credible local implementation evidence somewhere in the fork, including preserved reviewed donors and bounded candidates that still need current-base port, review, or integration?

Build Reach is intentionally higher than Qualified Reach when useful work exists on divergent or historical lineages.

A worker launch does not increase either score. A score changes only when durable evidence advances a capability maturity stage.

## Current baseline

Snapshot: **2026-10-06, W76 launched**

- **Qualified Reach: 44.8%**
- **Build Reach: 53.9%**
- **Current milestone: M2, Recoverable Lifecycle**
- **Next milestone: M3, Durable Work Lifecycle**
- **M3 score threshold: 45%**
- **Distance to M3 by score: 0.2 percentage points**
- **Session Resume qualified stage: 60% (fresh-review source qualified; current-tip semantic port active)**
- **Lawnmower qualified stage: 25% (W76 reviewer-lifecycle F3 repair active; immediate M3 hard gate)**
- **Runtime permission attestation: source Slice A fresh-review qualified; W76 current-tip port active**
- **Tier1 RECOVERY composition: fresh-review PASS; isolated runtime smoke remains a separate explicit authority gate**

W76 critical lanes:

- Lawnmower F3 repair: gent-orchestrator-685
- Runtime permission attestation current-tip port: gent-orchestrator-686
- Session Resume current-tip semantic port: gent-orchestrator-687
- CLI/Hook Identity current-tip semantic port: gent-orchestrator-688
- Provider-event retention #16-A current-tip port: gent-orchestrator-689

The operational objective is no longer broad fork archaeology. It is to complete the self-propelling lifecycle loop required for M4/M5.


## Milestone ladder

| Milestone | Score band | Name | System reach |
| --- | ---: | --- | --- |
| M0 | 0 to <15% | Executor | Spawn bounded workers, run tasks, retain basic session/worktree state. |
| M1 | 15 to <30% | Truthful Lifecycle | Exact identity/base/evidence/authority boundaries are reliable enough to reason about execution truth. |
| M2 | 30 to <45% | Recoverable Lifecycle | AO can recover lifecycle failures, preserve candidates, resume owners, and safely reason about resource release. |
| M3 | 45 to <65% | Durable Work Lifecycle | Logical work identity and ownership survive turns/restarts; leases, reports, and durable lifecycle facts begin composing into a work system. |
| M4 | 65 to <85% | Autonomous Orchestration | Exact parent re-engagement, dependency-aware admission, backpressure, and unattended continuation form a reliable orchestration loop. |
| M5 | 85 to 100% | Overnight AO | Multi-hour chaos qualification proves restart, provider, worker, review, cleanup, recovery, and resource-pressure behavior without fabricated terminal truth. |

The score band is necessary but not sufficient. Milestone hard gates below prevent averaging from hiding a missing critical capability.

## Capability maturity ruler

Every weighted capability uses the same fixed maturity stages.

| Stage | Value | Meaning |
| --- | ---: | --- |
| UNMAPPED | 0% | Capability is not yet durably specified or tracked. |
| CONTRACT_FROZEN | 15% | Outcome, ownership, invariants, and acceptance contract are durable. |
| IMPLEMENTATION_PACKET_READY | 25% | Source-backed implementation scope and verification packet are actionable. |
| CURRENT_BASE_CANDIDATE_GREEN | 45% | Candidate is green on its authorized current base under deterministic local evaluation. |
| FRESH_REVIEW_PASS | 60% | Fresh independent review passed against the exact candidate/evidence. |
| CANONICAL_INTEGRATION_GREEN | 75% | Capability is integrated on the declared canonical fork path and integration validation is green. |
| REAL_E2E_RUNTIME_DOGFOOD_PASS | 90% | Real disposable/runtime end-to-end behavior passed, not merely source/unit checks. |
| CHAOS_PRODUCTION_CLASS_QUALIFIED | 100% | Required failure/restart/resource-pressure/chaos acceptance is green for the capability. |

### Qualified versus Build stage

For **Qualified Reach**, use only the strongest maturity stage supported on the intended current/canonical qualification path.

For **Build Reach**, use the strongest credible durable evidence available anywhere in the fork, including preserved reviewed donor candidates that still need port/requalification.

Do not promote model prose, worker self-report, a launch receipt, or an unreviewed historical branch into a higher stage.

## Weighted capability tracker

The weights sum to 100. Overall reach is:

`sum(capability_weight × capability_stage) / 100`

| Macro capability | Weight | Qualified | Build | Primary issue families | Current meaning |
| --- | ---: | ---: | ---: | --- | --- |
| Proof / admission foundation | 10% | 75% | 75% | AO #13, accepted #14, #2 closeout, #22 spine reconciliation | Exact-base, evidence, review, authority, and canonical-base foundations are substantial; effective runtime attestation remains a separate gap. |
| Spawn recovery + lifecycle continuity | 12% | 60% | 75% | AO #3, #4 | Repaired RECOVERY passed fresh review; W62 is porting it to current upstream. Broader #4 continuity still needs current-native qualification. |
| Session Resume | 12% | 25% | 45% | AO #21 + ECP #16 | Architecture is frozen. W62 implements Chat-first AO v1 and canonical ECP `session-resume` compiler/staging. TUI is explicitly unsupported in v1. |
| Lawnmower / REAP / FINALIZE | 14% | 25% | 60% | AO #20 -> #15 + ECP #28 | Donor/reconciliation work exists and a REAP candidate exists, but current-base native REAP and FINALIZE qualification are not complete. |
| Durable Work Identity + Execution Lease | 16% | 25% | 60% | AO #5 -> #6 | Strong donors exist. W62 ports #5 onto current upstream. #6 wakes after #5 acceptance. |
| Worker Reports + Event Outbox | 12% | 45% | 60% | AO #8 + #7 | Significant native/report/outbox behavior exists; governed work-level binding and final coexistence/integration remain incomplete. |
| Parent Re-engagement | 8% | 25% | 45% | AO #9 | Contract and prior art exist, but exact work-bound parent wake/recovery depends on #7/#8 plus Session Resume. |
| DAG / admission / backpressure | 8% | 15% | 25% | AO #10 | Contract exists; implementation remains downstream of durable work ownership/lease/re-engagement. |
| Overnight chaos qualification | 8% | 15% | 15% | AO #11 | Required chaos scenarios are frozen, but full composed overnight qualification has not started. |

Current weighted totals:

- Qualified Reach = **35.0%**
- Build Reach = **53.9%**, displayed as **54%**

## Milestone hard gates

### Enter M1: Truthful Lifecycle

Required:

- execution identity/base can be bound and checked;
- evidence distinguishes requested/observed/proven;
- missing proof does not become success.

### Enter M2: Recoverable Lifecycle

Required:

- Spawn Recovery at least `CURRENT_BASE_CANDIDATE_GREEN`;
- lifecycle uncertainty preserves evidence/candidates rather than fabricating terminal state;
- recoverable owner/session state is not silently replaced.

### Enter M3: Durable Work Lifecycle

In addition to Qualified Reach >=45%:

- Spawn Recovery >= `FRESH_REVIEW_PASS` on the current/canonical path;
- Session Resume >= `CURRENT_BASE_CANDIDATE_GREEN`;
- Lawnmower REAP/FINALIZE >= `CURRENT_BASE_CANDIDATE_GREEN`;
- Durable Work Identity >= `CURRENT_BASE_CANDIDATE_GREEN`;
- no unresolved hard authority/evidence conflict invalidates those capabilities.

### Enter M4: Autonomous Orchestration

In addition to Qualified Reach >=65%:

- Execution Lease >= `FRESH_REVIEW_PASS`;
- Reports + Outbox >= `FRESH_REVIEW_PASS`;
- Parent Re-engagement >= `FRESH_REVIEW_PASS`;
- Session Resume and Lawnmower have real end-to-end/runtime proof sufficient for unattended use;
- no duplicate-wake or stale-owner authority gap remains open.

### Enter M5: Overnight AO

In addition to Qualified Reach >=85%:

- DAG/admission/backpressure >= `FRESH_REVIEW_PASS`;
- overnight chaos scenarios >= `REAL_E2E_RUNTIME_DOGFOOD_PASS`;
- final composed chaos run proves daemon/provider/worker/reviewer/restart/resource-pressure behavior;
- no fabricated terminal facts, duplicate accepted completion, stale-owner reclaim, or unsafe cleanup;
- ECP may be disconnected for the required AO-native overnight acceptance scenario.

100% is reserved for the complete required overnight chaos contract, not "all current issues closed."

## Capability dependency spine

The main autonomy chain is:

```text
Proof / admission foundation
        |
        +--> Spawn Recovery + lifecycle continuity
        |
        +--> Session Resume
        |
        +--> Lawnmower: REAP -> FINALIZE
        |
        +--> #5 Durable Work Identity
                  |
                  v
             #6 Execution Lease
                  |
          +-------+-------+
          v               v
      #7 Outbox       #8 Reports
          \               /
           \             /
            v           v
             #9 Parent Re-engagement
                      |
                      v
             #10 DAG / Backpressure
                      |
                      v
              #11 Overnight Chaos
```

AO #18 Work Units / Capability-Grouped Session Roles is a product layer above the durable work kernel. It depends on #5, the AO evaluator/profile boundary, and Session Resume. It is important, but it is not separately weighted in the core Overnight Autonomy Readiness score.

## Supporting capabilities and hidden plumbing

Not every important change deserves a new top-level percentage bucket. Hidden plumbing advances the macro capability that owns the invariant.

Examples:

| Supporting capability / plumbing | Issue / surface | Counts toward |
| --- | --- | --- |
| Reviewer evidence closure | AO #2 | Proof / admission foundation |
| AO evaluator/profile | AO #13 | Proof / admission foundation |
| Daemon / SQLite compatibility admission | accepted AO #14 | Proof / admission foundation |
| Canonical integration-spine reconciliation | AO #22 | Proof / admission foundation and anti-duplication |
| Hide/Unhide lifecycle separation | AO #12 | Lifecycle correctness, but not a separate autonomy weight |
| Provider-event retention | AO #16 | Storage sustainability and overnight viability |
| Storage status / safe cleanup planning | AO #17 | Storage sustainability and overnight viability |
| Work Units / grouped roles | AO #18 | Product/orchestration layer above #5/#6/Session Resume |
| Exact-base admission | lifecycle/session/worktree plumbing | Proof foundation, Spawn Recovery, Session Resume |
| Input/writer exclusion | session manager/chat/controller plumbing | Session Resume |
| Generation fencing | runtime/controller/lifecycle plumbing | Recovery, Session Resume, Lawnmower, leases |
| Idempotency / delivery receipts | chat/report/outbox plumbing | Session Resume, Reports, Outbox, parent wake |
| Runtime process ownership | AO #20 | Lawnmower and overnight viability |
| Database retention / disk pressure | AO #16/#17 | Overnight viability |

A plumbing change changes the readiness score only when it advances the owning macro capability through a defined maturity gate.

## Critical path from M2 to Overnight AO

The serial autonomy spine is now:

`	ext
Lawnmower / ExecutionBaseSHA acceptance
        |
        v
#6 Execution Lease
        |
        +----------+
        v          v
   #7 Outbox   #8 Reports
        \          /
         \        /
          v      v
      #9 Parent Re-engagement
               |
               v
      #10 DAG / Backpressure
               |
               v
      #11 Overnight Chaos
`

Parallel M4 plumbing that must advance without stealing the serial spine:

- Session Resume current-tip/runtime/actuator qualification;
- native effective-permission attestation (ypass-permissions requested is not enough);
- storage sustainability (#16/#17);
- CLI/Hook identity and compatibility/migration maintenance;
- AO #13 only when a **production** ECP trusted-evaluator/profile contract exists.

### Immediate priorities

1. Close Lawnmower F3 and fresh-review the repaired current-tip lineage.
2. Wake #6 immediately after shared ExecutionBaseSHA/Lawnmower acceptance.
3. Reconcile/implement #7 + #8 as sibling durable completion/delivery substrates.
4. Implement #9 Parent Re-engagement using exact parent identity + Session Resume, never idle-means-wake.
5. Implement #10 dependency-aware admission/backpressure so resource pressure reduces admission instead of amplifying retries.
6. Run sustained multi-generation dogfood.
7. Qualify #11 Overnight Chaos and cross M5.

### Supporting/deferred rules

- #13 is not Step 0 for the serial autonomy spine while ECP's production evaluator contract is absent.
- accepted #14, #2/#12 closeout, historical migration maintenance, and storage work may run in parallel but must not preempt the serial spine unless they block source/runtime truth or safe operation.
- Tier1 RECOVERY source/composition work is complete enough for its next explicit gate: isolated runtime smoke.

A FULLWAVE should always ask: **what advances the serial autonomy spine, what unblocks the next lane, and what parallel maintenance can run without stealing critical resources?**


## Fullwave readiness reporting contract

Every AO FULLWAVE harvest must include a compact readiness section with:

```text
AO Autonomy Readiness

Qualified Reach: <before>% -> <after>% (<delta>)
Build Reach:     <before>% -> <after>% (<delta>)
Milestone:       <before> -> <after or unchanged>
Distance to next milestone: <points>

Capability deltas:
- <capability>: <stage before> -> <stage after> because <durable evidence>
- ...

No movement:
- <important capability>: <reason>

Newly unlocked:
- ...

Critical path:
1. ...
2. ...
```

A wave with useful implementation but no maturity-stage transition may legitimately report **0 percentage-point improvement**. The narrative should still say what risk was retired or what gate became reachable.

## Score update rules

1. Do not award points for worker creation, prompt acceptance, token spend, changed lines, test count, or elapsed effort.
2. Award a stage only when its exact evidence condition is durable.
3. Qualified Reach cannot use a historical donor merely because it once reviewed green.
4. Build Reach may use a preserved reviewed donor, but must say what prevents qualification.
5. A fresh repair does not retain the previous reviewer PASS automatically. The repaired candidate returns to the appropriate review stage until fresh review passes.
6. A current-base port must re-prove affected behavior. Byte-identical donor history is evidence, not automatic qualification.
7. Runtime/E2E stages require real isolated runtime evidence. Source tests cannot manufacture runtime proof.
8. Missing or ambiguous evidence never rounds upward.
9. A milestone cannot advance until both its score threshold and hard gates are satisfied.
10. When issue scope is superseded or split, preserve the capability weight. Do not make progress jump by renaming buckets.
11. Weights change only by an explicit roadmap revision with a recorded rationale, never to make the score look better.
12. Keep Qualified and Build scores separate in every report.

## How future waves update the tracker

Canonical machine state lives at:

`docs/plans/ao-autonomy-readiness.json`

After harvesting a FULLWAVE:

1. read the current tracker;
2. reconcile durable worker/review/integration/runtime evidence;
3. change only capability stages justified by that evidence;
4. recompute weighted Qualified Reach and Build Reach;
5. evaluate milestone hard gates;
6. record the wave ID and per-capability deltas;
7. report the before/after score in chat;
8. update this Markdown only when the roadmap, weights, milestone definitions, or capability ownership changes materially.

The JSON tracker is operational state. This Markdown is the durable scoring doctrine and roadmap.

## Current issue map

Core overnight-autonomy roadmap:

- AO #1: Trusted Unattended AO / Overnight Autonomy
- AO #3: Orchestrator Spawn Recovery
- AO #4: Orchestrator Liveness & Replacement Continuity
- AO #5: Durable Work Identity & Ownership
- AO #6: Execution Contract Lease
- AO #7: Durable Orchestration Event Outbox
- AO #8: Durable Worker Reports
- AO #9: Parent Re-engagement & Recovery
- AO #10: Dependency DAG, Admission & Backpressure
- AO #11: Overnight Chaos Acceptance
- AO #13: AO Adapter/Profile for ECP Trusted PStack Evaluator
- AO #15: FINALIZE / worktree reclamation
- AO #20: exact runtime REAP / process ownership
- AO #21: Session Resume
- AO #22: canonical integration-spine reconciliation

Supporting/product issues:

- AO #2: Reviewer Evidence Closure
- AO #12: Hide/Unhide
- AO #16: bounded provider-event retention
- AO #17: storage status / cleanup planning
- AO #18: Durable Work Units & Capability-Grouped Session Roles

Cross-repo capability partners:

- ECP #16: Session Resume consumer/admission/compiler/actuator
- ECP #28: Lawnmower semantic retirement policy/adapter
- ECP trusted-evaluator work: protected evaluation authority consumed by AO #13

## Baseline evidence

The initial score is calibrated from the fork-wide W61 reconciliation and W62 launch state, including:

- W61 fresh RECOVERY re-review PASS for `e35de7c97f05c119333f0e95ba4219d24eb2613e`;
- W61 AO #22 capability/spine audit;
- W61 Session Resume AO/ECP reconciliation;
- W62 launch receipt for AO 653/654/655 and ECP 64;
- current upstream source base at W62 launch: `dded9f11257083227221b4e47d8d32f446a290b5`;
- current ECP base at W62 launch: `343745c01b5d444d60fc487a2d1d746fa265b84b`.

These references establish the starting score. Future waves should advance the tracker from durable evidence rather than recalibrating the baseline from memory.
