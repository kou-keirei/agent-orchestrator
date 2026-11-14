# AO-Fork Full Wave Acceptance Registry

**Registry:** `ao-fork.acceptance.v1.0.0`
**State:** APPROVED/FROZEN definitions; native acceptance remains evidence-qualified
**Scope:** P0 and P1, unified BLD/INT; 11 P0 and 10 P1 evidence gates
**Definition source:** AO-Fork merged [PR #93](https://github.com/kou-keirei/agent-orchestrator/pull/93), `72cc284f30de8ac26186349e538d4893d3b5112b`
**Initial benchmark:** [2026-11-14 v1.0.0](baselines/2026-11-14-v1.md)
**Workspace policy:** ECP `main` `bc49f0c8b4d479b466a8d8a4d1643dff13e6a6e1`

## Authority and interpretation

This is an **approved, versioned `C/WS/G/V` acceptance definition**, not a second
issue tracker, stage machine, score ledger, evaluator, permission Gate or source
of runtime verdicts. The [existing roadmap](../plans/ao-fork-autonomy-roadmap.md)
and [readiness tracker](../plans/ao-autonomy-readiness.json) retain their nine
established macro-capability IDs, weighted maturity logic, and formal `M0–M5`
milestones. Their older snapshots are **not** current native acceptance scores.
Existing P0 F1–F5 functional evidence remains a separate historical witness,
not a replacement or denominator for these frozen gates. Original receipt:
`~/.ao/handoffs/agent-orchestrator/p0-unified-W1420-20261113/evidence/W1420-functional-scorecard.json`
(machine-local, not a portable repository link).

`C` = outcome capability; `WS` = accountable delivery workstream;
`G` = one falsifiable evidence criterion with a declared source;
`V` = a real native entrypoint → execution → observable result.
Issue IDs are owners and evidence pointers, never extra `G`s. A gate can
consume many issues/features; a feature can contribute to several gates.
`P0` and `P1` classify scope, **not** permission or worker custody.

Approval freezes definitions, **not** native PASS, a release, or M-promotion.
Version v1.0.0 fixes **11 P0 and 10 P1 unique gates**. The immutable [initial
benchmark](baselines/2026-11-14-v1.md) reports **0/21 proven** under the
bounded evidence available on 2026-11-14, not zero implementation. Future
progress requires matching source-bound receipts and qualified native
observations; unknown evidence is never PASS. Version changed criteria rather
than retroactively changing this denominator. The local/external obligation
ledger and Git/AO/CI/evaluator receipts retain execution truth.

Each gate has exactly one owning C in the tables below. Other capability
relationships are shared prerequisites, never additional ownership or a second
count. Repeated WS and V IDs deliberately reuse delivery/evidence surfaces;
sharing a witness does not prove every consuming gate.

This frozen v1.0.0 defines **10 C (nine weighted, one unweighted), 15 distinct
WS, 21 distinct G (11 P0, 10 P1), and 14 distinct V (7 P0, 7 P1)**.
The denominator is approved, and the [initial native benchmark](baselines/2026-11-14-v1.md)
is evidence-scoped. Neither merge nor an approved definition proves any V.

## C: capability index

The first nine IDs below are **reused verbatim** from the existing readiness
tracker; they are not newly renumbered. `storage-sustainability` is a new
**unweighted supporting** capability, not an extra autonomy weight.

| C ID | Outcome | Priority | Canonical issue family |
| --- | --- | --- | --- |
| `proof-admission-foundation` | Trusted source, authority, review and release identity | P0, P1 | [#22](https://github.com/kou-keirei/agent-orchestrator/issues/22), [#13](https://github.com/kou-keirei/agent-orchestrator/issues/13) |
| `spawn-recovery-lifecycle-continuity` | Recover exact owner without duplication | P0, P1 | [#3](https://github.com/kou-keirei/agent-orchestrator/issues/3), [#4](https://github.com/kou-keirei/agent-orchestrator/issues/4) |
| `session-resume` | Continue the same session, worktree and provider history | P0, P1 | [#21](https://github.com/kou-keirei/agent-orchestrator/issues/21), [#23](https://github.com/kou-keirei/agent-orchestrator/issues/23) |
| `lawnmower-reap-finalize` | Exact runtime ownership and safe terminal disposition | P0, P1 | [#20](https://github.com/kou-keirei/agent-orchestrator/issues/20), [#15](https://github.com/kou-keirei/agent-orchestrator/issues/15) |
| `durable-work-identity-execution-lease` | One persisted WorkItem, owner, attempt and fenced lease | P0 | [#5](https://github.com/kou-keirei/agent-orchestrator/issues/5), [#6](https://github.com/kou-keirei/agent-orchestrator/issues/6) |
| `worker-reports-event-outbox` | Authored report and immutable original-parent outbox | P0 | [#8](https://github.com/kou-keirei/agent-orchestrator/issues/8), [#7](https://github.com/kou-keirei/agent-orchestrator/issues/7) |
| `parent-reengagement` | Re-engage the original parent and prove semantic ACK | P0 | [#9](https://github.com/kou-keirei/agent-orchestrator/issues/9) |
| `dag-admission-backpressure` | Resource-bounded and dependency-aware work issuance | P0, P1 | [#10](https://github.com/kou-keirei/agent-orchestrator/issues/10), [#28](https://github.com/kou-keirei/agent-orchestrator/issues/28) |
| `overnight-chaos` | One uninterrupted native prompt-to-PR, then resilient overnight operation | P0, P1 | [#1](https://github.com/kou-keirei/agent-orchestrator/issues/1), [#11](https://github.com/kou-keirei/agent-orchestrator/issues/11) |
| `storage-sustainability` | Replay-safe retention, bounded storage and lower write amplification | P1 | [#16](https://github.com/kou-keirei/agent-orchestrator/issues/16), [#17](https://github.com/kou-keirei/agent-orchestrator/issues/17), [#32](https://github.com/kou-keirei/agent-orchestrator/issues/32) |

## P0: workstreams, missing gates and native witnesses

| WS / accountable issue | Owning C ID | G: falsifiable criterion | Required upstream fact | Qualified evidence surface | V |
| --- | --- | --- | --- | --- | --- |
| `WS-P0-BASE-BIRTH` · #3/#22 | `proof-admission-foundation` | `G-P0-EXACT-BASE`: Fresh spawn at an explicitly authorized SHA retains identical execution base and source ref through native SQLite/session readback; mismatches cause zero provider births. | Approved Git source/ref, native spawn and persistence | Native AO spawn/readback, source SHA, SQLite reopening, provider-start observation | `V-P0-BASE-SPAWN` |
| `WS-P0-BASE-BIRTH` · #3 | `spawn-recovery-lifecycle-continuity` | `G-P0-SPAWN-RECOVERY`: Interrupted native spawn followed by repeated startup durably recovers, preserves or refuses the original attempt; dirty/uncertain work remains intact and zero duplicate owners are born. | `G-P0-EXACT-BASE` as shared admission prerequisite, original attempt/owner identity | Native interruption/repeated-start receipts, persisted recovery decision, before/after dirty-state evidence and provider/session owner/birth counts | `V-P0-SPAWN-RECOVERY` |
| `WS-P0-CONTINUITY` · #20/#21 | `session-resume` | `G-P0-SAME-SESSION`: After positively observed provider exit, one authorized resume continues the original AO session/worktree/provider thread with one new completed turn and no replay. | Exact owner, generation and historical provider thread | Native provider/process exit and new turn receipts, readback | `V-P0-RESUME` |
| `WS-P0-WORK-LEASE` · #5/#6/#10 | `durable-work-identity-execution-lease` | `G-P0-OWNER-FENCE`: Persisted AO-issued WorkItem/attempt/owner identity and lease survive restart; expired/stale/foreign owners, wrong execution base/workspace and duplicate completion are rejected without authoritative writes or privileged effects. Privileged allowed effects fail closed when permission is absent or unproven; flags alone never prove effective permissions. | Native issuance and persisted original owner | Native issuance/SQLite reopen, identity/lease readback, each rejected-write/completion/effect negative and allowed-effect permission evidence; absent effective-permission telemetry remains UNPROVEN | `V-P0-WORK-REPORT-PARENT` |
| `WS-P0-REPORT-PARENT` · #7/#8 | `worker-reports-event-outbox` | `G-P0-AUTHORED-REPORT`: Authenticated native worker invocation produces exactly one immutable report/outbox fact under lost-response retry. | `G-P0-OWNER-FENCE`, authenticated worker caller | Native report invocation, receipt, outbox/store and retry readback | `V-P0-WORK-REPORT-PARENT` |
| `WS-P0-REPORT-PARENT` · #9 | `parent-reengagement` | `G-P0-ORIGINAL-PARENT`: Following a real positive parent exit, the report reaches only its original fenced parent and an authenticated parent-native semantic ACK resolves it once after restart. | `G-P0-AUTHORED-REPORT`, `G-P0-SAME-SESSION` | Native parent controller/turn identity, delivery counts, semantic event and durable ACK | `V-P0-WORK-REPORT-PARENT` |
| `WS-P0-RUNTIME-SAFETY` · #20 | `session-resume` | `G-P0-SINGLE-WRITER`: Concurrent and stale controller generations cannot both write to one Chat session across restore. | AO generation and input-admission identity | Native concurrent send/restore negative-case receipts | `V-P0-BOUNDED-WORKER` |
| `WS-P0-RUNTIME-SAFETY` · #28 | `dag-admission-backpressure` | `G-P0-COMMAND-BOUND`: A deliberately bounded recursive tool-command fixture enforces positive process-count and committed-memory ceilings under [#28 Acceptance limits](https://github.com/kou-keirei/agent-orchestrator/issues/28#acceptance-limits), without terminating unrelated processes. | Supported exact command-process ownership | Windows Job/process-tree native fixture, surviving-sibling observation | `V-P0-BOUNDED-WORKER` |
| `WS-P0-ADMISSION` · #10 | `dag-admission-backpressure` | `G-P0-CAPACITY-ONE`: A real capacity-one admission permits at most one real native worker birth per authorized issuance across crash, timeout, restart and stale ACK; positively reconcile an uncertain birth before any retry, with no unbounded retries. | `G-P0-OWNER-FENCE`, resource ceiling | Native admission/restart, provider and session birth counts plus reservation/session readback; uncertain-birth reconciliation before retry | `V-P0-BOUNDED-WORKER` |
| `WS-P0-INTEGRATION` · #22 | `proof-admission-foundation` | `G-P0-MATCHED-RELEASE`: Source-matched Electron/Go/SQLite package passes real installed native session/readback and paired recovery/rollback checks. | Exact integrated `main`, independent review, required CI and separate install authority | GitHub merged tree/CI, package hashes, native daemon/UI readback | `V-P0-MATCHED-RELEASE` |
| `WS-P0-AUTONOMOUS-PR` · #1/#11 | `overnight-chaos` | `G-P0-AUTONOMOUS-PR`: One approved prompt produces an actual reviewer finding, same-owner repair, fresh independent exact-source re-review of the repaired SHA/tree, passing required hosted CI and an authorized published PR with GitHub-verified URL/repository/head/base, all in one unattended run without another operator message or ECP dependency. | Work/report/parent loop; valid Git publication authority | One disposable end-to-end native trace with finding/owner/repair/re-review receipts, exact reviewed SHA/tree and GitHub PR/head/base/required hosted-CI readback | `V-P0-UNATTENDED-PR` |

### P0 native V: what must be seen

| V | Real entrypoint → execution → observable outcome |
| --- | --- |
| `V-P0-BASE-SPAWN` | Native AO CLI/API exact-source spawn → new worker admission → persisted exact base/ref on session GET, with zero-turn refusal on drift. |
| `V-P0-SPAWN-RECOVERY` | Native exact-base admitted spawn interrupted before completion → repeated daemon startup/recovery → durable recover/preserve/refuse decision, intact dirty/uncertain state and zero duplicate owners by provider/session count and readback. |
| `V-P0-RESUME` | Native `exit-agent`/positive process exit and `resume-agent` → restored original thread → new contextual provider response, no duplicate predecessor. |
| `V-P0-WORK-REPORT-PARENT` | Original parent issues WorkItem → real worker authors report → durable outbox → restored original parent receives and explicitly reconciles once. |
| `V-P0-BOUNDED-WORKER` | Native admitted worker runs bounded command → restores/competes under controlled pressure → no duplicate writer, escaped subtree or extra native worker birth, with process-count/committed-memory and provider/session count readback, including uncertain-birth reconciliation before retry. |
| `V-P0-MATCHED-RELEASE` | Installed, source-matched AO app starts → exercises current native workflow → observable readback and verified recoverable package state. |
| `V-P0-UNATTENDED-PR` | One user instruction → worker/tests → actual reviewer finding → same-owner repair → fresh independent exact-source re-review → passing required hosted CI and authorized published PR with GitHub URL/repository/head/base and reviewed SHA/tree readback, without operator continuation. |

## P1: workstreams, missing gates and native witnesses

P1 extends P0's original owner/contracts; it does not supersede P0 worker
custody or turn a prerequisite into a second scheduler, outbox or evaluator.

| WS / accountable issue | Owning C ID | G: falsifiable criterion | Required upstream fact | Qualified evidence surface | V |
| --- | --- | --- | --- | --- | --- |
| `WS-P1-COLD-WAKE` · #23 | `session-resume` | `G-P1-DEMAND-WAKE`: Restart, list, history and UI browsing leave idle workers cold; one addressed authorized Send wakes only its target once with original history. | P0 session writer/identity | Windows native daemon restart, process census, real addressed provider turn | `V-P1-DEMAND-WAKE` |
| `WS-P1-LIVENESS` · #4 | `spawn-recovery-lifecycle-continuity` | `G-P1-REPLACEMENT`: A failed/slow probe never replaces a live parent, while confirmed owner loss admits at most one properly fenced recovery. | Work/lease and generation ownership | Native positive-live, timeout/loss and restart recovery cases | `V-P1-LIVENESS-RECOVERY` |
| `WS-P1-FINALIZE` · #20 | `lawnmower-reap-finalize` | `G-P1-POSITIVE-REAP`: AO positively verifies exact-owned process-tree release, including descendants, without affecting sibling runtimes. | Terminal disposition and exact generation | Windows native process release and after-teardown census | `V-P1-SAFE-FINALIZE` |
| `WS-P1-FINALIZE` · #15 | `lawnmower-reap-finalize` | `G-P1-SAFE-FINALIZE`: Repeated terminal finalize reclaims only positively released, unpinned clean worktrees while preserving dirty/unique/reviewer/unknown work. | `G-P1-POSITIVE-REAP` and protected Git evidence | Native worker retirement, Git/worktree inventory, restart and nondeletion negatives | `V-P1-SAFE-FINALIZE` |
| `WS-P1-STORAGE` · #16 | `storage-sustainability` | `G-P1-REPLAY-SAFE-RETENTION`: Only proven reconstructible settled event segments qualify for compaction while protected provider history still replays faithfully after reopen. | Verified turn/checkpoint and protection provenance | Native SQLite/replay/branch tests and readback, bounded byte counts | `V-P1-SUSTAINABLE-STORAGE` |
| `WS-P1-STORAGE` · #17 | `storage-sustainability` | `G-P1-SAFE-STORAGE-PLAN`: A read-only storage inventory/dry-run distinguishes protected, unknown and candidate bytes with stable paths and no deletions. | Verified storage topology and backup lineage | Native storage-status/plan readback and unchanged before/after fingerprints | `V-P1-SUSTAINABLE-STORAGE` |
| `WS-P1-STORAGE` · #32 | `storage-sustainability` | `G-P1-MEASURED-WRITES`: Equivalent native archive/replay workload demonstrates less SQLite write amplification without changed event identity or missing frames. | Existing archive/retention contracts | Paired exact-source measurements and native replay-equivalence proof | `V-P1-SUSTAINABLE-STORAGE` |
| `WS-P1-EVALUATOR-PROFILE` · #13 | `proof-admission-foundation` | `G-P1-PROTECTED-FACTS`: An AO profile delivers source/runtime/reviewer facts to a qualified external ECP evaluator while missing effective permission remains explicitly unproven. | Qualified ECP evaluator/issuer and AO source identity | Real cross-system authenticated evaluator response and negative spoof tests | `V-P1-PROTECTED-EVAL` |
| `WS-P1-DEPENDENCIES` · #10 | `dag-admission-backpressure` | `G-P1-FAIR-DAG`: Restarted bounded multiworker dependency execution rejects cycles/stale completions and admits ready peers without starvation or overcapacity. | P0 capacity-one and durable identity | Native scheduler/DAG run, restart/resource receipts | `V-P1-DEPENDENCY-RUN` |
| `WS-P1-OVERNIGHT` · #11 | `overnight-chaos` | `G-P1-CHAOS-RECOVERY`: An extended real unattended run completes its accepted workflow across all ten mandatory chaos cases in [#11 Acceptance limits](https://github.com/kou-keirei/agent-orchestrator/issues/11#acceptance-limits) without fabricated completion, duplicated PRs or data loss. | P0 prompt-to-PR, cold wake, safe runtime and persistent events | Multi-hour native failure/restart/resource-pressure trace with GitHub outcome | `V-P1-OVERNIGHT-RESILIENCE` |

### P1 native V: what must be seen

| V | Real entrypoint → execution → observable outcome |
| --- | --- |
| `V-P1-DEMAND-WAKE` | Start with cold persisted workers → passive navigation then targeted Send → only addressed original worker becomes active and responds. |
| `V-P1-LIVENESS-RECOVERY` | Observe live and confirmed-dead parents → refusal/replacement as appropriate → one recoverable owner with intact history. |
| `V-P1-SAFE-FINALIZE` | Native completed worker → generation-fenced positive release → idempotent safe retirement with dirty work visibly preserved. |
| `V-P1-SUSTAINABLE-STORAGE` | Native archive grows under bounded workload → report/plan/replay/measure → no protected history loss and demonstrable storage economics. Actual pruning needs separate authority. |
| `V-P1-PROTECTED-EVAL` | AO emits genuine provenance → authenticated ECP evaluator consumes it → qualified decision or typed hold with no worker-authored PASS. |
| `V-P1-DEPENDENCY-RUN` | Native DAG admission → concurrent bounded dependent work through restart → correct dependency release and no starvation/duplicate owner. |
| `V-P1-OVERNIGHT-RESILIENCE` | One real unattended multi-hour workload → all ten #11 Acceptance limits disruptions and recovery → surviving native receipts, correct PR and bounded resource footprint. |

## Source map and supersession guard

- **Capability IDs, legacy weights and formal M milestones:**
  [Autonomy roadmap](../plans/ao-fork-autonomy-roadmap.md) and
  [machine tracker](../plans/ao-autonomy-readiness.json). Preserve their
  existing historical score and ID lineage; do **not** add these gates to
  its weighted percentage automatically.
- **Technical source/features/test selectors:** repository code, generated
  contracts, and the unmerged [AO-native PStack skill PR #87](https://github.com/kou-keirei/agent-orchestrator/pull/87)
  with its feature map. That PR's content is a draft, not accepted `main`.
- **Provenance and progress:** GitHub issues/PRs, original AO/ECP receipts,
  the owning local AO/ECP orchestration workspace's external/local
  `.agents/state/workspace-obligations.json` and native evaluator records.
  That ledger is not tracked at the accepted AO main SHA and is not an
  available artifact in this repository; no portable ledger location is
  established here. None of those surfaces are overwritten by this document.
- **Known source-only but nonnative work:** P0 [PRs #86–#92](https://github.com/kou-keirei/agent-orchestrator/pulls)
  contain relevant review/CI candidates. Successful CI/fixture tests are
  not native `V` proof. P0 W1520's exact-base/source-ref persistence
  blocker remains an active implementation owner, not a new registry task.
- **Existing F1–F5 scorecard:** preserves its original frozen denominator;
  its 20% is **not comparable** to any future `G` score here.

## Adoption and report contract

1. Review the **definition diff**, issue ownership and duplicate C/G mapping;
   identify any approved criteria omitted or any proposed requirement that
   materially changes user intent. Do not change current issue states.
2. Independently review this exact Markdown against the current accepted
   `main` and linked owner contracts. A Git commit or PR alone does not
   approve new gates or mean the registry is integrated.
3. This accepted `v1.0.0` freezes the P0/P1 criteria and denominator. The
   initial snapshot records 0/11 P0 and 0/10 P1 fully proven under its bounded
   evidence; later PASS requires **qualified native evidence**, not inference.
4. `FULLWAVE` then reconciles the existing receipts; `FULLWAVE REPORT`
   reads them. `FULLWAVE REGISTRY` evolves definitions under version control.
   No extra dispatch Gate, issue migration, historical backfill or new
   mutable score store is created.

**Effect boundary:** This adopted registry records criteria and a historical
benchmark only. It cannot dispatch workers, replay provider turns, grant Git
or evaluator authority, mutate the obligation ledger, install a binary or
delete data. PR #93 integrated the original definitions, not native PASS.
