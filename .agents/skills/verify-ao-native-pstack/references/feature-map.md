# AO native PStack feature map

Baseline: `1d50e775ec0abbaa952ea8bc518552d7ee00954c`. Paths below are
repository-relative. Recheck at the requested SHA; this map is navigation and
evidence classification, not a new acceptance Gate or proof of installed code.

| Feature / tier | Source and focused tests | Boundary / current limit |
| --- | --- | --- |
| Provider exit/resume, 1–2 | `backend/internal/adapters/chatdriver/codexappserver/native_exit_resume_test.go`: `TestNativeExitResumeDisposable`, `TestNativeExitResumeRequestGuard`, `TestNativeExitResumePersistedExchange`, `TestNativeExitResumeBoundedCleanup`, `TestNativeExitResumeRefusalBoundaries` | Real test: account-only probe, predecessor exit, fresh process, same thread/history, one turn. Auth/zero turns skip = HOLD. Other named tests are fixtures. No AO report/semantic proof. |
| Immutable WorkItem/attempt/lease, 1 → 3 prerequisite | `backend/internal/domain/work_item_identity.go`, `work_item_lease.go`, `work_item_parent.go`; `backend/internal/storage/sqlite/store/work_item_store.go`, `work_item_lease_store.go`, `work_item_parent_store.go` and adjacent tests | AO-issued identity and transactional readback own provenance; caller-authored values or legacy missing target cannot issue authority. |
| Issuance-time original parent, 1 → 3 | `backend/internal/service/chat/work_item_parent.go`, adjacent `TestOriginalParentIssuer*`; `backend/internal/session_manager/work_item_spawn.go` | Captures original live controller and native scope. Replacement coordinator cannot become original parent. |
| Worker caller and report ingress, 1; 3 HOLD | `backend/internal/service/chat/worker_report_actor.go`, `TestNativeReportActor*`; `backend/internal/adapters/reportactor/acceptance.go`, `unavailable.go`; `backend/internal/httpd/worker_report.go`; `backend/internal/cli/worker_report.go` | Internal acceptance exists; public handler is not mounted. `WorkerReportNativeIngressBlock` / `HOLD_WORKER_REPORT_ACTOR_BINDING_UNAVAILABLE`: no accepted native invocation carrying daemon-issued caller handle. CLI report JSON is not authentication. |
| Immutable receipt and exact outbox, 1 → 3 | `backend/internal/storage/sqlite/store/worker_report_store.go`, `worker_report_delivery_store.go`, `worker_report_parent_reader.go`; `worker_report_composition_test.go` (`TestComposedParentReport*`, `TestReportRuntime*`) | Fixtures cover restart, lost transport result, stale lease/generation/wrong parent and no resend. DB reopen is not a real daemon/provider restart. |
| Exact delivery and continuity, 1; native 3 HOLD | `backend/internal/orchestrationevents/report_exact_parent.go`, `report_exact_parent_test.go`, `report_resume_test.go`; `backend/internal/service/chat/report_parent.go`, `report_parent_test.go` | Delivery prepares durable binding before send; admission awaits semantics. `WithResumedReportParent` returns `report_resume_generation_continuity_unavailable`; ordinary Resume cannot prove issuance-generation continuity. |
| Parent-native semantic ACK, 1; 4 HOLD | `backend/internal/ports/worker_report_delivery.go`: `ExactReportReconciliation`; `backend/internal/service/chat/report_parent.go`: `ReadReportReconciliation`; `TestExactReportSemanticEventRejectsEchoAndForeignBindings`, `TestReportParentAdapterDeliveryIsNotSemanticACK` | Production reader returns `report_parent_semantic_producer_unbound`. Fixture semantic event ≠ authenticated native producer. Explicit reconciliation must bind the entire immutable message, client ID and native event. |
| Legacy transport exclusion, 1 | `backend/internal/orchestrationevents/report_delivery_test.go`; `backend/internal/storage/sqlite/store/worker_report_delivery_store_test.go` | Generic ACK, mixed batch, current-orchestrator delivery and attention resolution cannot silently resolve an exact-parent report. |

Use the source test selection in SKILL.md for doctor/run; inspect selected test
counts and failures rather than interpreting this table as evidence. For native
tiers without an accepted disposable entrypoint, preserve HOLD and request the
separate product/implementation decision. Never reuse ECP named-pipe artifacts,
historical transcripts or the user's logged-in Codex profile to fill these gaps.
