# Exact original-parent worker reports

`ExactParentReports` is an opt-in, transport-neutral consumer of an existing
immutable worker-report receipt and its existing singleton outbox event. It is
not wired into the daemon, report ingress, generic dispatcher or provider driver.
The generic transport ACK paths continue to refuse reports and mixed batches.

The reviewed source scaffold is W571
`8a441437277ec31b094b680b12791614c547c241` (W574 source review). Its exact-message
token, stable client ID, semantic/native ports, dead-letter reconciliation and
notification retries are retained. This consumer adds a bounded Resume hook,
fresh work/lease and outbox admission checks, authenticated semantic replay, and
atomic retention of notification retry evidence on semantic resolution.

## Required integration contracts

The composed SQLite `ReadReportOriginalParent` adapter projects the existing
validated immutable parent readback. It grants no live delivery or ACK authority.
Chat's `WithLiveReportParent` supplies an opt-in Codex delivery adapter under the
existing input, controller and dispatch gates. It independently compares the
live handle/generation and active branch/scope with the immutable target, reads
native history against AO's existing high-water contract, then uses Chat's
durable client-message reservation. Busy parents are held before claim; reports
are never queued for a later unfenced drain. The adapter and consumer remain
unmounted. Missing capabilities produce durable HOLD; native activation is
`BLOCKS_PRODUCTION`. There is no coordinator discovery or metadata backfill.

- PR61 corrected source `6d94d85b9e2d4e868019966e1970492b23df2e9c`
  and independently reviewed W593 composition
  `5fec99bd615d52f671c89346164eb3282106f53d` supply the immutable parent/lease/ACK
  contracts, including the retirement-only controller. The reader adapts
  immutable `GetWorkItemOriginalParent(work, attempt)` readback only after checking
  the complete `WorkItemParentTarget.ValidateFor` contract, including issuer,
  provenance and provider scope. `OriginalNativeSessionID` on a worker report is
  worker provenance and cannot supply this parent capability. Destination remains
  the issuance-time parent, even after owner transfer; a stale owner or work lease
  holds new delivery rather than retargeting it.
- PR60 strict Resume is preserved in accepted base
  `f913c96d3ed8127c77247fb38c09070168222570`. A native adapter must still
  verify genuine provider history and preserve the same native conversation and
  scope under AO's existing native ownership gate. A positive exit observation
  before callback admission permits at most one Resume attempt within the shared
  five-second attempt deadline. Busy/unknown/probe errors do not permit Resume.
  Replaced, terminated, foreign, missing or mismatched history holds. If Resume
  changes controller generation and accepted owner contracts do not prove exact
  continuity with issuance, HOLD; this consumer invents no generation translation.
  Chat's `WithResumedReportParent` returns
  `report_resume_generation_continuity_unavailable` before provider access.
  Missing/stopped controllers never imply a positive native exit or authorize
  ordinary restoration, generation reuse, or a fresh thread fallback.
- The native semantic reader must authenticate an explicit parent reconciliation
  event, after the parent durably reconciles the exact immutable receipt. It must
  bind the native event reference, stable client ID and complete exact message.
  Provider transport ACK, idle, turn completion, echo and HTTP 200 never qualify.
  The Chat adapter's `ReadReportReconciliation` returns
  `report_parent_semantic_producer_unbound`: accepted Codex has no trusted
  semantic producer for this interface. W584 pre-thread tool registration
  remains `HUMAN_DECISION_REQUIRED`; no worker tool or asserted ACK is substituted.

Integration trigger: independent review and executable qualification of the
delivery adapter, plus owner adjudication of trusted parent-native semantics and
Resume generation continuity. Source delivery does not clear those two holds.
No driver changes, runtime installation or production ECP263 wake belong here.

## Delivery and retry behavior

Prepare commits the exact message/payload into the existing `lease_token` before
Send. Every retry reuses those bytes and the same client ID through Chat's durable
idempotency boundary. Stale work generation, changed/released/expired work lease,
non-due event or stale outbox snapshot cannot prepare a send. Admission requires
this invocation's committed, unexpired outbox lease; reclaim invalidates a late
transport result. These checks are not reusable native permission grants.

Report Chat admission appends the message and reserves its native send in one
SQLite transaction, using Chat's existing reservation owner. It never commits a
pending ordinary queue entry. A reservation failure rolls back the append; a
crash, cancellation or read failure after commit retains unresolved reservation
state that blocks ordinary drain and replacement generations. Only the invocation
that commits a new reservation may dispatch; duplicate readback cannot recreate
that permission. Ordinary human messages retain their existing pending queue.

Submitted reports await semantics without another Send or Resume. Lost transport
results may be retried with the same durable key; lost semantic API responses are
authenticated again and produce no second state transition or wake. An exact
authenticated semantic event may resolve an already-prepared pending, leased,
submitted or dead-letter row, including after lease expiry or ownership change;
that historical reconciliation grants no fresh send authority. Mixed or legacy
tokens remain held. Existing reclaim, bounded attempts, attention and retention
policies continue to own retry scheduling; no new scheduler or outbox is added.

Semantic ACK retains the attention marker atomically. Notification resolution is
retried across restart, including when no report is held and no transport is due;
only successful notifier resolution followed by fresh semantic readback clears
the marker. An unresolved peer report prevents resolution for that worker.

Tests use disposable SQLite stores and public Go delivery APIs with explicit
native fixtures. They establish no real provider identity, Resume, hosted CI,
production admission or independent-review qualification.
