---
name: verify-ao-native-pstack
description: Verify AO native Codex provider exit/resume and original-parent WorkItem report delivery with separate semantic ACK evidence. Use for AO PStack verification, not ECP PowerShell or named-pipe qualification or a general audit.
---

# Verify AO native PStack

Use `doctor`, `run`, `evidence`, then `cleanup` below. Read the
[feature map](references/feature-map.md) before selecting tests or claiming a
native capability. AO owns lifecycle, durable identity, leases and delivery;
consume its existing contracts. Do not create a second scheduler, caller issuer,
trust primitive, evaluator, or Gate. The ECP PowerShell/named-pipe verification
skill proves a different system and cannot supply AO evidence.

## Evidence tiers (never collapse)

| Tier | Required evidence for PASS | Insufficient evidence |
| --- | --- | --- |
| 1: deterministic source/test | Exact source SHA/tree, selected tests and subtests, exit codes, no unexpected skips; assertions cover the mapped boundaries | Compilation alone, test-name matches, prior checkout results |
| 2: isolated real provider | Installed Codex executable hash; original AO session and native thread/turn; completed predecessor exchange; positively observed provider exit; different process reconnects to the same thread and byte-equivalent history; exactly one predecessor `turn/start`, zero replay turns | Mock child, fixture-created history, persistent-host adoption, idle, failed probe, auth refusal, skipped model turn |
| 3: AO immutable report to original parent | AO-issued WorkItem/attempt/owner/lease and immutable parent target; native worker-authored report invocation; receipt/event/source digest; exact original parent fenced through delivery; one logical send and physical-send counts across restart | CLI JSON claims, current coordinator lookup, transport admission as ACK, direct DB inserts, source fixtures |
| 4: authenticated parent-native semantic ACK | Trusted native reader authenticates explicit reconciliation event from the original parent, binds exact report/event/client-message/work/attempt/lease/native controller/scope, and performs one durable semantic transition; authenticated replay causes no send or second transition | Text saying ACK, tool echo, HTTP 200, idle, turn completion, generic orchestration acknowledgement, authored event ID |

Tier 2 does not prove tiers 3–4. Tier 1 negative tests do not prove native
negative cases. Report each tier as `PASS`, `FAIL`, or `HOLD` with reason and
artifact references. Missing capability, authentication refusal, unavailable
tooling, skipped model turn, missing identifiers or incomplete observation is
`HOLD`; observed contract violation is `FAIL`. A Go exit code 0 with a skip is
not native PASS. Overall PASS requires all four tiers PASS for the same run.

## Doctor: local preflight

Confirm the requested source/base, mutation scope and native-run authorization.
Do not fetch, push, create a PR, install packages, change runtime/auth config, or
use user AO DBs or historical native sessions. Keep this skill's artifacts out
of the repository. These PowerShell examples run from the AO worktree root:

```powershell
git status --short
git worktree list
git rev-parse HEAD
git rev-parse 'HEAD^{tree}'
# When the task supplies a required base, check it before running:
# git merge-base --is-ancestor <required-base> HEAD
if (-not $env:AO_CLI) { throw 'HOLD: AO_CLI is not set' }
Get-FileHash -Algorithm SHA256 -LiteralPath $env:AO_CLI
Get-Command go -CommandType Application -ErrorAction Stop
$pstackCodex = Get-Command codex -CommandType Application -ErrorAction SilentlyContinue
if ($pstackCodex) { Get-FileHash -Algorithm SHA256 -LiteralPath $pstackCodex.Source }
```

Record canonical paths and SHA256 for installed AO and Codex, including the
actual executable behind any wrapper; wrapper-only identity is HOLD. Record Go
version, OS/architecture, source dirty status and source-to-binary provenance.
An installed AO hash does not imply it was built from this checkout. The Go
tests below run source-built test binaries, not the installed daemon: report
installed-daemon coverage as HOLD unless separately observed. Recheck the
original checkout status before and after; stop on unexpected changes.

Do not run broad `ao doctor`: it includes unrelated provider/SCM checks. This
doctor is the bounded preflight above, not daemon health or native acceptance.
Inspect the feature map's holds at the tested SHA. Missing production ingress,
generation continuity or semantic producer means tiers 3–4 remain HOLD. Do not
mount a route or mint credentials to finish verification.

## Run: disposable source and optional native checks

Use a fresh child PowerShell process for these environment overrides so the
invoking AO session retains its settings. Create one disposable run root under
`~/.ao/scratch`, never reuse an existing directory. Preserve its absolute path
in the evidence. No daemon start, session spawn, project import or user DB read
is needed for the currently supported test route.

```powershell
$pstackRoot = Join-Path ([Environment]::GetFolderPath('UserProfile')) ('.ao/scratch/native-pstack-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $pstackRoot -ErrorAction Stop | Out-Null
$env:AO_DATA_DIR = Join-Path $pstackRoot 'data'
$env:AO_RUN_FILE = Join-Path $env:AO_DATA_DIR 'running.json'
$env:CODEX_HOME = Join-Path $pstackRoot 'codex'
$env:TMP = $pstackRoot
$env:TEMP = $pstackRoot
New-Item -ItemType Directory -Path $env:AO_DATA_DIR,$env:CODEX_HOME | Out-Null
Set-Content -LiteralPath (Join-Path $env:CODEX_HOME 'config.toml') -Value 'cli_auth_credentials_store = "file"'
$env:AO_CODEX_NATIVE_EXIT_RESUME = '0'
$env:AO_CODEX_LIVE = '0'
Push-Location backend
try {
  go test -json -count=1 -timeout=120s ./internal/adapters/chatdriver/codexappserver ./internal/service/chat ./internal/orchestrationevents ./internal/storage/sqlite/store ./internal/adapters/reportactor ./internal/daemon ./internal/httpd ./internal/cli -run 'Test(NativeExitResume(RequestGuard|PersistedExchange|BoundedCleanup|RefusalBoundaries)|Acceptance|Unavailable|ExactReport|Report[A-Z]|ComposedParent|OriginalParentIssuer|NativeReportActor|WorkerReport)' > (Join-Path $pstackRoot 'source.jsonl')
  $pstackSourceExit = $LASTEXITCODE
  Set-Content -LiteralPath (Join-Path $pstackRoot 'source.exit.txt') -Value $pstackSourceExit
} finally { Pop-Location }
```

Inspect every test result and skip, including matched-test count per package;
empty selection is HOLD, not PASS. Keep compiler stderr and failures. The
timeout is per package; cap the whole source command at ten minutes, stop on
deadline and preserve partial evidence. Do not retry a failed native model turn.
At most two repair cycles for in-scope command/path mistakes; missing source
behavior or credentials requires a separate authorized lane.

Only with explicit authorization for the isolated provider attempt, run once in
that child shell, after the source checks. The existing test creates its own
fresh workspace/profile under TEMP, pins file-only auth, supplies an allowlisted
child environment with blocked proxies, and owns bounded child cleanup. Never
loosen it, copy user auth, enable network, or substitute `AO_CODEX_LIVE`.

```powershell
$env:AO_CODEX_NATIVE_EXIT_RESUME = '1'
Push-Location backend
try {
  go test -json -count=1 -timeout=90s ./internal/adapters/chatdriver/codexappserver -run '^TestNativeExitResumeDisposable$' > (Join-Path $pstackRoot 'native.jsonl')
  $pstackNativeExit = $LASTEXITCODE
  Set-Content -LiteralPath (Join-Path $pstackRoot 'native.exit.txt') -Value $pstackNativeExit
} finally { Pop-Location }
```

Cap this whole command at three minutes. `NATIVE_E2E_UNAVAILABLE` or `skip`
means HOLD even if the command succeeds. `NATIVE_E2E_PASS` plus test success
proves the test's assertions only: it does not export all IDs/history/process
exit codes required by the evidence record. Mark those missing observations
HOLD rather than instrumenting production or fabricating values.

For tiers 3–4, first require an already accepted AO-native ingress and semantic
reader in the exact source/binary under test. At the mapped baseline they are
unavailable: record the typed holds and stop these dependent stages. There is
no supported CLI command for manufacturing native reconciliation. If a later
accepted implementation supplies a disposable integration entrypoint, read its
contract before use, lock its command/deadline/send budget in the run record,
and use only new AO-owned parent/worker sessions under this run root. Retain
the tier requirements above, including negative foreign-owner cases. Do not
silently turn this skill task into integration implementation.

## Evidence: one bounded run record

Write a local run record alongside logs (no secrets, credentials or caller
handles). Record actual values or `unobserved` with HOLD; never infer IDs from
names. Include:

- UTC start/end, run root, exact source SHA/tree/base and dirty diff identity;
  installed AO/Codex paths/hashes, tested binary provenance, commands/deadlines,
  exit codes, tests/subtests passed/failed/skipped and denominator.
- AO parent/worker session IDs; WorkItem ID, attempt ID, original worker and
  parent IDs, owner session/generation, runtime/controller generations, lease
  ID/revision/state/expiry; provider conversation/thread and scope IDs. Distinguish
  the worker's `OriginalNativeSessionID` from the parent's native target.
- Original native turn ID and completed exchange; report ID/content digest,
  immutable receipt, orchestration event ID/source revision, persisted delivery
  binding and client-message ID; authenticated semantic native event/turn ID.
- Before/after process identities and observed exit codes (or signal), positive
  exit evidence, same-thread/history comparison, predecessor send count and
  reconnect replay count. A teardown callback returning nil is not an OS exit code.
- Restart timeline: provider exit/reconnect separately from daemon/store reopen;
  report pending/prepared/leased/submitted/acknowledged or held/dead-letter state,
  generation/lease changes, attempts and logical/physical sends before/after each
  restart; lost transport result, semantic replay and no-resend observations.
- Negative cases and observed no-send/no-transition result: foreign workspace,
  wrong thread, stale generation/owner, foreign WorkItem/attempt/parent, expired
  or stale lease, replacement coordinator, foreign semantic event and ACK echo.
  Label fixture versus real-native evidence explicitly.
- Each tier's PASS/FAIL/HOLD, exact typed refusal, artifact path/hash and remaining
  gaps; cleanup outcome. No local/source result qualifies hosted CI or production.

Keep the summary at most 1 MiB, definitions at most 256 KiB, generated logs at
most 8 MiB, and instructions/lanes within 128/64. Stop with
`RESOURCE_ENVELOPE_EXHAUSTED` if exceeded; retain bounded evidence and describe
any omitted log bytes. Do not claim an unbound evaluator or supplied rubric
hash was independently verified.

## Cleanup: only this disposable run

Let the test's cleanup finish and inspect its errors. No global `ao stop`,
kill-by-name, user session termination or worktree removal. If an owned child
remains, inspect its exact PID/path/run association before graceful termination;
uncertain identity means HOLD and preserve the directory. Keep evidence outside
any data directory selected for removal. Close the child shell to discard its
environment overrides.

Default cleanup retains the run root for review and reports its path. Delete
only if this exact disposable root is authorized for removal and all owned
process exits were observed: resolve its full path, verify it is a direct
`native-pstack-*` child of the intended `~/.ao/scratch` directory with no reparse
points, then use PowerShell `Remove-Item -LiteralPath <verified-root> -Recurse`.
Never pass discovered paths to another shell. Recheck worktree/original checkout
status and report PASS/FAIL/HOLD for cleanup separately from verification.
