# Read-only storage status

`GET /api/v1/storage/status` and `GET /api/v1/storage/cleanup-plan` return
the same versioned `ao.storage.v1` retention report through the existing daemon
API. There is no apply, prune, or reap operation.

Both accept `limit` (1–256, default 100) and `after` (the preceding response's
`nextAfter`). An empty `nextAfter` ends traversal. Pagination is a live keyset,
not a frozen inventory: concurrent inserts before the cursor may be missed,
deletions may disappear, and revisions may change between pages. Refresh from
the beginning for a new view. Page results must never authorize cleanup.

Rows identify the configured DB/WAL/SHM and durable AO workspace records.
Registered per-repository worktrees take precedence over a session's workspace
container; sessions without registered repositories use their recorded workspace
path. Session and repository names are hex encoded into stable row keys. These
are logical record identities, not physical extent identities. Recorded paths
are not probed and may be absent, aliases, junctions, symlinks, denied or changing.

`cleanupGeneration` is the owning session's durable generation.
`sourceRevision` versions that session row only, not its Git state or the
registered worktree row. Null provenance fields and byte counts mean UNKNOWN;
`ownerSession` and `workspaceTier` use explicit `UNKNOWN` when unavailable.
The current schema does not establish SESSION/SCRATCH/STABLE tier provenance.
Dirty state, unique commits, and cleanup-row revision are unproven and protected.

Every response reports `SOURCE_GAP`, retains every row, and has
`executableCandidates: 0`. Logical file length and physical allocation are
separate nullable fields; neither is measured by this live read interface.
The existing `CollectMetadata` adapter still requires externally maintained
quiescence and returns `SOURCE_GAP` for unproven physical allocation. This API
does not fabricate its attestations.

The current AO store has no qualified backup ownership/consistent-snapshot/
restorability catalog or rebuildable-cache ownership catalog. Their absence is
reported in `sourceGaps`; arbitrary backup/cache directories are not enumerated
or inferred from names or age. The report is not a complete physical census.
It never copies SQLite files, runs Git, creates snapshots, or invokes lifecycle
cleanup. A qualified measurement/catalog adapter requires separate scope.

The opt-in service-only `ProbeSize` seam is not bound to these HTTP routes.
On Windows it returns `ErrSourceGap` with diagnostic `platform_source_gap`
before filesystem access, even for an explicitly trusted root. The current
request supplies a numeric `OwnerUID` without a trusted mapping to a Windows
owner SID/ACL. Reading the target's owner, using the current process token, or
equating a SID's final RID with a UID cannot supply that missing authority.
Logical bytes and device identity remain null, entries remain zero, physical
bytes remain null, retention stays true, and executable candidates stay zero.
Invalid requests and cancellation still use their existing diagnostics.

Windows native support remains blocked on that ownership contract and qualified
handle-relative no-reparse traversal, including ancestor and parent/name binding,
stable volume/file identity, hardlink and namespace-alias rejection, bounded
enumeration, and mutation/budget checks. Windows temp-fixture canaries establish
the fail-closed contract only; they do not qualify traversal or measurement.
Linux keeps its existing native source; Darwin remains unsupported.
