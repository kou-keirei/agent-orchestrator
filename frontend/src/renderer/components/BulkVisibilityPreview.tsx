import { useRef, useState } from "react";
import { isOrchestratorSession, type WorkspaceSession } from "../types/workspace";

type Action = "hide" | "unhide";
type Result = { id: string; error?: string };
/** Offline presentation seam, not a daemon DTO. No production adapter is bound. */
export type VisibilityPreviewAdapter = (action: Action, ids: readonly string[]) => Promise<readonly Result[]>;
export const VISIBILITY_CHUNK_SIZE = 200;

export function visibilityPreview(sessions: readonly WorkspaceSession[], selected: ReadonlySet<string>, action: Action, activeId?: string) {
	const byId = new Map(sessions.map((session) => [session.id, session]));
	const eligible: string[] = [];
	for (const id of selected) {
		const session = byId.get(id);
		if (!session || id === activeId || session.kind !== "worker" || isOrchestratorSession(session) || session.mode !== "chat" ||
			session.cloud || session.isTerminated !== true || !["terminated", "exited", "merged"].includes(session.status) ||
			(session.activity && session.activity.state !== "exited") ||
			Boolean(session.hiddenAt) !== (action === "unhide")) continue;
		// Runtime termination cannot discharge delivery or controller obligations.
		// Optional summaries may be absent on older data; any supplied summary
		// must affirm completion. PR facts must be present and resolved, including
		// when a stale summary claims the worker is already archived or merged.
		if (session.activeAgentSwitch !== undefined ||
			(session.scmStatus !== undefined && session.scmStatus !== "merged") ||
			(session.kanbanColumn !== undefined && session.kanbanColumn !== "archive") ||
			!Array.isArray(session.prs) || session.prs.some((pr) =>
				!pr || !["merged", "closed"].includes(pr.state) || pr.ci !== "passing" ||
				!["approved", "none"].includes(pr.review) || pr.mergeability !== "mergeable" ||
				pr.reviewComments !== false)) continue;
		eligible.push(id);
	}
	return {
		eligible,
		skipped: selected.size - eligible.length,
		chunks: Array.from({ length: Math.ceil(eligible.length / VISIBILITY_CHUNK_SIZE) }, (_, index) =>
			eligible.slice(index * VISIBILITY_CHUNK_SIZE, (index + 1) * VISIBILITY_CHUNK_SIZE)),
	};
}

export function BulkVisibilityPreview({ sessions, activeId, showHidden, adapter }: {
	sessions: readonly WorkspaceSession[];
	activeId?: string;
	showHidden: boolean;
	adapter?: VisibilityPreviewAdapter;
}) {
	const [selected, setSelected] = useState<ReadonlySet<string>>(new Set());
	const [action, setAction] = useState<Action>("hide");
	const [results, setResults] = useState<readonly Result[]>([]);
	const [pending, setPending] = useState(false);
	const running = useRef(false);
	const preview = visibilityPreview(sessions, selected, action, activeId);
	const candidates = sessions.filter((session) => showHidden || !session.hiddenAt);
	async function apply() {
		if (!adapter || running.current || preview.eligible.length === 0) return;
		running.current = true;
		setPending(true);
		const next: Result[] = [];
		try {
			for (const ids of preview.chunks) {
				try {
					const response = await adapter(action, ids);
					for (const id of ids) {
						const matches = response.filter((result) => result.id === id);
						next.push(matches.length === 1 ? matches[0] : { id, error: "Missing or ambiguous result" });
					}
				} catch (error) {
					next.push(...ids.map((id) => ({ id, error: error instanceof Error ? error.message : "Preview failed" })));
				}
			}
			setResults(next);
			setSelected(new Set(next.filter((result) => result.error).map((result) => result.id)));
		} finally {
			running.current = false;
			setPending(false);
		}
	}
	return <details className="px-2 text-sm">
		<summary>Bulk Hide preview</summary>
		<p>Presentation only. Active, orchestrator and ambiguous sessions are preserved.</p>
		{!adapter && <p>Batch integration unavailable pending backend qualification.</p>}
		<fieldset disabled={pending}>
			<legend>Worker chat selection</legend>
			<button type="button" onClick={() => { setAction("hide"); setResults([]); }}>Hide Selected</button>
			{showHidden && <>
				<button type="button" onClick={() => { setAction("unhide"); setResults([]); }}>Unhide Selected</button>
				<button type="button" onClick={() => {
					setAction("unhide"); setResults([]);
					setSelected(new Set(candidates.filter((session) => session.hiddenAt).map((session) => session.id)));
				}}>Unhide All (preview)</button>
			</>}
			<button type="button" onClick={() => { setSelected(new Set(candidates.map((session) => session.id))); setResults([]); }}>Select all shown</button>
			<button type="button" onClick={() => { setSelected(new Set()); setResults([]); }}>Clear selection</button>
			<div className="max-h-64 overflow-auto">
				{candidates.map((session) => <label key={session.id} className="flex gap-2">
					<input type="checkbox" checked={selected.has(session.id)} onChange={() => {
						setResults([]);
						setSelected((previous) => {
							const next = new Set(previous);
							if (next.has(session.id)) next.delete(session.id); else next.add(session.id);
							return next;
						});
					}} />
					{session.title} ({session.id}){session.hiddenAt ? " — Hidden" : ""}
				</label>)}
			</div>
		</fieldset>
		<p role="status">{action}: {selected.size} selected, {preview.eligible.length} eligible, {preview.skipped} skipped; {preview.chunks.length} chunks (maximum 200 each).</p>
		<button type="button" disabled={!adapter || pending || preview.eligible.length === 0} onClick={() => void apply()}>Run mock preview</button>
		<ul aria-label="Per-session preview results">{results.map((result) => <li key={result.id}>
			{result.id}: {result.error ? `Failed: ${result.error}` : "Succeeded"}
		</li>)}</ul>
	</details>;
}
