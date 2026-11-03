import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { WorkspaceSession } from "../types/workspace";
import { BulkVisibilityPreview, visibilityPreview } from "./BulkVisibilityPreview";

const card = (id: string, overrides: Partial<WorkspaceSession> = {}): WorkspaceSession => ({
	id, title: id, workspaceId: "p", workspaceName: "Project", provider: "claude-code",
	kind: "worker", mode: "chat", isTerminated: true, status: "terminated", prs: [], updatedAt: "2026-10-21", ...overrides,
});

describe("bounded offline visibility preview", () => {
	it("counts 371 mixed cards exactly and preserves unsafe or ambiguous items", () => {
		const cards = Array.from({ length: 371 }, (_, index) => card(String(index),
			index < 241 ? {} : index < 271 ? { isTerminated: false, status: "working" } :
			index < 301 ? { kind: "orchestrator" } : index < 331 ? { mode: undefined } : { hiddenAt: "yesterday" }));
		const selected = new Set(cards.map(({ id }) => id));
		const preview = visibilityPreview(cards, selected, "hide", "0");
		expect(preview.eligible).toHaveLength(240);
		expect(preview.skipped).toBe(131);
		expect(preview.chunks.map((chunk) => chunk.length)).toEqual([200, 40]);
		expect(new Set(preview.chunks.flat()).size).toBe(240);
		expect(visibilityPreview(cards, selected, "unhide", "0").eligible).toHaveLength(40);
	});

	it.each([
		{ kind: undefined }, { mode: "tui" }, { isTerminated: undefined }, { status: "unknown" },
		{ status: "needs_input" }, { activity: { state: "active", lastActivityAt: "today" } },
		{ cloud: { orgId: "o" } },
	] satisfies Partial<WorkspaceSession>[])("skips ambiguous or unsupported facts: %j", (overrides) => {
		expect(visibilityPreview([card("s", overrides)], new Set(["s", "missing"]), "hide")).toMatchObject({ eligible: [], skipped: 2, chunks: [] });
	});

	// Deliberately retain conflicting SCM facts after runtime termination: a
	// terminated process does not resolve its delivery obligations.
	const resolvedPR: WorkspaceSession["prs"][number] = {
		url: "https://example.test/pull/1", number: 1, state: "merged",
		ci: "passing", review: "approved", mergeability: "mergeable",
		reviewComments: false, updatedAt: "2026-10-21",
	};
	it.each(["hide", "unhide"] as const)("excludes conflicting parent identity from %s", (action) => {
		const session = card("project-orchestrator", {
			kind: "worker", hiddenAt: action === "unhide" ? "yesterday" : undefined,
		});
		expect(visibilityPreview([session], new Set([session.id]), action)).toMatchObject({
			eligible: [], skipped: 1, chunks: [],
		});
	});

	it.each(["hide", "unhide"] as const)("admits a settled terminated worker with daemon merged status for %s", (action) => {
		// DeriveStatus returns merged when a terminated worker has a settled PR
		// set containing a merge, independently of its exited runtime activity.
		const session = card("s", {
			status: "merged", scmStatus: "merged", kanbanColumn: "archive", prs: [resolvedPR],
			activity: { state: "exited", lastActivityAt: "2026-10-21" },
			hiddenAt: action === "unhide" ? "yesterday" : undefined,
		});
		expect(visibilityPreview([session], new Set([session.id]), action)).toEqual({
			eligible: ["s"], skipped: 0, chunks: [["s"]],
		});
	});
	it.each([
		...["pr_open", "draft", "ci_failed", "review_pending", "changes_requested", "approved", "mergeable", "unknown"].map((scmStatus) => ({ scmStatus })),
		...["building", "validating", "needs_review", "ready", "unknown"].map((kanbanColumn) => ({ kanbanColumn })),
		{ scmStatus: "", kanbanColumn: "archive" },
		{ scmStatus: null }, { kanbanColumn: null }, { prs: undefined }, { prs: null },
		{ prs: "unknown" }, { prs: [null] }, { activeAgentSwitch: null },
		{ activeAgentSwitch: { id: "switch", state: "requested" } },
		{ activeAgentSwitch: { id: "switch", state: "unknown" } },
		{ activeAgentSwitch: { id: "switch", state: "completed" } },
		...["open", "draft", "unknown", undefined].map((state) => ({ prs: [{ ...resolvedPR, state }] })),
		...["failing", "pending", "unknown", undefined].map((ci) => ({ prs: [{ ...resolvedPR, ci }] })),
		...["changes_requested", "review_required", "unknown", undefined].map((review) => ({ prs: [{ ...resolvedPR, review }] })),
		...["blocked", "conflicting", "unstable", "unknown", undefined].map((mergeability) => ({ prs: [{ ...resolvedPR, mergeability }] })),
		{ prs: [{ ...resolvedPR, reviewComments: true }] },
		{ prs: [{ ...resolvedPR, reviewComments: undefined }] },
		{ prs: [resolvedPR, { ...resolvedPR, number: 2, state: "open" }] },
		{ scmStatus: "merged", kanbanColumn: "archive", prs: [{ ...resolvedPR, state: "open" }] },
		{ scmStatus: "merged", kanbanColumn: "archive", prs: [{ ...resolvedPR, ci: "unknown" }] },
		{ scmStatus: "ci_failed", kanbanColumn: "needs_review", prs: [{ ...resolvedPR, state: "open", review: "changes_requested" }] },
	])("fails closed on unresolved or unknown obligation facts: %j", (facts) => {
		// Cast only at the input boundary to exercise stale/mixed-version data
		// that the compile-time WorkspaceSession union cannot represent.
		for (const status of ["terminated", "merged"] as const) {
			for (const action of ["hide", "unhide"] as const) {
				const session = card("s", { status, ...facts, hiddenAt: action === "unhide" ? "yesterday" : undefined } as Partial<WorkspaceSession>);
				expect(visibilityPreview([session], new Set(["s"]), action)).toMatchObject({ eligible: [], skipped: 1, chunks: [] });
			}
		}
	});

	it.each([
		{}, { kanbanColumn: "archive" },
		{ scmStatus: "merged", kanbanColumn: "archive", prs: [resolvedPR] },
		{ prs: [{ ...resolvedPR, state: "closed", review: "none" }] },
	] satisfies Partial<WorkspaceSession>[])("keeps genuinely inactive workers eligible: %j", (facts) => {
		expect(visibilityPreview([card("s", facts)], new Set(["s"]), "hide").eligible).toEqual(["s"]);
	});

	it("keeps the unbound UI at zero operations", () => {
		render(<BulkVisibilityPreview sessions={[card("s")]} showHidden={false} />);
		fireEvent.click(screen.getByText("Select all shown"));
		expect(screen.getByRole("status")).toHaveTextContent("1 selected, 1 eligible, 0 skipped");
		expect(screen.getByText("Run mock preview")).toBeDisabled();
		expect(screen.getByText(/integration unavailable/)).toBeInTheDocument();
		expect(screen.queryByText("Unhide Selected")).not.toBeInTheDocument();
	});

	it("reports partial and missing results by ID and retains failures for retry", async () => {
		const adapter = vi.fn().mockResolvedValue([{ id: "a" }, { id: "b", error: "Denied" }]);
		render(<BulkVisibilityPreview sessions={[card("a"), card("b"), card("c")]} showHidden adapter={adapter} />);
		fireEvent.click(screen.getByText("Select all shown"));
		fireEvent.click(screen.getByText("Run mock preview"));
		await waitFor(() => expect(screen.getByText("b: Failed: Denied")).toBeInTheDocument());
		expect(screen.getByText("a: Succeeded")).toBeInTheDocument();
		expect(screen.getByText("c: Failed: Missing or ambiguous result")).toBeInTheDocument();
		expect(screen.getByRole("status")).toHaveTextContent("2 selected, 2 eligible");
		expect(adapter).toHaveBeenCalledExactlyOnceWith("hide", ["a", "b", "c"]);
	});

	it("continues after a rejected chunk and never sends more than 200 IDs", async () => {
		const cards = Array.from({ length: 201 }, (_, index) => card(String(index), { hiddenAt: "yesterday" }));
		const adapter = vi.fn().mockRejectedValueOnce(new Error("Offline")).mockResolvedValueOnce([{ id: "200" }]);
		render(<BulkVisibilityPreview sessions={cards} showHidden adapter={adapter} />);
		fireEvent.click(screen.getByText("Unhide All (preview)"));
		fireEvent.click(screen.getByText("Run mock preview"));
		await waitFor(() => expect(screen.getByText("200: Succeeded")).toBeInTheDocument());
		expect(screen.getByText("0: Failed: Offline")).toBeInTheDocument();
		expect(adapter.mock.calls.map(([action, ids]) => [action, ids.length])).toEqual([["unhide", 200], ["unhide", 1]]);
	});

	it("recomputes obligation eligibility before running the mock", () => {
		const adapter = vi.fn();
		const { rerender } = render(<BulkVisibilityPreview sessions={[card("s")]} showHidden adapter={adapter} />);
		fireEvent.click(screen.getByText("Select all shown"));
		rerender(<BulkVisibilityPreview sessions={[card("s", { scmStatus: "ci_failed" })]} showHidden adapter={adapter} />);
		expect(screen.getByRole("status")).toHaveTextContent("1 selected, 0 eligible, 1 skipped");
		fireEvent.click(screen.getByText("Run mock preview"));
		expect(adapter).not.toHaveBeenCalled();
	});

	it("recomputes eligibility when a selected session becomes active", () => {
		const adapter = vi.fn();
		const { rerender } = render(<BulkVisibilityPreview sessions={[card("s")]} showHidden adapter={adapter} />);
		fireEvent.click(screen.getByText("Select all shown"));
		rerender(<BulkVisibilityPreview sessions={[card("s", { isTerminated: false })]} activeId="s" showHidden adapter={adapter} />);
		expect(screen.getByRole("status")).toHaveTextContent("1 selected, 0 eligible, 1 skipped");
		expect(screen.getByText("Run mock preview")).toBeDisabled();
		expect(adapter).not.toHaveBeenCalled();
	});
});
