import { useQuery, useQueryClient, type Query } from "@tanstack/react-query";
import type { TraySessionEntry } from "../../shared/tray";
import { useEffect, useMemo } from "react";
import type { components } from "../../api/schema";
import { apiClient, apiErrorCode, hasTrustedApiBaseUrl } from "../lib/api-client";
import type { CloudCpProject, CloudCpSession } from "../lib/cloud-cp";
import { useCloudCp } from "./useCloudCp";
import { useCloudOrg } from "./useCloudOrg";
import { mockWorkspaces } from "../lib/mock-data";
import { usesPreviewWorkspaceData } from "../lib/preview-mode";
import { toReviewerHarnessId } from "../lib/reviewer-harnesses";
import { captureRendererEvent } from "../lib/telemetry";
import { agentSwitchVisibility } from "../lib/agent-switch-visibility";
import {
	type AgentSwitchSummary,
	type PRState,
	type PullRequestFacts,
	toAgentProvider,
	toKanbanColumn,
	toProjectKind,
	toSessionActivity,
	toSessionStatus,
	newestActiveOrchestrator,
	attentionZone,
	sessionIsActive,
	workerSessions,
	type WorkspaceSession,
	type WorkspaceSummary,
	STANDALONE_PROJECT_KIND,
	STANDALONE_WORKSPACE_ID,
} from "../types/workspace";

const AD_HOC_AGENTS_WORKSPACE_NAME = "Ad hoc agents";

function mergeWorkspaceSnapshots(
	primary: readonly WorkspaceSummary[],
	additional: readonly WorkspaceSummary[],
): WorkspaceSummary[] {
	const merged = new Map(primary.map((workspace) => [workspace.id, workspace]));
	for (const workspace of additional) {
		const existing = merged.get(workspace.id);
		if (!existing) {
			merged.set(workspace.id, workspace);
			continue;
		}
		const sessions = new Map(existing.sessions.map((session) => [session.id, session]));
		// Additional snapshots fill gaps without replacing current visible rows.
		for (const session of workspace.sessions) {
			if (!sessions.has(session.id)) sessions.set(session.id, session);
		}
		merged.set(workspace.id, { ...existing, sessions: [...sessions.values()] });
	}
	return [...merged.values()];
}

function placeStandaloneWorkspaceLast(workspaces: WorkspaceSummary[]): WorkspaceSummary[] {
	const standalone = workspaces.find((workspace) => workspace.id === STANDALONE_WORKSPACE_ID);
	if (!standalone) return workspaces;
	return [
		...workspaces.filter((workspace) => workspace.id !== STANDALONE_WORKSPACE_ID),
		standalone,
	];
}

function toAgentSwitchSummary(
	agentSwitch: components["schemas"]["AgentSwitch"],
): AgentSwitchSummary {
	return {
		agentHandoffStatus: agentSwitch.agentHandoffStatus,
		errorCode: agentSwitch.errorCode,
		fromHarness: agentSwitch.fromHarness,
		id: agentSwitch.id,
		state: agentSwitch.state,
		targetHarness: agentSwitch.targetHarness,
		updatedAt: agentSwitch.updatedAt,
	};
}

function toPullRequestFacts(pr: components["schemas"]["SessionPRFacts"]): PullRequestFacts {
	return {
		url: pr.url,
		number: pr.number,
		state: pr.state as PRState,
		ci: pr.ci,
		review: pr.review,
		mergeability: pr.mergeability,
		reviewComments: pr.reviewComments,
		updatedAt: pr.updatedAt,
	};
}

function toWorkspaceSession(
	session: components["schemas"]["ControllersSessionView"],
	project: Pick<WorkspaceSummary, "id" | "name">,
): WorkspaceSession {
	const statusReadiness = session.statusReadiness ?? "ready";
	const status =
		statusReadiness === "ready" ? toSessionStatus(session.status, session.isTerminated) : "unknown";
	const scmStatus = session.scmStatus ? toSessionStatus(session.scmStatus) : undefined;
	const kanbanColumn = toKanbanColumn(session.kanbanColumn, status);
	const activity = statusReadiness === "ready" ? toSessionActivity(session.activity) : undefined;
	if (statusReadiness === "ready" && status === "unknown") reportUnknownSessionField("status", session.status);
	if (statusReadiness === "ready" && (!activity || activity.state === "unknown")) {
		reportUnknownSessionField("activity", session.activity?.state);
	}
	return {
		id: session.id,
		terminalHandleId: session.terminalHandleId,
		terminalGeneration: session.terminalGeneration,
		workspaceId: project.id,
		workspaceName: project.name,
		title: session.displayName ?? session.issueId ?? session.id,
		issueId: session.issueId,
		provider: toAgentProvider(session.harness),
		reviewerHarness: toReviewerHarnessId(session.reviewerHarness),
		reviewerConfig: session.reviewerConfig
			? {
				model: session.reviewerConfig.model ?? undefined,
				mode: session.reviewerConfig.mode ?? undefined,
				permissions: session.reviewerConfig.permissions ?? undefined,
			}
			: undefined,
		autoReviewEnabled: session.autoReviewEnabled ?? false,
		kind: session.kind === "orchestrator" ? "orchestrator" : session.kind === "worker" ? "worker" : undefined,
		mode: session.mode === "chat" ? "chat" : "tui",
		branch: session.branch || undefined,
		status,
		scmStatus,
		kanbanColumn,
		displayStatus: session.displayStatus || undefined,
		statusReadiness,
		isTerminated: session.isTerminated,
		hiddenAt: session.hiddenAt ?? undefined,
		chatProviderPreserved: session.chatProviderPreserved,
		terminateOnPrMerge: session.terminateOnPrMerge ?? false,
		autoInjectReview: session.autoInjectReview ?? true,
		autoInjectCI: session.autoInjectCI ?? true,
		createdAt: session.createdAt,
		updatedAt: session.updatedAt,
		lastUserMessageAt: session.lastUserMessageAt ?? undefined,
		activity,
		activeAgentSwitch: session.activeAgentSwitch ? toAgentSwitchSummary(session.activeAgentSwitch) : undefined,
		previewUrl: session.previewUrl,
		previewRevision: session.previewRevision,
		isPinned: session.isPinned ?? false,
		pinnedAt: session.pinnedAt ?? undefined,
		prs: (session.prs ?? []).map(toPullRequestFacts),
	};
}

export const workspaceQueryKey = ["workspaces"] as const;
export function workspaceStatusesChecking(workspaces: WorkspaceSummary[] | undefined): boolean {
	return workspaces?.some((workspace) => workspace.sessions.some((session) => session.statusReadiness === "checking")) ?? false;
}
const reportedUnknownSessionFields = new Set<string>();

function reportUnknownSessionField(field: "status" | "activity", value?: string): void {
	const reason = value ? "unrecognized" : "missing";
	const key = `${field}:${reason}`;
	if (reportedUnknownSessionFields.has(key)) return;
	reportedUnknownSessionFields.add(key);
	void captureRendererEvent("ao.renderer.session_state_unknown", { field, reason });
}

function toLocalWorkspaceSession(
	session: components["schemas"]["ControllersSessionView"],
	workspaceId: string,
	workspaceName: string,
): WorkspaceSession {
	const status = toSessionStatus(session.status, session.isTerminated);
	const scmStatus = session.scmStatus ? toSessionStatus(session.scmStatus) : undefined;
	const kanbanColumn = toKanbanColumn(session.kanbanColumn, status);
	const activity = toSessionActivity(session.activity);
	if (status === "unknown") reportUnknownSessionField("status", session.status);
	if (!activity || activity.state === "unknown") reportUnknownSessionField("activity", session.activity?.state);
	return {
		id: session.id,
		terminalHandleId: session.terminalHandleId,
		terminalGeneration: session.terminalGeneration,
		workspaceId,
		workspaceName,
		title: session.displayName ?? session.issueId ?? session.id,
		issueId: session.issueId,
		provider: toAgentProvider(session.harness),
		reviewerHarness: toReviewerHarnessId(session.reviewerHarness),
		reviewerConfig: session.reviewerConfig ? {
			model: session.reviewerConfig.model ?? undefined,
			mode: session.reviewerConfig.mode ?? undefined,
			permissions: session.reviewerConfig.permissions ?? undefined,
		} : undefined,
		autoReviewEnabled: session.autoReviewEnabled ?? false,
		kind: session.kind === "orchestrator" ? "orchestrator" : session.kind === "worker" ? "worker" : undefined,
		// Carried through verbatim: the session surface must render from
		// the mode this session was created with, not from the current default.
		mode: session.mode === "chat" ? "chat" : "tui",
		branch: session.branch || undefined,
		status,
		scmStatus,
		kanbanColumn,
		displayStatus: session.displayStatus || undefined,
		isTerminated: session.isTerminated,
		hiddenAt: session.hiddenAt ?? undefined,
		terminateOnPrMerge: session.terminateOnPrMerge ?? false,
		autoInjectReview: session.autoInjectReview ?? true,
		autoInjectCI: session.autoInjectCI ?? true,
		createdAt: session.createdAt,
		updatedAt: session.updatedAt,
		lastUserMessageAt: session.lastUserMessageAt ?? undefined,
		activity,
		activeAgentSwitch: session.activeAgentSwitch ? toAgentSwitchSummary(session.activeAgentSwitch) : undefined,
		previewUrl: session.previewUrl,
		previewRevision: session.previewRevision,
		isPinned: session.isPinned ?? false,
		pinnedAt: session.pinnedAt ?? undefined,
		prs: (session.prs ?? []).map(toPullRequestFacts),
	};
}

// e2e seam (dev:web only): the Playwright fake-agent harness injects
// `window.__aoFakeAgent` (see e2e/support/fake-bridge.ts) to drive a
// deterministic, mutable session timeline off the SSE refetch path. Compiled
// out of the packaged build — the packaged renderer never sets VITE_NO_ELECTRON
// and always hits the real daemon.
type FakeAgentSeam = { snapshot: () => WorkspaceSummary[] };

async function fetchWorkspaces(includeHidden = false): Promise<WorkspaceSummary[]> {
	if (usesPreviewWorkspaceData) {
		const fake =
			typeof window !== "undefined"
				? (window as unknown as { __aoFakeAgent?: FakeAgentSeam }).__aoFakeAgent
				: undefined;
		return fake ? fake.snapshot() : mockWorkspaces;
	}
	if (!hasTrustedApiBaseUrl()) {
		throw new Error("AO daemon API is not ready");
	}

	const [{ data: projectsData, error: projectsError }, { data: sessionsData, error: sessionsError }] =
		await Promise.all([
			apiClient.GET("/api/v1/projects"),
			apiClient.GET("/api/v1/sessions", { params: { query: includeHidden ? { includeHidden: true } : {} } }),
		]);

	if (projectsError || sessionsError) {
		agentSwitchVisibility.setQueryHealthy("active", false, "workspaces");
		agentSwitchVisibility.setQueryHealthy("history", false, "workspaces");
		throw projectsError ?? sessionsError;
	}
	agentSwitchVisibility.setQueryHealthy("active", true, "workspaces");
	agentSwitchVisibility.setQueryHealthy("history", true, "workspaces");

	const sessions = sessionsData?.sessions ?? [];
	const projects = (projectsData?.projects ?? []).map((project) => {
		const kind = toProjectKind(project.kind);
		return {
			id: project.id,
			name: project.name,
			kind,
			path: project.path,
			folderMissing: project.folderMissing,
			orchestratorAgent: project.orchestratorAgent ? toAgentProvider(project.orchestratorAgent) : undefined,
			sessions: sessions
				.filter((session) => session.projectId === project.id)
				.map((session) => toWorkspaceSession(session, project)),
		};
	});
	const standalone: WorkspaceSummary = {
		id: STANDALONE_WORKSPACE_ID,
		name: AD_HOC_AGENTS_WORKSPACE_NAME,
		kind: STANDALONE_PROJECT_KIND,
		path: "Not attached to a project",
		sessions: sessions
			.filter((session) => !session.projectId)
			.map((session) => toLocalWorkspaceSession(session, STANDALONE_WORKSPACE_ID, AD_HOC_AGENTS_WORKSPACE_NAME)),
	};
	return standalone.sessions.length > 0 ? placeStandaloneWorkspaceLast([...projects, standalone]) : projects;
}

// Shared so route loaders can prefetch via queryClient.ensureQueryData (paired
// with the router's defaultPreload: "intent") and the hook reads the same cache.
export const workspaceQueryOptions = {
	queryKey: workspaceQueryKey,
	queryFn: () => fetchWorkspaces(false),
	retry: 1,
	staleTime: 10_000,
	refetchInterval: (query: Query<WorkspaceSummary[]>) =>
		workspaceStatusesChecking(query.state.data) ? 300 : 15_000,
};

export const hiddenWorkspaceQueryKey = ["workspaces", "include-hidden"] as const;
export const hiddenWorkspaceQueryOptions = {
	queryKey: hiddenWorkspaceQueryKey,
	queryFn: () => fetchWorkspaces(true),
	retry: 1,
	staleTime: 10_000,
	refetchInterval: (query: Query<WorkspaceSummary[]>) =>
		workspaceStatusesChecking(query.state.data) ? 300 : 15_000,
};

export function useHiddenWorkspaceQuery(options: WorkspaceSubscriptionOptions = {}) {
	return useQuery({ ...hiddenWorkspaceQueryOptions, subscribed: options.subscribed });
}

// Cloud projects are a separate query so a control-plane failure can never
// break the local list: on error TanStack keeps this query's last known data,
// and until the first successful fetch the merge below simply omits cloud
// items. Invalidated by the cloud create flow (CreateProjectFlow).
export const cloudProjectsQueryKey = ["cloud-projects"] as const;
export const cloudSessionsQueryKey = ["cloud-sessions"] as const;

// Maps one control-plane session onto the board's session shape. Cloud sessions
// carry the same status/activity/harness vocabulary as local ones, so the same
// product-ui mappers apply; fields with no cloud analogue take safe defaults.
function toCloudWorkspaceSession(
	session: CloudCpSession,
	project: CloudCpProject,
	orgId: string,
): WorkspaceSession {
	return {
		id: session.id,
		// The terminal pane only mounts for a session that has a terminal handle.
		// A cloud session's PTY is addressed by the session id over its ticketed
		// CP WebSocket, so the session id is its handle.
		terminalHandleId: session.id,
		// The worker epoch advances on every fresh worker connection (resume from
		// idle-pause, restore, re-provision). Folding it into the terminal
		// generation makes the terminal pane re-mint against the new epoch and
		// attach to the live agent, instead of clinging to the previous epoch's
		// exited terminal (the "connected but TERMINAL ENDED / can't type" loop,
		// which then idle-pauses the session again because nothing attached).
		// Stable within an epoch, so it does not churn the pane between resumes.
		terminalGeneration: session.workerEpoch ? String(session.workerEpoch) : undefined,
		workspaceId: project.id,
		workspaceName: project.displayName,
		title: session.displayName || session.id,
		provider: toAgentProvider(session.harness),
		kind: session.kind === "orchestrator" ? "orchestrator" : "worker",
		branch: session.branch || undefined,
		status: toSessionStatus(session.status, session.isTerminated),
		isTerminated: session.isTerminated,
		runtimeConnected: session.runtimeConnected,
		createdAt: session.createdAt,
		updatedAt: session.updatedAt,
		activity: toSessionActivity({ state: session.activityState }),
		prs: [],
		// Marks this as a control-plane session so the terminal opens against the
		// CP (ticket + sandbox WebSocket) instead of the local daemon mux.
		cloud: {
			orgId,
			sandboxProvider: session.sandboxProvider,
			desiredState: session.desiredState,
			observedState: session.observedState,
		},
	};
}

function toCloudWorkspace(
	project: CloudCpProject,
	sessions: CloudCpSession[],
	orgId: string,
): WorkspaceSummary {
	return {
		id: project.id,
		name: project.displayName,
		kind: "cloud",
		// Cloud projects run in control-plane sandboxes; there is no local folder.
		path: "",
		sessions: sessions
			.filter((session) => session.projectId === project.id)
			.map((session) => toCloudWorkspaceSession(session, project, orgId)),
	};
}

type WorkspaceSubscriptionOptions = {
	subscribed?: boolean;
	/** Explicit history surfaces opt into presentation-hidden local sessions. */
	includeHidden?: boolean;
};

export function useCloudProjectsQuery(options: WorkspaceSubscriptionOptions = {}) {
	const { client, ready, baseUrl } = useCloudCp();
	const { org } = useCloudOrg();
	const orgId = org?.id;
	return useQuery({
		queryKey: [...cloudProjectsQueryKey, baseUrl, orgId ?? ""],
		enabled: ready && orgId !== undefined,
		subscribed: options.subscribed,
		retry: 1,
		queryFn: async (): Promise<CloudCpProject[]> => {
			if (orgId === undefined) return [];
			// First page only (control-plane max page size); pagination UI is a
			// later phase alongside cloud sessions.
			const response = await client.listProjects(orgId, { limit: 100 });
			return response.items;
		},
	});
}

export function useCloudSessionsQuery(options: WorkspaceSubscriptionOptions = {}) {
	const { client, ready, baseUrl } = useCloudCp();
	const { org } = useCloudOrg();
	const orgId = org?.id;
	return useQuery({
		queryKey: [...cloudSessionsQueryKey, baseUrl, orgId ?? ""],
		enabled: ready && orgId !== undefined,
		subscribed: options.subscribed,
		retry: 1,
		// A provisioning sandbox changes state without a client action, so poll to
		// reflect requested -> running -> ready the same way local sessions stream.
		refetchInterval: 5000,
		queryFn: async (): Promise<CloudCpSession[]> => {
			if (orgId === undefined) return [];
			const response = await client.listSessions(orgId, { limit: 100 });
			return response.items;
		},
	});
}

export function useWorkspaceQuery(options: WorkspaceSubscriptionOptions = {}) {
	const local = useQuery({
		...(options.includeHidden ? hiddenWorkspaceQueryOptions : workspaceQueryOptions),
		subscribed: options.subscribed,
	});
	const cloud = useCloudProjectsQuery(options);
	const cloudSessions = useCloudSessionsQuery(options);
	const { org, ready } = useCloudOrg();
	const orgId = org?.id;
	const localData = local.data;
	const cloudData = cloud.data;
	const cloudSessionData = cloudSessions.data;
	const data = useMemo(() => {
		// Local stays authoritative for loading/error semantics: cloud items only
		// render once the local list exists, and never replace it.
		if (localData === undefined || cloudData === undefined || cloudData.length === 0) return localData;
		// Signing out (or turning the offering off) disables the cloud queries,
		// but react-query keeps their last data; without this gate the stale
		// cloud projects would keep rendering for a signed-out user.
		if (!ready || orgId === undefined) return localData;
		const sessions = cloudSessionData ?? [];
		return placeStandaloneWorkspaceLast([
			...localData,
			...cloudData.map((project) => toCloudWorkspace(project, sessions, orgId)),
		]);
	}, [localData, cloudData, cloudSessionData, orgId, ready]);
	return { ...local, data };
}

/**
 * Subscribe a detail surface to one session instead of the complete workspace
 * tree. TanStack Query applies structural sharing to the selected value, so an
 * activity update elsewhere no longer redraws the open session workspace.
 */
export function useWorkspaceSession(sessionId: string) {
	const queryClient = useQueryClient();
	const selectLocalSession = useMemo(
		() => (workspaces: WorkspaceSummary[]) =>
			workspaces.flatMap((workspace) => workspace.sessions).find((session) => session.id === sessionId),
		[sessionId],
	);
	const local = useQuery({ ...workspaceQueryOptions, select: selectLocalSession });
	const localWorkspaces = useQuery({ ...workspaceQueryOptions, subscribed: false, enabled: Boolean(sessionId) });
	const hidden = useQuery({
		...hiddenWorkspaceQueryOptions,
		select: selectLocalSession,
		enabled: Boolean(sessionId) && local.data === undefined,
	});
	const direct = useQuery({
		queryKey: ["session", sessionId],
		enabled: Boolean(sessionId) && local.data === undefined && hidden.isFetched && hidden.data === undefined,
		retry: (attempt, error) => apiErrorCode(error) === "SESSION_NOT_FOUND" && attempt < 4,
		retryDelay: 250,
		queryFn: async () => {
			const { data, error } = await apiClient.GET("/api/v1/sessions/{sessionId}", {
				params: { path: { sessionId } },
			});
			if (error) throw error;
			const session = data?.session;
			if (!session) return undefined;
			const project = session.projectId
				? localWorkspaces.data?.find((workspace) => workspace.id === session.projectId) ??
					({ id: session.projectId, name: "" } satisfies Pick<WorkspaceSummary, "id" | "name">)
				: ({ id: STANDALONE_WORKSPACE_ID, name: AD_HOC_AGENTS_WORKSPACE_NAME } satisfies Pick<WorkspaceSummary, "id" | "name">);
			return toWorkspaceSession(session, project);
		},
	});
	const cloud = useCloudProjectsQuery();
	const cloudSessions = useCloudSessionsQuery();
	const { org, ready } = useCloudOrg();
	const resolvedDirectSession = useMemo(() => {
		if (!direct.data) return undefined;
		const workspace = localWorkspaces.data?.find((candidate) => candidate.id === direct.data?.workspaceId);
		if (!workspace || direct.data.workspaceName === workspace.name) return direct.data;
		return { ...direct.data, workspaceName: workspace.name };
	}, [direct.data, localWorkspaces.data]);
	const cloudSession = useMemo(() => {
		if (!ready || !org?.id || !cloud.data || !cloudSessions.data) return undefined;
		const session = cloudSessions.data.find((candidate) => candidate.id === sessionId);
		if (!session) return undefined;
		const project = cloud.data.find((candidate) => candidate.id === session.projectId);
		return project ? toCloudWorkspaceSession(session, project, org.id) : undefined;
	}, [cloud.data, cloudSessions.data, org?.id, ready, sessionId]);
	useEffect(() => {
		if (!resolvedDirectSession) return;
		const cacheKey = resolvedDirectSession.hiddenAt ? hiddenWorkspaceQueryKey : workspaceQueryKey;
		queryClient.setQueryData<WorkspaceSummary[]>(cacheKey, (current) => {
			if (!current) return current;
			let changed = false;
			const next = current.map((workspace) => {
				if (workspace.id !== resolvedDirectSession.workspaceId) return workspace;
				if (workspace.sessions.some((session) => session.id === resolvedDirectSession.id)) return workspace;
				changed = true;
				return { ...workspace, sessions: [...workspace.sessions, resolvedDirectSession] };
			});
			return changed ? next : current;
		});
	}, [queryClient, resolvedDirectSession]);
	return {
		...local,
		data: local.data ?? hidden.data ?? resolvedDirectSession ?? cloudSession,
		isLoading: local.isLoading || hidden.isLoading || direct.isLoading,
	};
}

export type WorkspaceScope = {
	project?: Pick<WorkspaceSummary, "id" | "kind" | "name" | "orchestratorAgent">;
	hasWorkerSessions: boolean;
	session?: WorkspaceSession;
	orchestrator?: WorkspaceSession;
};

function selectWorkspaceScope(
	workspaces: WorkspaceSummary[],
	projectId: string | undefined,
	sessionId: string | undefined,
): WorkspaceScope {
	const session = sessionId
		? workspaces.flatMap((workspace) => workspace.sessions).find((candidate) => candidate.id === sessionId)
		: undefined;
	const resolvedProjectId = session?.workspaceId ?? projectId;
	const workspace = resolvedProjectId ? workspaces.find((candidate) => candidate.id === resolvedProjectId) : undefined;
	// Do not carry the project's complete sessions array into shell chrome. With
	// React Query's structural sharing, this small metadata projection retains
	// its identity when another session in the same project streams an update.
	const project = workspace
		? {
				id: workspace.id,
				kind: workspace.kind,
				name: workspace.name,
				orchestratorAgent: workspace.orchestratorAgent,
			}
		: undefined;
	return {
		project, session,
		// Hidden live workers stay out of shell/topbar presence and empty-state
		// decisions. Direct session and orchestrator resolution above remains
		// inclusive so durable action targets are not lost.
		hasWorkerSessions: workspace
			? workerSessions(workspace.sessions).some(
					(candidate) =>
						!candidate.hiddenAt ||
						candidate.isTerminated === true ||
						candidate.status === "terminated" ||
						candidate.kanbanColumn === "archive",
				)
			: false,
		orchestrator: workspace ? newestActiveOrchestrator(workspace.sessions) : undefined,
	};
}

/**
 * Subscribe shell chrome to just the routed project and session. This avoids
 * redrawing the topbar for streamed activity from every other project.
 */
export function useWorkspaceScope(projectId?: string, sessionId?: string) {
	const selectLocalScope = useMemo(
		() => (workspaces: WorkspaceSummary[]) => selectWorkspaceScope(workspaces, projectId, sessionId),
		[projectId, sessionId],
	);
	// Keep visible rows authoritative for shell presentation state. Merge the
	// hidden-inclusive snapshot only for routed-session and action resolution.
	const local = useQuery({ ...workspaceQueryOptions });
	const hiddenLocal = useQuery({ ...hiddenWorkspaceQueryOptions });
	const localScope = useMemo(() => {
		const workspaces = mergeWorkspaceSnapshots(local.data ?? [], hiddenLocal.data ?? []);
		return workspaces.length > 0 ? selectLocalScope(workspaces) : undefined;
	}, [hiddenLocal.data, local.data, selectLocalScope]);
	const cloud = useCloudProjectsQuery();
	const cloudSessions = useCloudSessionsQuery();
	const { org, ready } = useCloudOrg();
	const cloudScope = useMemo(() => {
		if (!ready || !org?.id || !cloud.data) return undefined;
		const workspaces = cloud.data.map((project) => toCloudWorkspace(project, cloudSessions.data ?? [], org.id));
		return selectWorkspaceScope(workspaces, projectId, sessionId);
	}, [cloud.data, cloudSessions.data, org?.id, projectId, ready, sessionId]);
	// Match useWorkspaceQuery's local-first semantics: do not reveal cloud
	// records before the local workspace query has resolved successfully.
	const data = localScope?.project || localScope?.session || !local.isSuccess
		? localScope
		: cloudScope ?? localScope;
	return { ...local, data };
}

function selectTraySessions(workspaces: WorkspaceSummary[]): TraySessionEntry[] {
	const entries: TraySessionEntry[] = [];
	for (const workspace of workspaces) {
		for (const session of workerSessions(workspace.sessions)) {
			if (!sessionIsActive(session)) continue;
			const zone = attentionZone(session);
			if ((zone === "merge" && session.status === "merged") || (zone !== "action" && zone !== "merge")) continue;
			entries.push({
				projectId: session.workspaceId,
				projectName: workspace.name,
				sessionId: session.id,
				title: session.title,
				zone,
			});
		}
	}
	return entries;
}

/**
 * The tray lives for the whole app lifetime, but only attention-worthy worker
 * sessions affect its native payload. Select that compact projection at the
 * query boundary so ordinary streamed activity does not wake the runtime.
 */
export function useWorkspaceTraySessions() {
	// Tray attention is operational shell state, so hidden sessions still need to
	// participate even though ordinary board/sidebar queries stay presentation-only.
	const local = useQuery({ ...hiddenWorkspaceQueryOptions, select: selectTraySessions });
	const cloud = useCloudProjectsQuery();
	const cloudSessions = useCloudSessionsQuery();
	const { org, ready } = useCloudOrg();
	const cloudEntries = useMemo(() => {
		if (!ready || !org?.id || !cloud.data) return [];
		return selectTraySessions(cloud.data.map((project) => toCloudWorkspace(project, cloudSessions.data ?? [], org.id)));
	}, [cloud.data, cloudSessions.data, org?.id, ready]);
	const data = useMemo(() => {
		if (local.data === undefined) return undefined;
		return cloudEntries.length === 0 ? local.data : [...local.data, ...cloudEntries];
	}, [cloudEntries, local.data]);
	return { ...local, data };
}
