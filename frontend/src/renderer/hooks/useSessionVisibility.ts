import { useMutation, useQueryClient } from "@tanstack/react-query";
import type { WorkspaceSession } from "../types/workspace";
import { hiddenWorkspaceQueryKey, workspaceQueryKey } from "./useWorkspaceQuery";
import { apiClient, apiErrorMessage } from "../lib/api-client";

async function changeVisibility(session: WorkspaceSession, hidden: boolean): Promise<void> {
	const request = hidden
		? apiClient.POST("/api/v1/sessions/{sessionId}/hide", { params: { path: { sessionId: session.id } } })
		: apiClient.DELETE("/api/v1/sessions/{sessionId}/hide", { params: { path: { sessionId: session.id } } });
	const { error, response } = await request;
	if (error) {
		throw new Error(apiErrorMessage(error, response ? `Failed to change session visibility (${response.status})` : "Failed to change session visibility"));
	}
}

function useSessionVisibility(hidden: boolean) {
	const queryClient = useQueryClient();
	return useMutation({
		mutationFn: (session: WorkspaceSession) => changeVisibility(session, hidden),
		onSuccess: async () => {
			await Promise.all([
				queryClient.invalidateQueries({ queryKey: workspaceQueryKey }),
				queryClient.invalidateQueries({ queryKey: hiddenWorkspaceQueryKey }),
			]);
		},
	});
}

export function useHideSession() {
	return useSessionVisibility(true);
}

export function useUnhideSession() {
	return useSessionVisibility(false);
}
