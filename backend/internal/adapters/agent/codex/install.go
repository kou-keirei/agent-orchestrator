package codex

import "context"

// ResolveBinary resolves the executable path for the plugin.
func (p *Plugin) ResolveBinary(ctx context.Context) (string, error) {
	// Discovery and preflight must observe an npm/package-manager switch made
	// after the adapter first launched a session. Keep the session launch cache
	// private to codexBinary; this resolver is the freshness boundary used by
	// model-cache invalidation and native Chat probes.
	return ResolveCodexBinary(ctx)
}

// ResolveBinaryForWorkspace is the workspace-aware discovery surface used by
// session launches and Codex Chat. It deliberately does not populate the
// machine-wide launch cache: one plugin instance serves multiple workspaces.
func (p *Plugin) ResolveBinaryForWorkspace(ctx context.Context, workspacePath string) (string, error) {
	return ResolveCodexBinaryForWorkspace(ctx, workspacePath)
}
