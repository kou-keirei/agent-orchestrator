# ao spawn

Spawn a worker agent session in a registered project, or a standalone workspace session that is not tied to a project.
Standalone sessions run in an AO-managed directory. Register a project first with `ao project add` for project-scoped sessions.

## Syntax

```
ao spawn [flags]
```

## Flags

| Flag | Meaning | Default / Required |
|---|---|---|
| `--branch string` | Branch for the session worktree | `ao/<session-id>/root` |
| `--claim-pr string` | Immediately claim an existing PR for the spawned session | - |
| `--harness string` | Agent harness to use (see list below) | Project `worker.agent`; required if the project has none |
| `--issue string` | Issue id to associate with the session | - |
| `--model string` | Exact agent model override for this session | Project/role model when omitted |
| `--name string` | Display name shown in the sidebar (max 20 characters) | Required |
| `--no-takeover` | Refuse if another active session owns the claimed PR (requires `--claim-pr`) | - |
| `--project string` | Project id to spawn the session in | Optional when `--standalone` is used; defaults to `AO_PROJECT_ID` or the current repo's registered project |
| `--standalone` | Spawn a projectless worker session in an AO-managed directory | Disabled when `--project` is set |
| `--effort string` | Exact provider-advertised reasoning effort for this session | Provider/model default when omitted |
| `--prompt string` | Initial prompt for the agent | - |

`--agent` is an alias for `--harness`.

Available harnesses: `claude-code`, `codex`, `aider`, `opencode`, `grok`, `droid`, `amp`, `agy`, `crush`, `cursor`, `qwen`, `copilot`, `goose`, `auggie`, `continue`, `devin`, `cline`, `kimi`, `kiro`, `kilocode`, `vibe`, `pi`, `autohand`.

## Model and effort dispatch

When a parent or dispatch authority supplies an explicit model or effort, pass
that exact value to `ao spawn`. Do not substitute, downgrade, upgrade, or
normalize it to another supported value, and do not reinterpret one value as
another (for example, keep `max` as `max`). Omitted fields may use the normal
project, role, or provider defaults.

A successful spawn only proves that AO accepted and created the session; it is
not conformance proof. External control or acceptance must verify the actual
child provider-selected model and reasoning effort for the spawned child and
reject any mismatch.

## Examples

```bash
# Spawn a worker for issue 142 in the agent-orchestrator project
ao spawn --project agent-orchestrator --issue 142 --name "fix-session-leak" --prompt "Fix the session leak described in issue 142. Branch off upstream/main."
```

```bash
# Spawn a worker and immediately claim an open PR
ao spawn --project agent-orchestrator --name "review-pr-88" --claim-pr 88 --harness claude-code
```

```bash
# Preserve explicit parent-supplied model and effort values
ao spawn --project agent-orchestrator --name "luna-worker" --harness codex \
  --model gpt-5.6-sol --effort max --prompt "Run the assigned task."
```
