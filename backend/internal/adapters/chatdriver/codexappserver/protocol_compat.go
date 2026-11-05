package codexappserver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/aoagents/agent-orchestrator/backend/internal/adapters/chatdriver/codexappserver/codexproto"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	aoprocess "github.com/aoagents/agent-orchestrator/backend/internal/process"
)

// Resume needs native history and a usable continuation, not every method in
// the generated inventory. In particular prediction/read-state notifications
// and attachment-owner discovery are not part of this operation.
var resumeProtocolMethods = []string{
	codexproto.MethodThreadResume, codexproto.MethodThreadRead, codexproto.MethodTurnStart,
}

type codexProtocol struct {
	version string
	methods map[string]string
}

func (p *codexProtocol) require(methods ...string) error {
	// Pipe fixtures and an already-initialized surviving host retain their own
	// protocol. Never infer the surviving process's version from a new binary.
	if p == nil {
		return nil
	}
	for _, name := range methods {
		var direction string
		for _, method := range codexproto.Methods {
			if method.Name == name {
				direction = method.Direction
				break
			}
		}
		if direction == "" || p.methods[name] != direction {
			return fmt.Errorf("%w: %s does not declare %s as %s (got %q)",
				ports.ErrChatDriverIncompatible, p.version, name, direction, p.methods[name])
		}
	}
	return nil
}

func (p *codexProtocol) capabilities() ports.ChatCapabilities {
	caps := capabilities()
	if p.require(resumeProtocolMethods...) != nil {
		caps[ports.ChatCapabilityResume] = false
	}
	if p.require(codexproto.MethodThreadRead) != nil {
		caps[ports.ChatCapabilityHistory] = false
	}
	return caps
}

func (d *Driver) checkProtocol(ctx context.Context, contract LaunchContract, required ...string) (*codexProtocol, error) {
	if d.protocolProbe == nil {
		return nil, nil // injected pipe driver
	}
	p, err := d.protocolProbe(ctx, contract)
	if err != nil {
		return nil, err
	}
	if p == nil {
		return nil, fmt.Errorf("%w: Codex protocol probe returned no declarations", ports.ErrChatDriverIncompatible)
	}
	if err := p.require("initialize", "initialized"); err != nil {
		return nil, err
	}
	if err := p.require(required...); err != nil {
		return nil, err
	}
	return p, nil
}

// Export the exact launch contract's locally installed declarations. This does
// not initialize an account, open a thread, or prove payload/native semantics:
// normal response parsing and identity checks still own those boundaries. The
// exact version inventory check below does not replace response validation.
func probeCodexProtocol(ctx context.Context, contract LaunchContract) (*codexProtocol, error) {
	probeCtx, cancel := context.WithTimeout(ctx, capabilityProbeTimeout)
	defer cancel()
	version, err := installedCodexVersionContract(probeCtx, contract)
	if err != nil {
		return nil, fmt.Errorf("%w: protocol version: %w", ports.ErrChatDriverIncompatible, err)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("%w: protocol scratch home: %w", ports.ErrChatDriverIncompatible, err)
	}
	scratch := filepath.Join(home, ".ao", "scratch")
	if err := os.MkdirAll(scratch, 0o700); err != nil {
		return nil, fmt.Errorf("%w: protocol scratch: %w", ports.ErrChatDriverIncompatible, err)
	}
	dir, err := os.MkdirTemp(scratch, "codex-protocol-")
	if err != nil {
		return nil, fmt.Errorf("%w: protocol scratch: %w", ports.ErrChatDriverIncompatible, err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	cmd := aoprocess.CommandContext(probeCtx, contract.Executable, "app-server", "generate-json-schema", "--out", dir)
	cmd.Dir, cmd.Env = contract.WorkspacePath, contract.Environment
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%w: %s protocol export: %w", ports.ErrChatDriverIncompatible, strings.TrimSpace(version), err)
	}
	return readSelectedCodexProtocol(dir, strings.TrimSpace(version))
}

func readCodexProtocol(dir, version string) (*codexProtocol, error) {
	p := &codexProtocol{version: version, methods: make(map[string]string)}
	for _, direction := range []string{"ClientRequest", "ClientNotification", "ServerRequest", "ServerNotification"} {
		if err := p.readDirection(filepath.Join(dir, direction+".json"), direction); err != nil {
			return nil, fmt.Errorf("%w: %s protocol %s: %w", ports.ErrChatDriverIncompatible, version, direction, err)
		}
	}
	return p, nil
}

func (p *codexProtocol) readDirection(path, direction string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	const maxSchemaBytes = 1 << 20
	raw, err := io.ReadAll(io.LimitReader(f, maxSchemaBytes+1))
	if err != nil {
		return err
	}
	if len(raw) > maxSchemaBytes {
		return fmt.Errorf("schema exceeds %d bytes", maxSchemaBytes)
	}
	var doc struct {
		OneOf []struct {
			Properties struct {
				Method struct {
					Enum []string `json:"enum"`
				} `json:"method"`
			} `json:"properties"`
		} `json:"oneOf"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return err
	}
	if len(doc.OneOf) == 0 {
		return fmt.Errorf("missing method declarations")
	}
	for _, arm := range doc.OneOf {
		if len(arm.Properties.Method.Enum) != 1 || arm.Properties.Method.Enum[0] == "" {
			return fmt.Errorf("unrecognized method declaration")
		}
		name := arm.Properties.Method.Enum[0]
		if _, exists := p.methods[name]; exists {
			return fmt.Errorf("duplicate method declaration %q", name)
		}
		p.methods[name] = direction
	}
	return nil
}
