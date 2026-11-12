package codexappserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/adapters/chatdriver/codexappserver/codexproto"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

const workerReportToolName = "ao_worker_report"
const workerReportToolBindWait = 5 * time.Second

var _ ports.AuthenticatedControllerReportSource = (*conversation)(nil)
var _ ports.CommittedWorkerReportTool = (*conversation)(nil)

func workerReportToolSpecs() []codexproto.DynamicToolSpec {
	name := workerReportToolName
	description := "Submit an explicitly authored WorkItem report to AO's frozen original-parent outbox. Acceptance is not parent semantic acknowledgement. Supply report content only; AO owns work, parent, session and lease identity. Retry with the same id and content."
	return []codexproto.DynamicToolSpec{{Type: codexproto.DynamicToolSpecTypeFunction, Name: &name, Description: &description,
		InputSchema: json.RawMessage(`{"type":"object","properties":{"id":{"type":"string"},"type":{"type":"string"},"version":{"type":"integer"},"content":{"type":"string"},"artifactReference":{"type":"string"},"digest":{"type":"string"}},"required":["id","type","version","content","artifactReference","digest"],"additionalProperties":false}`)}}
}

// BindAuthenticatedControllerReportCallback installs only a daemon-issued
// callback for this native turn. It does not activate it during installation.
func (c *conversation) BindAuthenticatedControllerReportCallback(ctx context.Context, providerTurnID string,
	submit func(context.Context, ports.AuthoredWorkerReportContent) (domain.WorkerReportReceipt, error),
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	turn := c.nativeID(providerTurnID)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || !c.reportToolOffered || submit == nil || turn == "" || c.activeTurn != turn || c.reportToolTurn == turn {
		return domain.WorkerReportNativeIngressBlock{}
	}
	c.reportToolTurn, c.reportToolSubmit, c.reportToolReady = turn, submit, false
	return nil
}

// ConnectCommittedWorkerReportTurn is called by AO after primary projection,
// never by tool dispatch. Do not block the projector on controller gates: Stop
// may own one while waiting for that projector to finish.
func (c *conversation) ConnectCommittedWorkerReportTurn(ctx context.Context, providerTurnID string, connect func(context.Context) error) {
	go func() {
		bindCtx, cancel := context.WithTimeout(ctx, workerReportToolBindWait)
		defer cancel()
		if err := connect(bindCtx); err != nil {
			c.log.Debug("native worker report tool held", "error", err)
			return
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		turn := c.nativeID(providerTurnID)
		if !c.closed && c.activeTurn == turn && c.reportToolTurn == turn && c.reportToolSubmit != nil {
			c.reportToolReady = true
			close(c.reportToolChanged)
			c.reportToolChanged = make(chan struct{})
		}
	}()
}

func (c *conversation) handleWorkerReportTool(ctx context.Context, raw json.RawMessage) (any, error) {
	var p codexproto.DynamicToolCallParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, err
	}
	c.mu.Lock()
	offered := c.reportToolOffered
	threadID := c.threadID
	c.mu.Unlock()
	if !offered || p.Tool != workerReportToolName || p.Namespace != nil {
		return nil, c.refuseDynamicToolCall(raw)
	}
	if p.ThreadID == "" || p.ThreadID != threadID || p.TurnID == "" || p.CallID == "" {
		return nil, domain.ErrWorkerReportActorMismatch
	}
	// Strict content decoding rejects even plausible authority-shaped fields.
	var args struct {
		ID                string `json:"id"`
		Type              string `json:"type"`
		Version           int    `json:"version"`
		Content           string `json:"content"`
		ArtifactReference string `json:"artifactReference"`
		Digest            string `json:"digest"`
	}
	if len(p.Arguments) > 1<<20 {
		return nil, domain.ErrInvalidWorkerReport
	}
	decoder := json.NewDecoder(bytes.NewReader(p.Arguments))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&args); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, domain.ErrInvalidWorkerReport
	}
	content := ports.AuthoredWorkerReportContent{ID: args.ID, Type: args.Type, Version: args.Version,
		Content: args.Content, ArtifactReference: args.ArtifactReference, Digest: args.Digest}
	// Provider dispatch and durable projection are independent consumers. Wait
	// only for AO's already-triggered committed-turn installation, never mint it
	// in response to this request. The retained bridge rechecks all generations.
	waitCtx, cancel := context.WithTimeout(ctx, workerReportToolBindWait)
	defer cancel()
	for {
		c.mu.Lock()
		if c.closed || c.activeTurn != "" && c.activeTurn != p.TurnID {
			c.mu.Unlock()
			return nil, domain.ErrWorkerReportActorMismatch
		}
		submit, ready, changed := c.reportToolSubmit, c.activeTurn == p.TurnID && c.reportToolReady && c.reportToolTurn == p.TurnID, c.reportToolChanged
		c.mu.Unlock()
		if ready && submit != nil {
			receipt, err := submit(ctx, content)
			if err != nil {
				return nil, err
			}
			// Return only accepted/outbox identity, never a delivery or ACK claim.
			body, err := json.Marshal(map[string]any{"reportId": receipt.Report.ID, "eventId": receipt.EventID, "accepted": true, "semanticAck": "HOLD"})
			if err != nil {
				return nil, err
			}
			return map[string]any{"success": true, "contentItems": []map[string]string{{"type": "inputText", "text": string(body)}}}, nil
		}
		select {
		case <-changed:
		case <-waitCtx.Done():
			return nil, domain.WorkerReportNativeIngressBlock{}
		}
	}
}
