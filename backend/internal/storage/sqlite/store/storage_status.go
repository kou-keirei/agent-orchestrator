package store

import (
	"context"
	"fmt"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	storagesvc "github.com/aoagents/agent-orchestrator/backend/internal/service/storage"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

// ListStorageWorkspaces reads a bounded live page of durable workspace ownership.
// It never probes filesystem paths or invokes lifecycle/workspace adapters.
func (s *Store) ListStorageWorkspaces(ctx context.Context, after string, limit int) ([]storagesvc.WorkspaceRecord, error) {
	if limit < 1 || limit > storagesvc.MaxStatusPage+1 || len(after) > 4096 {
		return nil, fmt.Errorf("invalid storage page")
	}
	rows, err := s.qr.ListStorageWorkspaces(ctx, gen.ListStorageWorkspacesParams{AfterID: domain.SessionID(after), PageLimit: int64(limit)})
	if err != nil {
		return nil, err
	}
	result := make([]storagesvc.WorkspaceRecord, 0, len(rows))
	for _, row := range rows {
		id, ok := row.ID.(string)
		if !ok {
			return nil, fmt.Errorf("invalid storage identity type")
		}
		result = append(result, storagesvc.WorkspaceRecord{ID: id, SessionID: string(row.SessionID), Path: row.Path,
			Generation: row.CleanupGeneration, Revision: row.Revision, Pinned: row.IsPinned})
	}
	return result, nil
}
