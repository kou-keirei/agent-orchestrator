package store

import (
	"context"
	"fmt"
)

// ProjectProviderEventBatch commits a bounded live-stream unit atomically.
// Each ProjectProviderEvent still owns its generation check, archive identity,
// dedupe and projection. Callers acknowledge native IDs only after this returns.
func (s *Store) ProjectProviderEventBatch(ctx context.Context, project func(context.Context) error) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin provider event batch: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	txCtx := context.WithValue(ctx, conversationProjectionTxKey{}, s.qw.WithTx(tx))
	if err := project(txCtx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit provider event batch: %w", err)
	}
	return nil
}
