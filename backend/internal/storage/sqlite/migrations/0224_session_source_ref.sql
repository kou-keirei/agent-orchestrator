-- +goose Up
-- The named source ref is historical birth evidence, separate from the exact
-- execution commit and comparison metadata. Existing sessions have no ref proof.
ALTER TABLE sessions ADD COLUMN source_ref TEXT NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE sessions DROP COLUMN source_ref;
