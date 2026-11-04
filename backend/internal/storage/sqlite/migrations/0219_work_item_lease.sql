-- +goose Up
-- One current lease, referencing the accepted identity rather than copying it.
-- Expiry is only a logical authorization deadline; it never reclaims ownership.
CREATE TABLE work_item_leases (
    work_item_id TEXT NOT NULL PRIMARY KEY REFERENCES work_items(id),
    owner_generation INTEGER NOT NULL CHECK (typeof(owner_generation) = 'integer' AND owner_generation > 0),
    revision INTEGER NOT NULL CHECK (typeof(revision) = 'integer' AND revision > 0),
    lease_id TEXT NOT NULL,
    duration_ms INTEGER NOT NULL CHECK (duration_ms > 0),
    expires_at_ms INTEGER NOT NULL,
    allowed_effects TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('active', 'released', 'completed'))
);

-- +goose Down
DROP TABLE work_item_leases;
