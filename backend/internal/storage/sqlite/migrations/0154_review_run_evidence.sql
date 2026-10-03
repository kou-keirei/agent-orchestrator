-- +goose Up
-- Reviewer evidence is additive. Existing rows intentionally receive empty
-- binding/provenance values, which decode as unknown/unproven rather than being
-- reconstructed from mutable session or workspace state.
ALTER TABLE review_run ADD COLUMN target_binding_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE review_run ADD COLUMN reviewer_provenance_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE review_run ADD COLUMN launch_id TEXT NOT NULL DEFAULT '';
ALTER TABLE review_run ADD COLUMN provider_native_session_id TEXT NOT NULL DEFAULT '';
ALTER TABLE review_run ADD COLUMN provider_native_first_turn_id TEXT NOT NULL DEFAULT '';
ALTER TABLE review_run ADD COLUMN provider_native_completion_id TEXT NOT NULL DEFAULT '';
ALTER TABLE review_run ADD COLUMN provider_native_observed_at TIMESTAMP;

-- The launch binding is immutable. Result fields and append-only provider-native
-- fields remain updateable, while any attempt to rewrite target/workspace,
-- requested/configured/AO-resolved provenance, or launch generation aborts.
-- +goose StatementBegin
CREATE TRIGGER review_run_immutable_binding
BEFORE UPDATE OF target_sha, target_binding_json, reviewer_provenance_json, launch_id
ON review_run
WHEN OLD.target_sha != NEW.target_sha
  OR OLD.target_binding_json != NEW.target_binding_json
  OR OLD.reviewer_provenance_json != NEW.reviewer_provenance_json
  OR OLD.launch_id != NEW.launch_id
BEGIN
    SELECT RAISE(ABORT, 'review_run immutable launch binding');
END;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TRIGGER review_run_immutable_binding;
-- +goose StatementEnd
ALTER TABLE review_run DROP COLUMN provider_native_observed_at;
ALTER TABLE review_run DROP COLUMN provider_native_completion_id;
ALTER TABLE review_run DROP COLUMN provider_native_first_turn_id;
ALTER TABLE review_run DROP COLUMN provider_native_session_id;
ALTER TABLE review_run DROP COLUMN launch_id;
ALTER TABLE review_run DROP COLUMN reviewer_provenance_json;
ALTER TABLE review_run DROP COLUMN target_binding_json;
