-- +goose Up
-- +goose StatementBegin
-- This records AO intent, never provider acceptance. One attempt per durable
-- turn; duplicate reservations cannot authorize another external call.
CREATE TABLE conversation_send_attempts (
    turn_id TEXT PRIMARY KEY REFERENCES conversation_turns(id) ON DELETE CASCADE,
    conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    state TEXT NOT NULL CHECK (state IN ('pending', 'reserved', 'legacy', 'bound', 'not_delivered')),
    controller_generation TEXT NOT NULL DEFAULT '',
    reserved_at TIMESTAMP,
    non_delivery_evidence TEXT NOT NULL DEFAULT '',
    CHECK ((state IN ('pending', 'legacy') AND reserved_at IS NULL AND controller_generation = '')
        OR (state IN ('reserved', 'bound', 'not_delivered') AND reserved_at IS NOT NULL AND controller_generation <> '')),
    CHECK (state <> 'not_delivered' OR non_delivery_evidence <> '')
);
CREATE INDEX idx_conversation_send_unresolved
    ON conversation_send_attempts(conversation_id) WHERE state IN ('reserved', 'legacy');

-- Old queued/unbound rows cannot be distinguished from possibly delivered
-- sends. Do not infer pending from their lifecycle state or an empty reference.
INSERT INTO conversation_send_attempts (turn_id, conversation_id, state)
SELECT id, conversation_id, 'legacy' FROM conversation_turns
WHERE state = 'queued' AND provider_turn_id = '';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE conversation_send_attempts;
-- +goose StatementEnd
