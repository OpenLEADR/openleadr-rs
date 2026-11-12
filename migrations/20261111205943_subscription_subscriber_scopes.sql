-- Scopes saved from owner's token. Used for webhook filter delivery.
-- Empty means no delivery
ALTER TABLE "subscription"
    ADD COLUMN subscriber_scopes scope[] NOT NULL DEFAULT '{}';