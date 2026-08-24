-- The distinction for write_reports_bl and write_reports_ven is necessary to allow for
-- BLs to delete all reports: https://github.com/OpenLEADR/openleadr-rs/issues/478
-- Mirrors creation of the subscriptions scope in migrations/20260506070840_subscriptions_client_id.sql

-- Left over from the write_subscriptions split (20260506070840_subscriptions_client_id.sql),
-- which renamed the old `scope` type to `_scope` but never dropped it.
DROP TYPE _scope;

-- The same holds for removing values from enum types, with the added problem that we need to migrate data here properly
ALTER TYPE scope RENAME TO _scope;
CREATE TYPE scope AS ENUM (
    'read_all',
    'read_targets',
    'read_ven_objects',
    'write_programs',
    'write_events',
    'write_reports_bl',
    'write_reports_ven',
    'write_subscriptions_bl',
    'write_subscriptions_ven',
    'write_vens_bl',
    'write_vens_ven',
    'write_users'
    );
ALTER TABLE "user" RENAME COLUMN scopes to _scopes;
ALTER TABLE "user"
    ADD COLUMN scopes scope[] NOT NULL DEFAULT '{}';
UPDATE "user" SET scopes = array(
    SELECT (CASE WHEN scope = 'write_reports' THEN 'write_reports_ven' ELSE scope::TEXT END)::scope
    FROM (SELECT unnest(_scopes) AS scope)
);
ALTER TABLE "user" DROP COLUMN _scopes;
