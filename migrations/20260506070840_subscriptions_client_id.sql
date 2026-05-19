-- Unfortunately, table recreation is the only way to remove foreign key constraints.
CREATE TABLE temp AS SELECT * FROM subscription;
DROP TABLE subscription;
CREATE TABLE subscription
(
    id                     TEXT        NOT NULL
        constraint subscription_pk
            primary key,
    created_date_time      TIMESTAMPTZ NOT NULL,
    modification_date_time TIMESTAMPTZ NOT NULL,
    client_id              TEXT        NOT NULL,
    client_name            TEXT        NOT NULL,
    program_id             TEXT,
    object_operations      jsonb
);
INSERT INTO subscription SELECT * FROM temp;
DROP TABLE temp;

-- The same holds for removing values from enum types, with the added problem that we need to migrate data here properly
ALTER TYPE scope RENAME TO _scope;
CREATE TYPE scope AS ENUM (
    'read_all',
    'read_targets',
    'read_ven_objects',
    'write_programs',
    'write_events',
    'write_reports',
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
    SELECT (CASE WHEN scope = 'write_subscriptions' THEN 'write_subscriptions_ven' ELSE scope::TEXT END)::scope 
    FROM (SELECT unnest(_scopes) AS scope)
);
ALTER TABLE "user" DROP COLUMN _scopes;
