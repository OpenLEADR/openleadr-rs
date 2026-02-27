ALTER TABLE program
    DROP COLUMN targets,
    ADD COLUMN targets    text[] NOT NULL DEFAULT '{}',
    DROP COLUMN program_long_name,
    DROP COLUMN retailer_name,
    DROP COLUMN retailer_long_name,
    DROP COLUMN program_type,
    DROP COLUMN country,
    DROP COLUMN principal_subdivision,
    DROP COLUMN binding_events,
    DROP COLUMN local_price,
    DROP COLUMN business_id,
    ADD COLUMN attributes jsonb;

ALTER TABLE event
    DROP COLUMN targets,
    ADD COLUMN targets  text[] NOT NULL DEFAULT '{}',
    -- ISO8601 formated string.
    -- Unfortunately, using the `interval` type turns out impractical because
    -- the exact meaning of the duration is dependent on the start date when
    -- using month and/or years (28 - 31 days per month / leap year).
    -- Therefore, we cannot interpret it into a properly defined PgInterval
    ADD COLUMN duration text;

ALTER TABLE ven
    DROP COLUMN targets,
    ADD COLUMN targets   text[] NOT NULL DEFAULT '{}',
    ADD COLUMN client_id text   NOT NULL;

-- See https://github.com/oadr3-org/specification/discussions/372
CREATE UNIQUE INDEX ven_client_id_unique ON ven (client_id);

ALTER TABLE resource
    DROP COLUMN targets,
    ADD COLUMN targets text[] NOT NULL DEFAULT '{}';

DROP TABLE ven_program;

ALTER TABLE report
    DROP COLUMN program_id,
    ADD COLUMN client_id text NOT NULL,
    DROP COLUMN ven_id;


DROP TABLE any_business_user;
DROP TABLE user_ven;
DROP TABLE user_manager;
DROP TABLE user_business;
DROP TABLE business;
DROP TABLE ven_manager;

CREATE TYPE scope AS ENUM (
    'read_all',
    'read_targets',
    'read_ven_objects',
    'write_programs',
    'write_events',
    'write_reports',
    'write_subscriptions',
    'write_vens',
    'write_users'
    );

ALTER TABLE "user"
    ADD COLUMN scopes scope[] NOT NULL DEFAULT '{}';
