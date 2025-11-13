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

ALTER TABLE resource
    DROP COLUMN targets,
    ADD COLUMN targets   text[] NOT NULL DEFAULT '{}',
    ADD COLUMN client_id text   NOT NULL;

DROP TABLE ven_program;

ALTER TABLE report
    DROP COLUMN program_id,
    ADD COLUMN client_id text NOT NULL;
