ALTER TABLE program DROP COLUMN targets;
ALTER TABLE program ADD COLUMN targets text[] NOT NULL DEFAULT '{}';

ALTER TABLE event DROP COLUMN targets;
ALTER TABLE event ADD COLUMN targets text[] NOT NULL DEFAULT '{}';

ALTER TABLE ven DROP COLUMN targets;
ALTER TABLE ven ADD COLUMN targets text[] NOT NULL DEFAULT '{}';

ALTER TABLE resource DROP COLUMN targets;
ALTER TABLE resource ADD COLUMN targets text[] NOT NULL DEFAULT '{}';

DROP TABLE ven_program;