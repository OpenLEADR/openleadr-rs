-- no-transaction
-- Reports by event; also used by the foreign key check on event delete.
-- CONCURRENTLY, so writes to the table continue while the index is built. It can't run
-- inside a transaction, hence one statement per migration. If the build fails it leaves an
-- INVALID index behind and the next start fails on "already exists": run
-- `DROP INDEX CONCURRENTLY report_event_id_idx;` and restart.
CREATE INDEX CONCURRENTLY report_event_id_idx ON report (event_id);
