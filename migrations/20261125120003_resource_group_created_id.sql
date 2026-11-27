-- no-transaction
CREATE INDEX CONCURRENTLY IF NOT EXISTS resource_group_created_id_idx ON resource_group (created_date_time, id);
