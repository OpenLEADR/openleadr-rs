-- no-transaction
CREATE INDEX CONCURRENTLY IF NOT EXISTS rg_child_ven_resource_child_idx ON rg_child_ven_resource (rg_child_ven_resource_id);
