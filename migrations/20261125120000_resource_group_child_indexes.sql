-- Children of a group. Without these, rg_family and the children of each group are found by
-- scanning the whole table.
CREATE INDEX rg_child_ven_resource_parent_idx ON rg_child_ven_resource (rg_parent_rg_id);
CREATE INDEX rg_child_rg_parent_idx ON rg_child_rg (rg_parent_rg_id);
