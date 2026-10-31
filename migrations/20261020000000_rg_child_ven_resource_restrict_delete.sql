-- Block deleting a resource while it is still a member of a resource group.
-- 'no action' instead of 'restrict' so the violation is reported as a foreign key violation (23503)
alter table rg_child_ven_resource
    drop constraint rg_child_ven_resource_rg_child_ven_resource_id_fkey,
    add constraint rg_child_ven_resource_rg_child_ven_resource_id_fkey
        foreign key (rg_child_ven_resource_id) references resource (id) on delete no action;
