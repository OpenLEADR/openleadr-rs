-- Add migration script here
create table resource_group
(
    id                     text        not null
        constraint resource_group_pk
            primary key,
    created_date_time      timestamptz not null,
    modification_date_time timestamptz not null,
    resource_group_name    text        not null,
    attributes             jsonb,
    targets                text[] not null default '{}'
);

create table rg_child_ven_resource
(
    rg_parent_rg_id text not null references resource_group (id) on delete cascade,
    rg_child_ven_resource_id text not null references resource (id) on delete cascade
);

create table rg_child_rg
(
    rg_parent_rg_id text not null references resource_group (id) on delete cascade,
    rg_child_rg_id text not null references resource_group (id) on delete cascade
);
