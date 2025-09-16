create table business
(
    id text not null
        constraint business_pk primary key
);

create table program
(
    id                     text        not null
        constraint program_pk
            primary key,
    created_date_time      timestamptz not null,
    modification_date_time timestamptz not null,

    program_name           text        not null,
    program_long_name      text,
    retailer_name          text,
    retailer_long_name     text,
    program_type           text,
    country                text,
    principal_subdivision  text,
    -- deliberately omitted: time_zone_offset
    interval_period        jsonb,
    program_descriptions   jsonb,
    binding_events         boolean,
    local_price            boolean,
    payload_descriptors    jsonb,
    targets                jsonb,
    business_id            text references business (id)
);

create unique index program_program_name_uindex
    on program (program_name);

create table event
(
    id                     text        not null
        constraint event_pk
            primary key,
    created_date_time      timestamptz not null,
    modification_date_time timestamptz not null,

    program_id             text        not null references program (id),
    event_name             text,
    priority               bigint,
    report_descriptors     jsonb,
    payload_descriptors    jsonb,
    interval_period        jsonb,
    intervals              jsonb       not null,
    targets                jsonb
);

create index event_event_name_index
    on event (event_name);


create table report
(
    id                     text        not null
        constraint report_pk
            primary key,
    created_date_time      timestamptz not null,
    modification_date_time timestamptz not null,

    program_id             text        not null references program (id),
    event_id               text        not null references event (id),
    client_name            text        not null,
    report_name            text,
    payload_descriptors    jsonb,
    resources              jsonb       not null
);

create unique index report_report_name_uindex
    on report (report_name);

create table "user"
(
    id          text primary key,
    reference   text        not null,
    description text,
    created     timestamptz not null,
    modified    timestamptz not null
);

create table user_credentials
(
    user_id       text not null references "user" (id) on delete cascade,
    client_id     text primary key,
    client_secret text not null
    -- TODO maybe the credentials require their own role?
);

create table ven
(
    id                     text        not null
        constraint ven_pk
            primary key,
    created_date_time      timestamptz not null,
    modification_date_time timestamptz not null,
    ven_name               text        not null,
    attributes             jsonb,
    targets                jsonb
);

create unique index ven_ven_name_uindex
    on ven (ven_name);

create table user_ven
(
    ven_id  text not null references ven (id) on delete cascade,
    user_id text not null references "user" (id) on delete cascade
);

create table resource
(
    id                     text        not null
        constraint resource_pk
            primary key,
    created_date_time      timestamptz not null,
    modification_date_time timestamptz not null,
    resource_name          text        not null,
    ven_id                 text        not null references ven (id), -- TODO is this actually 'NOT NULL'?
    attributes             jsonb,
    targets                jsonb

);

create unique index resource_ven_id_resource_name_uindex
    on resource (ven_id, resource_name);


create table user_business
(
    user_id     text not null references "user" (id) on delete cascade,
    business_id text not null references business (id) on delete cascade
);

create unique index uindex_user_business
    on user_business (user_id, business_id);

create table ven_manager
(
    user_id text primary key references "user" (id) on delete cascade
);

create table user_manager
(
    user_id text primary key references "user" (id) on delete cascade
);

create table any_business_user
(
    user_id text primary key references "user" (id) on delete cascade
);
