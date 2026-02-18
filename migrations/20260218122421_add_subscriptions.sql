create table subscription
(
    id                     text        not null
        constraint subscription_pk
            primary key,
    created_date_time      timestamptz not null,
    modification_date_time timestamptz not null,
    client_id              text        not null references user_credentials (client_id),
    client_name            text        not null,
    program_id             text,
    object_operations      jsonb
);
