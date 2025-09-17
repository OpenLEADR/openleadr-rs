INSERT INTO ven (id,
                 created_date_time,
                 modification_date_time,
                 ven_name,
                 attributes,
                 targets)
VALUES ('ven-1',
        '2024-07-25 08:31:10.776000 +00:00',
        '2024-07-25 08:31:10.776000 +00:00',
        'ven-1-name',
        NULL,
        ARRAY['group-1', 'private-value']),
       ('ven-2',
        '2024-07-25 08:31:10.776000 +00:00',
        '2024-07-25 08:31:10.776000 +00:00',
        'ven-2-name',
        NULL,
        NULL);

INSERT INTO user_ven (ven_id, user_id)
VALUES ('ven-1', 'user-1');
