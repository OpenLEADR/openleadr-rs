INSERT INTO ven (id,
                 created_date_time,
                 modification_date_time,
                 ven_name,
                 attributes,
                 targets,
                 client_id)
VALUES ('ven-1',
        '2024-07-25 08:31:10.776000 +00:00',
        '2024-07-25 08:31:10.776000 +00:00',
        'ven-1-name',
        NULL,
        ARRAY ['group-1', 'private-value'],
        'ven-1-client-id'),
       ('ven-2',
        '2024-07-25 08:31:10.776000 +00:00',
        '2024-07-25 08:31:10.776000 +00:00',
        'ven-2-name',
        NULL,
        ARRAY []::text[],
        'ven-2-client-id');

-- Fixme this should not be needed with object privacy
INSERT INTO user_ven (ven_id, user_id)
VALUES ('ven-1', 'user-1');
