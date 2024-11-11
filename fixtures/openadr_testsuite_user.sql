INSERT INTO "user" (id, reference, description, created, modified)
VALUES ('bl_user',
        'bl_test_user',
        null,
        now(),
        now());

-- secret: 1001
INSERT INTO user_credentials (user_id, client_id, client_secret)
VALUES ('bl_user', 'bl_client',
        '$argon2id$v=19$m=16,t=2,p=1$YmJkMTJrU0ptMVprYVJLSQ$mu1Fbbt5PzBsE/dJevKazw');

INSERT INTO any_business_user (user_id) VALUES ('bl_user');
INSERT INTO ven_manager (user_id) VALUES ('bl_user');

INSERT INTO "user" (id, reference, description, created, modified)
VALUES ('ven_user',
        'ven_test_user',
        null,
        now(),
        now());

-- secret: 999
INSERT INTO user_credentials (user_id, client_id, client_secret)
VALUES ('ven_user', 'ven_client',
        '$argon2id$v=19$m=16,t=2,p=1$RGhDTmVkbEl5cEZDY0Fubg$qPtSCpK6Z5XKQkOLHC/+qg');

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
        NULL);

INSERT INTO user_ven VALUES ('ven-1', 'ven_user');
INSERT INTO ven_manager (user_id) VALUES ('ven_user');
