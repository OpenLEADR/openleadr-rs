
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

INSERT INTO user_ven VALUES ('ven-1', 'ven_user');