-- create users
INSERT INTO "user" (id, reference, description, created, modified)
VALUES ('ven-manager',
        'ven-manager',
        'for automated test cases',
        now(),
        now());

INSERT INTO "user" (id, reference, description, created, modified)
VALUES ('user-manager',
        'user-manager',
        'for automated test cases',
        now(),
        now());

INSERT INTO "user" (id, reference, description, created, modified)
VALUES ('any-business',
        'any-business',
        'for automated test cases',
        now(),
        now());

INSERT INTO "user" (id, reference, description, created, modified)
VALUES ('business-1-user',
        'business-1-user',
        'for automated test cases',
        now(),
        now());

INSERT INTO "user" (id, reference, description, created, modified)
VALUES ('ven-1-user',
        'ven-1-user',
        'for automated test cases',
        now(),
        now());

-- associate roles to users
INSERT INTO ven_manager (user_id)
VALUES ('ven-manager');
INSERT INTO user_manager (user_id)
VALUES ('user-manager');
INSERT INTO any_business_user (user_id)
VALUES ('any-business');

INSERT INTO business (id)
VALUES ('business-1');
INSERT INTO user_business (user_id, business_id)
VALUES ('business-1-user', 'business-1');

INSERT INTO ven (id, created_date_time, modification_date_time, ven_name, attributes, targets)
VALUES ('ven-1',
        now(),
        now(),
        'ven-1-name',
        null,
        ARRAY[]::text[]);

INSERT INTO user_ven (ven_id, user_id)
VALUES ('ven-1', 'ven-1-user');

-- create login credentials
INSERT INTO user_credentials (user_id, client_id, client_secret)
VALUES ('ven-manager',
        'ven-manager',
           --ven-manager
        '$argon2id$v=19$m=16,t=2,p=1$NGxoR0w0MG1oQVhTNTlWYw$NaIKUON6vrNcM2jXzqwX5Q');

INSERT INTO user_credentials (user_id, client_id, client_secret)
VALUES ('user-manager',
        'user-manager',
           --user-manager
        '$argon2id$v=19$m=16,t=2,p=1$eDRuSHFPUG16M09JNGo5WQ$LjaFZJVr2Qpna45k51yuhw');

INSERT INTO user_credentials (user_id, client_id, client_secret)
VALUES ('any-business',
        'any-business',
           --any-business
        '$argon2id$v=19$m=16,t=2,p=1$WWJENTVTbEpYZkFjdlhOUQ$UXEQI8OPlnbtXwStitu6Vw');

INSERT INTO user_credentials (user_id, client_id, client_secret)
VALUES ('business-1-user',
        'business-1',
           --business-1
        '$argon2id$v=19$m=16,t=2,p=1$bVlaZTI4QndaaERNa1Q5bg$0IjdRSjo601S9vHICOLRdQ');

INSERT INTO user_credentials (user_id, client_id, client_secret)
VALUES ('ven-1-user',
        'ven-1',
           --ven-1
        '$argon2id$v=19$m=16,t=2,p=1$WGpkRFZGampQM0N3S0ZZVw$lO3AhXZYKu/Wsk3Z1NE/Aw');
