INSERT INTO "user" (id, reference, description, created, modified)
VALUES ('admin', 'admin-ref', null, '2024-07-25 08:31:10.776000 +00:00', '2024-07-25 08:31:10.776000 +00:00');

INSERT INTO any_business_user (user_id)
VALUES ('admin');
INSERT INTO user_manager (user_id)
VALUES ('admin');
INSERT INTO ven_manager (user_id)
VALUES ('admin');

INSERT INTO user_credentials (user_id, client_id, client_secret)
VALUES ('admin', 'admin', '$argon2id$v=19$m=16,t=2,p=1$QmtwZnBPVnlIYkJTWUtHZg$lMxF0N+CeRa99UmzMaUKeg'); -- secret: admin

INSERT INTO "user" (id, reference, description, created, modified)
VALUES ('user-1', 'user-1-ref', 'desc', '2024-07-25 08:31:10.776000 +00:00', '2024-07-25 08:31:10.776000 +00:00');

INSERT INTO user_credentials (user_id, client_id, client_secret)
VALUES ('user-1', 'user-1-client-id',
        '$argon2id$v=19$m=16,t=2,p=1$R04zbWxDNVhtVHB4aVJLag$mRpShTDhgZ9+bVNLa8GBgw'); -- secret: user-1

