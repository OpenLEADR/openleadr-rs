INSERT INTO "user" (id, reference, description, scopes, created, modified)
VALUES ('bl-client', 'bl-client-ref', null, '{"read_all", "write_vens", "write_programs", "write_events", "write_users"}', '2024-07-25 08:31:10.776000 +00:00', '2024-07-25 08:31:10.776000 +00:00');

INSERT INTO user_credentials (user_id, client_id, client_secret)
VALUES ('bl-client', 'bl-client', '$argon2id$v=19$m=16,t=2,p=1$MWt1QVNFdHdlZVJhNEZzUA$Rmkguwgaz+A2GWIaDRtv8w'); -- secret: bl-client

INSERT INTO "user" (id, reference, description, scopes, created, modified)
VALUES ('ven-client', 'ven-client-ref', 'desc', '{"read_targets", "read_ven_objects", "write_reports", "write_subscriptions"}', '2024-07-25 08:31:10.776000 +00:00', '2024-07-25 08:31:10.776000 +00:00');

INSERT INTO user_credentials (user_id, client_id, client_secret)
VALUES ('ven-client', 'ven-client-client-id',
        '$argon2id$v=19$m=16,t=2,p=1$YWlOSE8xRGFVdVVIa212Ug$tjmQC+zNC3QXc9K8mEXRrA'); -- secret: ven-client

