INSERT INTO business (id)
VALUES ('business-1');

INSERT INTO user_business (user_id, business_id)
VALUES ('user-1', 'business-1');

UPDATE program
SET business_id = 'business-1'
WHERE id = 'program-3';