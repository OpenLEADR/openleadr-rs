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
        '[
          {
            "type": "GROUP",
            "values": [
              "group-1"
            ]
          },
          {
            "type": "PRIVATE_LABEL",
            "values": [
              "private value"
            ]
          }
        ]'),
       ('ven-2',
        '2024-07-25 08:31:10.776000 +00:00',
        '2024-07-25 08:31:10.776000 +00:00',
        'ven-2-name',
        NULL,
        NULL);
