INSERT INTO program (id,
                     created_date_time,
                     modification_date_time,
                     program_name,
                     program_long_name,
                     retailer_name,
                     retailer_long_name,
                     program_type,
                     country,
                     principal_subdivision,
                     interval_period,
                     program_descriptions,
                     binding_events,
                     local_price,
                     payload_descriptors,
                     targets)
VALUES ('program-1',
        '2024-07-25 08:31:10.776000 +00:00',
        '2024-07-25 08:31:10.776000 +00:00',
        'program-1',
        'program long name',
        'retailer name',
        'retailer long name',
        'program type',
        'country',
        'principal-subdivision',
        '{
          "start": "2024-07-25T08:31:10.776Z"
        }',
        '[
          {
            "URL": "https://program-description-1.com"
          }
        ]',
        false,
        true,
        '[
          {
            "objectType": "EVENT_PAYLOAD_DESCRIPTOR",
            "payloadType": "EXPORT_PRICE"
          }
        ]',
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
       ('program-2',
        '2024-07-25 08:31:10.776000 +00:00',
        '2024-07-25 08:31:10.776000 +00:00',
        'program-2',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        '[
          {
            "type": "GROUP",
            "values": [
              "group-1",
              "group-2"
            ]
          }
        ]'),
       ('program-3',
        '2024-07-25 08:31:10.776000 +00:00',
        '2024-07-25 08:31:10.776000 +00:00',
        'program-3',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL);