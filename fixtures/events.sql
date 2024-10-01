INSERT INTO event (id, created_date_time, modification_date_time, program_id, event_name, priority, targets,
                   report_descriptors, payload_descriptors, interval_period, intervals)
VALUES ('event-1',
        '2024-07-25 08:31:10.776000 +00:00',
        '2024-07-25 08:31:10.776000 +00:00',
        'program-1',
        'event-1-name',
        '4',
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
        ]'::jsonb,
        null,
        null,
        '{
          "start": "2023-06-15T09:30:00+00:00",
          "duration": "P0Y0M0DT1H0M0S",
          "randomizeStart": "P0Y0M0DT1H0M0S"
        }'::jsonb,
        '[
          {
            "id": 3,
            "payloads": [
              {
                "type": "PRICE",
                "values": [
                  0.17
                ]
              }
            ],
            "intervalPeriod": {
              "start": "2023-06-15T09:30:00+00:00",
              "duration": "P0Y0M0DT1H0M0S",
              "randomizeStart": "P0Y0M0DT1H0M0S"
            }
          }
        ]'::jsonb),
       ('event-2',
        '2024-07-25 08:31:10.776000 +00:00',
        '2024-07-25 08:31:10.776000 +00:00',
        'program-2',
        'event-2-name',
        null,
        '[
          {
            "type": "SOME_TARGET",
            "values": [
              "target-1"
            ]
          }
        ]'::jsonb,
        null,
        null,
        null,
        '[
          {
            "id": 3,
            "payloads": [
              {
                "type": "SOME_PAYLOAD",
                "values": [
                  "value"
                ]
              }
            ]
          }
        ]'::jsonb),
       ('event-3',
        '2024-07-25 08:31:10.776000 +00:00',
        '2024-07-25 08:31:10.776000 +00:00',
        'program-3',
        'event-3-name',
        null,
        '[
          {
            "type": "SOME_TARGET",
            "values": [
              "target-1"
            ]
          }
        ]'::jsonb,
        null,
        null,
        null,
        '[
          {
            "id": 3,
            "payloads": [
              {
                "type": "SOME_PAYLOAD",
                "values": [
                  "value"
                ]
              }
            ]
          }
        ]'::jsonb);