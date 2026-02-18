-- event-4: program-1, VEN_NAME target for ven-1-name only
-- program-1 enrolls both ven-1 and ven-2 (via vens-programs fixture),
-- so only ven-1 should see this event (event-level filtering).
INSERT INTO event (id, created_date_time, modification_date_time, program_id, event_name, priority, targets,
                   report_descriptors, payload_descriptors, interval_period, intervals)
VALUES ('event-4',
        '2024-07-25 08:31:10.776000 +00:00',
        '2024-07-25 08:31:10.776000 +00:00',
        'program-1',
        'event-4-targeted',
        null,
        '[
          {
            "type": "VEN_NAME",
            "values": [
              "ven-1-name"
            ]
          }
        ]'::jsonb,
        null,
        null,
        null,
        '[
          {
            "id": 1,
            "payloads": [
              {
                "type": "PRICE",
                "values": [
                  0.25
                ]
              }
            ]
          }
        ]'::jsonb);
