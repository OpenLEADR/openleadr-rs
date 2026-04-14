INSERT INTO resource_group (
  id,
  created_date_time,
  modification_date_time,
  resource_group_name,
  attributes,
  targets
) VALUES (
  'resource-group-1',
  '2026-04-14 11:15:00.000000 +00:00',
  '2026-04-14 11:15:00.000000 +00:00',
  'resource-group-1-name',
  NULL,
  ARRAY ['group-1', 'somewhere-in-the-nowhere']),
(
  'resource-group-2',
  '2026-04-14 11:15:00.000000 +00:00',
  '2026-04-14 11:15:00.000000 +00:00',
  'resource-group-2-name',
  NULL,
  ARRAY ['group-1']),
(
  'resource-group-3',
  '2026-04-14 11:15:00.000000 +00:00',
  '2026-04-14 11:15:00.000000 +00:00',
  'resource-group-3-name',
  NULL,
  ARRAY ['group-2']),
(
  'resource-group-4',
  '2026-04-14 11:15:00.000000 +00:00',
  '2026-04-14 11:15:00.000000 +00:00',
  'resource-group-4-name',
  NULL,
  ARRAY []::text[]),
(
  'ouroboros',
  '2026-04-14 11:15:00.000000 +00:00',
  '2026-04-14 11:15:00.000000 +00:00',
  'circular-reference',
  NULL,
  ARRAY []::text[]);

INSERT INTO rg_child_rg (
  rg_parent_rg_id,
  rg_child_rg_id
) VALUES (
  'resource-group-1',
  'resource-group-2',
), (
  'resource-group-1',
  'resource-group-3',
), (
  'resource-group-2',
  'resource-group-4',
), (
  'ouroboros',
  'ouroboros',
);

INSERT INTO rg_child_ven_resource (
  rg_parent_rg_id,
  rg_child_ven_resource_id,
) VALUES (
  'resource-group-2',
  'resource-1',
), (
  'resource-group-3',
  'resource-1',
), (
  'resource-group-3',
  'resource-2',
), (
  'resource-group-3',
  'resource-3',
), (
  'resource-group-3',
  'resource-4',
);
