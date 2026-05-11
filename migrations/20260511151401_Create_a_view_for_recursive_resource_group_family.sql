-- Add migration script here
CREATE VIEW rg_family AS
WITH RECURSIVE _rg_family(root, id) AS NOT MATERIALIZED (
    SELECT id, id FROM resource_group
    UNION
    SELECT fam.root, child.rg_child_rg_id
    FROM rg_child_rg AS child
    JOIN _rg_family AS fam ON fam.id = child.rg_parent_rg_id
)
SELECT root, id FROM _rg_family;
