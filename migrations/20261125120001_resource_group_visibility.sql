-- The resource groups each client may see: those with a resource of the client's VEN somewhere
-- below them. Filter on `client_id` to get one client's groups.
CREATE VIEW rg_visible AS
SELECT DISTINCT v.client_id, fam.root AS rg_id
FROM rg_family AS fam
INNER JOIN rg_child_ven_resource AS rcvr ON rcvr.rg_parent_rg_id = fam.id
INNER JOIN resource AS r ON r.id = rcvr.rg_child_ven_resource_id
INNER JOIN ven AS v ON v.id = r.ven_id;
