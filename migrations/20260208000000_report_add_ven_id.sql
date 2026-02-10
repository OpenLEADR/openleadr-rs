-- Add ven_id to report so VENs can only see their own reports
ALTER TABLE report ADD COLUMN ven_id text REFERENCES ven(id);

-- Backfill: match existing reports to VENs by client_name = ven_name
UPDATE report r SET ven_id = v.id FROM ven v WHERE v.ven_name = r.client_name;
