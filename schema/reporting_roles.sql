-- Run as a PostgreSQL administrator after creating the dedicated cms_tableau database.
-- Set passwords out of band; never add credentials to this file or the repository.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cms_reporting_loader') THEN
        CREATE ROLE cms_reporting_loader LOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'tableau_reader') THEN
        CREATE ROLE tableau_reader LOGIN;
    END IF;
END
$$;

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA control, reporting, source_detail FROM PUBLIC;

GRANT USAGE ON SCHEMA reporting, source_detail, control TO tableau_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA reporting, source_detail TO tableau_reader;
GRANT SELECT ON control.active_reporting_snapshot,
    control.model_catalog,
    control.column_lineage
TO tableau_reader;

ALTER DEFAULT PRIVILEGES FOR ROLE cms_reporting_loader IN SCHEMA reporting
    GRANT SELECT ON TABLES TO tableau_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE cms_reporting_loader IN SCHEMA source_detail
    GRANT SELECT ON TABLES TO tableau_reader;

ALTER ROLE tableau_reader SET default_transaction_read_only = on;
