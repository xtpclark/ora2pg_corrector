-- PM Sample Schema - Drop script
-- Run as PM user to remove all schema objects

-- Drop tables (in correct order for FK constraints)
DROP TABLE product_catalog PURGE;
DROP TABLE media_files PURGE;
DROP TABLE print_media PURGE;
DROP TABLE product_information PURGE;

-- Drop types (in correct order for dependencies)
DROP TYPE textdoc_tab;
DROP TYPE textdoc_typ;
DROP TYPE adheader_typ;

-- Verify cleanup
SELECT object_name, object_type
FROM user_objects
WHERE object_type IN ('TABLE', 'TYPE', 'INDEX', 'LOB')
ORDER BY object_type, object_name;
