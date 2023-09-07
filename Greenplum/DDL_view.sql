-- проверить зависимость вьюхи.
SELECT DISTINCT srcobj.oid AS src_oid
  , srcnsp.nspname AS src_schemaname
  , srcobj.relname AS src_objectname
  , tgtobj.oid AS dependent_viewoid
  , tgtnsp.nspname AS dependant_schemaname
  , tgtobj.relname AS dependant_objectname
FROM pg_class srcobj
  JOIN pg_depend srcdep ON srcobj.oid = srcdep.refobjid
  JOIN pg_depend tgtdep ON srcdep.objid = tgtdep.objid
  JOIN pg_class tgtobj ON tgtdep.refobjid = tgtobj.oid
  LEFT JOIN pg_namespace srcnsp ON srcobj.relnamespace = srcnsp.oid
  LEFT JOIN pg_namespace tgtnsp ON tgtobj.relnamespace = tgtnsp.oid
WHERE tgtdep.deptype = 'i'::"char" AND tgtobj.relkind = 'v'::"char"
and srcobj.relname = 'shk_state'


--поменять тип колумна
ALTER TABLE tbl_name ALTER COLUMN col_name TYPE varchar (11);
