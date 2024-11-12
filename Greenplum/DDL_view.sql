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

--все вьюхи основанные на схеме
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
  LEFT JOIN pg_namespace shobj ON srcobj.relnamespace = shobj.oid
  LEFT JOIN pg_namespace srcnsp ON srcobj.relnamespace = srcnsp.oid
  LEFT JOIN pg_namespace tgtnsp ON tgtobj.relnamespace = tgtnsp.oid
WHERE tgtdep.deptype = 'i'::"char" AND tgtobj.relkind = 'v'::"char"
and shobj.nspname = 'stage_ut';
  

--поменять тип колумна
alter table stage_external.logistics_shipping_tsd alter column seal2 type int USING seal2::integer;

-- Получить список дропнутых столбцов ()

select
    ut.relname,
    attnum,
    *
from 
    pg_attribute pga
    inner join pg_catalog.pg_statio_user_tables ut on pga.attrelid = ut.relid
where
    -- attrelid = 'tz_world'::regclass
    atttypid = 0
    and attnum > 0
order by 
    attnum;

-- восстановить столбец №2 с типом `geometry` и именем `geometry`
UPDATE pg_attribute
SET attname = 'geometry',
    atttypid = 'geometry'::regtype,
    attstattarget = -1,
    attisdropped = FALSE
WHERE attrelid = 'tz_world'::regclass
AND attnum = 2;
