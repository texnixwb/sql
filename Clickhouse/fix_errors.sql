    --вместо
EXCHANGE TABLES buffer.open_registries AND datamart.open_registries; --(не поддерживается на некторых OS)
--написать:
RENAME TABLE buffer.open_registries TO buffer.open_registries_tmp
    , datamart.open_registries TO datamart.open_registries_old
    , buffer.open_registries_tmp TO datamart.open_registries;
drop table datamart.open_registries_old;
