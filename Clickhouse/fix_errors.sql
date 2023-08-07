    --вместо
EXCHANGE TABLES buffer.open_registries AND datamart.open_registries; --(не поддерживается на некторых OS)
--написать:
RENAME TABLE buffer.open_registries TO buffer.open_registries_tmp 
, datamart.open_registries TO buffer.open_registries 
, buffer.open_registries_tmp TO datamart.open_registries;
