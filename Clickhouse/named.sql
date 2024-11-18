

-- коллекция кафки
CREATE NAMED COLLECTION alljobswb_wbc_kafka AS
 kafka.security_protocol = 'SASL_PLAINTEXT',
 kafka.sasl_mechanism = 'SCRAM-SHA-256',
 kafka.sasl_password = 'pass',
 kafka.sasl_username = 'username',
 kafka_broker_list = 'first-broker.ru:9092,second-broker.ru:9092'

--создать юзера для справочников с доступом из локалки и локалхоста
create user dictator_ch3 on cluster dict_3 IDENTIFIED WITH sha256_password BY 'мукнышскуезыы' HOST IP '10.0.0.0/8','127.0.0.1' ;

-- на самом клике
drop NAMED COLLECTION if exists  dictator_ch3 on cluster distributed_cluster_3;
CREATE NAMED COLLECTION dictator_ch3 on cluster distributed_cluster_3 AS
  host = 'localhost',
  port = 9000,
  user = 'dictator_ch3',
  password = 'dictator_ch3_пароль';

--на других кликах
drop NAMED COLLECTION if exists  dictator_ch3 on cluster distributed_cluster_3;
CREATE NAMED COLLECTION dictator_ch3 on cluster distributed_cluster_3 AS
  host = 'хост-источник.ru',
  port = 9000,
  user = 'dictator_ch3',
  password = 'dictator_ch3_пароль';

-- справочник при помощи немед колекшена
DROP DICTIONARY IF EXISTS dict.mega_shk_sources on cluster dict_3;
CREATE DICTIONARY dict.mega_shk_sources on cluster dict_3
(
    id UInt64,
    name String,
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(NAME dictator_ch3 DB 'dict_history'
                  QUERY 'select id, name from dict_history.mega_shk_sources final'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(FLAT())
COMMENT '**' ;
