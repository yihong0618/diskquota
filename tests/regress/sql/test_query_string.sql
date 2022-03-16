\! gpconfig -c "log_statement" -v "all" > /dev/null
\! gpconfig -c "log_min_messages" -v "DEBUG5" > /dev/null
\! gpstop -u > /dev/null
\! gpconfig --show log_statement
\! gpconfig --show log_min_messages

SELECT pg_logfile_rotate();
SELECT pg_logfile_rotate() FROM gp_dist_random('gp_id');

DROP DATABASE IF EXISTS query_string_db;
CREATE DATABASE query_string_db;
\c query_string_db

CREATE EXTENSION diskquota;

CREATE SCHEMA s1;
SET SEARCH_PATH TO s1;

CREATE TABLE a(i int) DISTRIBUTED BY (i);
INSERT INTO a SELECT generate_series(1,100);
-- expect insert success
INSERT INTO a SELECT generate_series(1,100000);

SELECT diskquota.set_schema_quota('s1', '1 MB');
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert fail
INSERT INTO a SELECT generate_series(1,100);

SELECT diskquota.wait_for_worker_new_epoch();
DROP TABLE IF EXISTS a;
SELECT diskquota.wait_for_worker_new_epoch();

RESET SEARCH_PATH;
DROP SCHEMA s1 CASCADE;

SELECT DISTINCT ON (diskquota_related) REGEXP_REPLACE(logmessage, '.*(diskquota\.[a-z_]+).*', '\1') AS diskquota_related
FROM gp_toolkit.gp_log_database
WHERE logmessage LIKE '%diskquota.%'
  AND logmessage NOT LIKE '%gp_toolkit%'
  AND logtime >= NOW() - INTERVAL '60 min';

SELECT DISTINCT ON (diskquota_related) logmessage AS diskquota_related
FROM gp_toolkit.gp_log_system
WHERE logmessage LIKE '%diskquota%'
  AND logmessage LIKE '%pg_extension%'
  AND logmessage NOT LIKE '%gp_toolkit%'
  AND logtime >= NOW() - INTERVAL '60 min';

DROP EXTENSION diskquota;

\c contrib_regression
DROP DATABASE query_string_db;

-- reset GUC to default
\! gpconfig -c log_statement -m 'all' -v 'none' > /dev/null
\! gpconfig -c log_min_messages -v 'WARNING' > /dev/null
\! gpstop -u > /dev/null
\! gpconfig --show log_statement
\! gpconfig --show log_min_messages
