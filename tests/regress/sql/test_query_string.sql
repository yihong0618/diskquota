\! gpconfig -c "log_statement" -v "all" > /dev/null
\! gpstop -u > /dev/null
\! gpconfig --show log_statement

SELECT pg_logfile_rotate();
SELECT pg_logfile_rotate() FROM gp_dist_random('gp_id');

DROP DATABASE IF EXISTS query_string_db13;
CREATE DATABASE query_string_db13;
\c query_string_db13

CREATE EXTENSION diskquota;

CREATE SCHEMA s1;
SET search_path TO s1;

CREATE TABLE a(i int) DISTRIBUTED BY (i);
INSERT INTO a SELECT generate_series(1,100);
-- expect insert success
INSERT INTO a SELECT generate_series(1,100000);

SELECT diskquota.set_schema_quota('s1', '1 MB');
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert fail
INSERT INTO a SELECT generate_series(1,100);

SELECT pg_sleep(1);

SELECT diskquota.wait_for_worker_new_epoch();
DROP TABLE IF EXISTS a;
SELECT diskquota.wait_for_worker_new_epoch();

RESET SEARCH_PATH;
DROP SCHEMA s1 CASCADE;

SET SEARCH_PATH TO gp_toolkit;

SELECT DISTINCT on (x) regexp_replace(logmessage, '(-)?\d+', '', 'g') as x
FROM gp_toolkit.gp_log_database
WHERE logmessage LIKE 'statement: %';

RESET SEARCH_PATH;

DROP EXTENSION diskquota;

\c contrib_regression
DROP DATABASE query_string_db13;

\! gpconfig -c log_statement -m 'all' -v 'none' > /dev/null
\! gpstop -u > /dev/null
\! gpconfig --show log_statement
