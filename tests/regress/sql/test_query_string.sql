\! gpconfig -c "log_statement" -v "all" > /dev/null

SELECT pg_logfile_rotate();
SELECT pg_logfile_rotate() FROM gp_dist_random('gp_id');

\! gpstop -u > /dev/null

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

\! ./data/find_latest_log.sh
