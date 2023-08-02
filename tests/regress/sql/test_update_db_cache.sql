--start_ignore
CREATE DATABASE test_db_cache;
--end_ignore

\c test_db_cache
CREATE EXTENSION diskquota;
CREATE EXTENSION diskquota_test;

-- Wait until the db cache gets updated 
SELECT diskquota.wait_for_worker_new_epoch();

CREATE TABLE t(i) AS SELECT generate_series(1, 100000)
DISTRIBUTED BY (i);

SELECT diskquota.wait_for_worker_new_epoch();

SELECT tableid::regclass, size, segid
FROM diskquota.table_size
WHERE tableid = 't'::regclass
ORDER BY segid;

DROP EXTENSION diskquota;

-- Create table without extension
CREATE TABLE t_no_extension(i) AS SELECT generate_series(1, 100000)
DISTRIBUTED BY (i);

CREATE EXTENSION diskquota;

SELECT diskquota_test.wait('SELECT diskquota_test.check_cur_db_status(''UNREADY'');');

-- Should find nothing since t_no_extension is not recorded.
SELECT diskquota.diskquota_fetch_table_stat(0, ARRAY[]::oid[])
FROM gp_dist_random('gp_id');

DROP TABLE t;
DROP TABLE t_no_extension;
SELECT diskquota.pause();

DROP EXTENSION diskquota;

\c contrib_regression
DROP DATABASE test_db_cache;
