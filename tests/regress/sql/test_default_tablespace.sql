-- test role_tablespace_quota works with tables/databases in default tablespace
-- test role_tablespace_quota works with tables/databases in non-default tablespace with hard limits on

-- start_ignore
\! mkdir -p /tmp/custom_tablespace
-- end_ignore

SELECT diskquota.enable_hardlimit();

DROP TABLE if EXISTS t;
CREATE TABLE t (i int);
SELECT diskquota.set_role_tablespace_quota(current_role, 'pg_default', '1 MB');
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert to fail
INSERT INTO t SELECT generate_series(1, 1000000);
DROP TABLE IF EXISTS t;

-- database in customized tablespace with hard limits on
CREATE TABLESPACE custom_tablespace LOCATION '/tmp/custom_tablespace';
CREATE DATABASE db_with_tablespace TABLESPACE custom_tablespace;
\c db_with_tablespace;
CREATE EXTENSION diskquota;
SELECT diskquota.enable_hardlimit();

SELECT diskquota.set_role_tablespace_quota(current_role, 'custom_tablespace', '1 MB');
SELECT diskquota.wait_for_worker_new_epoch();

-- start_ignore
SELECT * from diskquota.blackmap;
-- end_ignore

-- expect create table to fail
CREATE TABLE t_in_custom_tablespace AS SELECT generate_series(1, 1000000);

-- clean up
DROP TABLE IF EXISTS t_in_custom_tablespace;
SELECT diskquota.disable_hardlimit();
DROP EXTENSION IF EXISTS diskquota;
\c contrib_regression;
DROP DATABASE IF EXISTS db_with_tablespace;
DROP TABLESPACE IF EXISTS custom_tablespace;
\! rm -rf /tmp/custom_tablespace

SELECT diskquota.disable_hardlimit();
