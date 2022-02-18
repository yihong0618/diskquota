-- test role_tablespace_quota works with tables/databases in default tablespace
-- test role_tablespace_quota works with tables/databases in non-default tablespace with hard limits on

-- start_ignore
\! mkdir -p /tmp/custom_tablespace
-- end_ignore

DROP ROLE if EXISTS role1;
CREATE ROLE role1 SUPERUSER;
SET ROLE role1;

DROP TABLE if EXISTS t;
CREATE TABLE t (i int);

-- with hard limits off
SELECT diskquota.disable_hardlimit();
SELECT diskquota.set_role_tablespace_quota('role1', 'pg_default', '1 MB');
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert to success
INSERT INTO t SELECT generate_series(1, 100);
INSERT INTO t SELECT generate_series(1, 1000000);
SELECT diskquota.wait_for_worker_new_epoch();
select r.rolname, t.spcname, b.target_type from diskquota.blackmap as b, pg_tablespace as t, pg_roles as r where b.tablespace_oid = t.oid and b.target_oid = r.oid;
-- expect insert to fail
INSERT INTO t SELECT generate_series(1, 1000000);

DROP TABLE IF EXISTS t;
CREATE TABLE t (i int);

-- with hard limits on
SELECT diskquota.enable_hardlimit();
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert to fail
INSERT INTO t SELECT generate_series(1, 1000000);
DROP TABLE IF EXISTS t;

-- database in customized tablespace
CREATE TABLESPACE custom_tablespace LOCATION '/tmp/custom_tablespace';
CREATE DATABASE db_with_tablespace TABLESPACE custom_tablespace;
\c db_with_tablespace;
SET ROLE role1;
CREATE EXTENSION diskquota;

-- with hard limits off
SELECT diskquota.disable_hardlimit();
SELECT diskquota.set_role_tablespace_quota('role1', 'custom_tablespace', '1 MB');
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert to success
CREATE TABLE t_in_custom_tablespace AS SELECT generate_series(1, 100);
INSERT INTO t_in_custom_tablespace SELECT generate_series(1, 1000000);
SELECT diskquota.wait_for_worker_new_epoch();
select r.rolname, t.spcname, b.target_type from diskquota.blackmap as b, pg_tablespace as t, pg_roles as r where b.tablespace_oid = t.oid and b.target_oid = r.oid;
-- expect insert to fail
INSERT INTO t_in_custom_tablespace SELECT generate_series(1, 1000000);

-- with hard limits on
SELECT diskquota.enable_hardlimit();
SELECT diskquota.wait_for_worker_new_epoch();

DROP TABLE IF EXISTS t_in_custom_tablespace;
-- expect create table to fail
CREATE TABLE t_in_custom_tablespace AS SELECT generate_series(1, 1000000);

-- clean up
DROP TABLE IF EXISTS t_in_custom_tablespace;
SELECT diskquota.disable_hardlimit();
DROP EXTENSION IF EXISTS diskquota;
\c contrib_regression;
DROP DATABASE IF EXISTS db_with_tablespace;
DROP TABLESPACE IF EXISTS custom_tablespace;

RESET ROLE;
DROP ROLE IF EXISTS role1;

SELECT diskquota.disable_hardlimit();
