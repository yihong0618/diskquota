-- Test role quota
-- start_ignore
\! mkdir -p /tmp/rolespc
-- end_ignore
DROP TABLESPACE  IF EXISTS rolespc;
CREATE TABLESPACE rolespc LOCATION '/tmp/rolespc';
CREATE SCHEMA rolespcrole;
SET search_path TO rolespcrole;

DROP ROLE IF EXISTS rolespcu1;
DROP ROLE IF EXISTS rolespcu2;
CREATE ROLE rolespcu1 NOLOGIN;
CREATE ROLE rolespcu2 NOLOGIN;
CREATE TABLE b (t TEXT) TABLESPACE rolespc DISTRIBUTED BY (t);
CREATE TABLE b2 (t TEXT) TABLESPACE rolespc DISTRIBUTED BY (t);
ALTER TABLE b2 OWNER TO rolespcu1;

INSERT INTO b SELECT generate_series(1,100);
-- expect insert success
INSERT INTO b SELECT generate_series(1,100000);
SELECT diskquota.set_role_tablespace_quota('rolespcu1', 'rolespc', '1 MB');
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert success
INSERT INTO b SELECT generate_series(1,100);
ALTER TABLE b OWNER TO rolespcu1;
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert fail
INSERT INTO b SELECT generate_series(1,100);
-- expect insert fail
INSERT INTO b2 SELECT generate_series(1,100);

-- Test show_fast_role_tablespace_quota_view
SELECT role_name, tablespace_name, quota_in_mb, rolsize_tablespace_in_bytes FROM diskquota.show_fast_role_tablespace_quota_view WHERE role_name = 'rolespcu1' and tablespace_name = 'rolespc';

-- Test alter owner
ALTER TABLE b OWNER TO rolespcu2;
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert succeed
INSERT INTO b SELECT generate_series(1,100);
-- expect insert succeed
INSERT INTO b2 SELECT generate_series(1,100);
ALTER TABLE b OWNER TO rolespcu1;
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert fail
INSERT INTO b SELECT generate_series(1,100);

-- Test alter tablespace
-- start_ignore
\! mkdir -p /tmp/rolespc2
-- end_ignore
DROP TABLESPACE  IF EXISTS rolespc2;
CREATE TABLESPACE rolespc2 LOCATION '/tmp/rolespc2';
ALTER TABLE b SET TABLESPACE rolespc2;
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert succeed
INSERT INTO b SELECT generate_series(1,100);
-- alter table b back to tablespace rolespc
ALTER TABLE b SET TABLESPACE rolespc;
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert fail
INSERT INTO b SELECT generate_series(1,100);

-- Test update quota config
SELECT diskquota.set_role_tablespace_quota('rolespcu1', 'rolespc', '10 MB');
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert success
INSERT INTO b SELECT generate_series(1,100);
-- expect insert success
INSERT INTO b SELECT generate_series(1,1000000);
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert fail
INSERT INTO b SELECT generate_series(1,100);

-- Test delete quota config
SELECT diskquota.set_role_tablespace_quota('rolespcu1', 'rolespc', '-1 MB');
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert success
INSERT INTO b SELECT generate_series(1,100);

-- superuser is blocked to set quota
-- start_ignore
SELECT rolname from pg_roles where rolsuper=true;
-- end_ignore
\gset
select diskquota.set_role_tablespace_quota(:'rolname', 'rolespc', '1mb');

-- start_ignore
\! mkdir -p /tmp/rolespc3
-- end_ignore
DROP ROLE IF EXISTS "Rolespcu3";
CREATE ROLE "Rolespcu3" NOLOGIN;
DROP TABLESPACE  IF EXISTS "Rolespc3";
CREATE TABLESPACE "Rolespc3" LOCATION '/tmp/rolespc3';
SELECT diskquota.set_role_tablespace_quota('rolespcu1', '"Rolespc3"', '-1 MB');
SELECT diskquota.set_role_tablespace_quota('"Rolespcu3"', 'rolespc', '-1 mB');
SELECT diskquota.set_role_tablespace_quota('"Rolespcu3"', '"Rolespc3"', '-1 Mb');

DROP TABLE b, b2;
DROP ROLE rolespcu1, rolespcu2;
RESET search_path;
DROP SCHEMA rolespcrole;
DROP TABLESPACE rolespc;
DROP TABLESPACE rolespc2;
DROP TABLESPACE "Rolespc3";
