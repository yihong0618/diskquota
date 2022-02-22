-- Test pause and resume.
CREATE SCHEMA s1;
SET search_path TO s1;

CREATE TABLE a(i int) DISTRIBUTED BY (i);

-- expect insert succeed
INSERT INTO a SELECT generate_series(1,100000);

SELECT diskquota.set_schema_quota('s1', '1 MB');
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert fail
INSERT INTO a SELECT generate_series(1,100);

-- pause extension
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();

SELECT tableid::regclass, size, segid FROM diskquota.table_size 
WHERE tableid = 'a'::regclass AND segid = -1;

-- expect insert succeed
INSERT INTO a SELECT generate_series(1,100000);

-- resume extension
SELECT diskquota.resume();
SELECT diskquota.wait_for_worker_new_epoch();

-- expect insert fail
INSERT INTO a SELECT generate_series(1,100);

-- table size should be updated after resume
SELECT tableid::regclass, size, segid FROM diskquota.table_size 
WHERE tableid = 'a'::regclass AND segid = -1;

-- enable hardlimit
\! gpconfig -c "diskquota.hard_limit" -v "on" > /dev/null
\! gpstop -u
SELECT diskquota.wait_for_worker_new_epoch();

-- blackmaps should be dispatched to segments
SELECT count(*) FROM (select diskquota.show_blackmap() from gp_dist_random('gp_id')) as blackmap_on_segs;

-- expect insert fail
INSERT INTO a SELECT generate_series(1,100);

-- pause extension
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();

-- pause should clear blackmap on segments
SELECT count(*) FROM (select diskquota.show_blackmap() from gp_dist_random('gp_id')) as blackmap_on_segs;
-- expect insert succeed
INSERT INTO a SELECT generate_series(1,100);

-- resume extension
SELECT diskquota.pause();
SELECT diskquota.wait_for_worker_new_epoch();

-- blackmaps should be dispatched to segments again
SELECT count(*) FROM (select diskquota.show_blackmap() from gp_dist_random('gp_id')) as blackmap_on_segs;

-- expect insert fail
INSERT INTO a SELECT generate_series(1,100);


-- disable hardlimit
\! gpconfig -c "diskquota.hard_limit" -v "off" > /dev/null
\! gpstop -u

RESET search_path;
DROP TABLE s1.a;
DROP SCHEMA s1;
