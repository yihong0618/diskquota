-- need run 1.0_set_quota before run this test
-- FIXME add version check here

insert into s1.a select generate_series(1, 100); -- fail
insert into srole.b select generate_series(1, 100); -- fail

drop role u1;
drop table s1.a, srole.b;
drop schema s1, srole;
