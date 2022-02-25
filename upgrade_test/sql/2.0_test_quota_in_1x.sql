-- need run 1.0_set_quota before run this test
-- FIXME add version check here

insert into rolespcrole.b select generate_series(1,100000); -- fail.
insert into spcs1.a select generate_series(1,100000); -- fail.
insert into srole.b select generate_series(1,100000); -- fail.
insert into s1.a select generate_series(1, 10000000); -- fail.

drop role u1, rolespcu1;
drop table s1.a, srole.b spcs1.a, rolespcrole.b;
drop schema s1, srole, spcs1, rolespcrole;
drop tablespace schemaspc, rolespc;
