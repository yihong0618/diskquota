\! gpconfig -c shared_preload_libraries -v 'diskquota-2.0.so'
\! gpstop -raf > /dev/null

\! gpconfig -s 'shared_preload_libraries'

alter extension diskquota update to '2.0';
-- FIXME add version check here
