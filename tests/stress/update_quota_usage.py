#!/usr/bin/env python3
#
# Update quota usage when the number of possible quota definition is large

from subprocess import run, PIPE, STDOUT

def db_exec(db, command):
    run(['time', 'psql', db, '-c', command])

def create_extension(db):
    db_exec(db, f'''"
        CREATE EXTENSION diskquota;
    "''')

def create_tables(db, num_tables):
    for i in range(num_tables):
        run(['mkdir', '-p', '/tmp/dir_for_table_{i}'])
        db_exec(db, f'''"
            CREATE SCHMEA schema_for_table_{i};
            CREATE ROLE role_for_table_{i};
            CREATE TABLESPACE tablespace_for_table_{i} LOCATION '/tmp/dir_for_table{i}';

            SET ROLE role_for_table_{i};
            CREATE TABLE schema_for_table_{i}.table_{i} (i int) DISTRIBUTED BY (i)
            TABLESPACE tablespace_for_table_{i};
        "''')

def wait_for_new_epoch(db):
    db_exec(db, '"SELECT diskquota.wait_for_worker_new_epoch();"')
