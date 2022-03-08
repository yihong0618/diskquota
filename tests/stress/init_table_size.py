#!/usr/bin/env python3
#
# Init diskquota.table_size when the number of tables is very large

from subprocess import run, PIPE, STDOUT

def db_exec(db, command):
    run(['time', 'psql', db, '-c', command])

def create_extension(db):
    db_exec(db, f'''"
        CREATE EXTENSION diskquota;
    "''')

def create_tables(db, num_tables, num_rows_per_table):
    db_exec(db, f'''"
        CREATE TABLE t1 (pk int, val int)
        DISTRIBUTED BY (pk)
        PARTITION BY RANGE (pk) (START (1) END ({num_tables}) INCLUSIVE EVERY (1));
    "''')

def init_table_size(db):
    db_exec(db, f'''"
        SELECT diskquota.init_table_size_table();
    "''')