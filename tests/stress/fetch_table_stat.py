#!/usr/bin/env python3
#
# Writing data to a large number of tables

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

        INSERT INTO t1 
        SELECT pk, val
        FROM generate_series(1, {num_rows_per_table}) AS val, generate_series(1, {num_tables}) AS pk;
    "''')