#!/usr/bin/env python3
#
# Init diskquota.table_size when the number of tables is very large

from __utils__ import *

def run(db, num_tables, num_rows_per_table):
    db_clear(db)
    db_exec(db, f'''"
        CREATE TABLE t1 (pk int, val int)
        DISTRIBUTED BY (pk)
        PARTITION BY RANGE (pk) (START (1) END ({num_tables}) INCLUSIVE EVERY (1));

        INSERT INTO t1 
        SELECT pk, val
        FROM generate_series(1, {num_rows_per_table}) AS val, generate_series(1, {num_tables}) AS pk;
    "''')
    db_exec(db, '"SELECT diskquota.init_table_size_table();"')
