#!/usr/bin/env python3
#
# Monitoring a large number of databases with active tables

import subprocess as sp
from __utils__ import *

def run(db_prefix, num_dbs, num_tables, num_rows_per_table):
    # +20 to make room for internal background processes and debugging connections
    sp.run(['gpconfig', '-c', 'max_connections', '-v', f'{num_dbs + 20}'])
    sp.run(['gpconfig', '-c', 'max_worker_processes', '-v', f'{num_dbs + 20}'])
    sp.run(['gpstop', '-far'])
    for i in range(num_dbs):
        db_clear(f'{db_prefix}_{i}')
    for i in range(num_dbs):
        db_exec(f'db_{i}', f'''"
            CREATE TABLE t1 (pk int, val int)
            DISTRIBUTED BY (pk)
            PARTITION BY RANGE (pk) (START (1) END ({num_tables}) INCLUSIVE EVERY (1));

            INSERT INTO t1 
            SELECT pk, val
            FROM generate_series(1, {num_rows_per_table}) AS val, generate_series(1, {num_tables}) AS pk;
        "''')
