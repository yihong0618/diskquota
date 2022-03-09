#!/usr/bin/env python3
#
# Update quota usage when the number of possible quota definition is large

import subprocess as sp
from __utils__ import *

def run(db, num_tables, num_tablespaces):
    db_clear(db)
    for i in range(num_tablespaces):
        sp.run(['mkdir', '-p', f'/tmp/dir_{i}'])
        db_exec(db, f"CREATE TABLESPACE tablespace_{i} LOCATION '/tmp/dir_{i}';")
    for i in range(num_tables):
        db_exec(db, f'''"
            CREATE SCHMEA schema_for_table_{i};
            CREATE ROLE role_for_table_{i};

            SET ROLE role_for_table_{i};
            CREATE TABLE schema_for_table_{i}.table_{i} (i int) DISTRIBUTED BY (i)
            TABLESPACE tablespace_{i % num_tablespaces};
        "''')
    db_exec(db, '"SELECT diskquota.wait_for_worker_new_epoch();"')

