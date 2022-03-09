import subprocess as sp

def db_exec(db, command):
    sp.run(['time', 'psql', db, '-c', command])

def db_clear(db):
    db_exec(db, f'''"
        DROP EXTENSION IF EXISTS diskquota;
    "''')
    sp.run(['dropdb', '--if-exists', db])
    sp.run(['createdb', db])
    db_exec(db, '''"
        CREATE EXTENSION diskquota;
        SELECT diskquota.wait_for_worker_new_epoch();
    "''')