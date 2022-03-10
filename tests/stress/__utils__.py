import subprocess as sp

def db_exec(db, command):
    sp.run(['time', 'psql', db], input=command, universal_newlines=True)

def db_clear(db):
    db_exec(db, 'DROP EXTENSION IF EXISTS diskquota;')
    sp.run(['dropdb', '--if-exists', db])
    sp.run(['createdb', db])
    db_exec(db, '''
        CREATE EXTENSION diskquota;
        SELECT diskquota.wait_for_worker_new_epoch();
    ''')