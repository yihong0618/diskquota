import subprocess as sp
import os

def gp_run(command):
    env = os.environ.copy()
    if 'PYTHONPATH' not in env or env['PYTHONPATH'] == '':
        env['PYTHONPATH'] = os.path.join(env['GPHOME'], 'lib', 'python')
    sp.run(command, universal_newlines=True, env=env)

def db_exec(db, command):
    sp.run(['time', 'psql', db], input=command, universal_newlines=True)

def db_clean(db):
    db_exec(db, 'DROP EXTENSION IF EXISTS diskquota;')
    sp.run(['dropdb', '--if-exists', db])
    sp.run(['createdb', db])
    db_exec(db, '''
        CREATE EXTENSION diskquota;
        SELECT diskquota.wait_for_worker_new_epoch();
    ''')
