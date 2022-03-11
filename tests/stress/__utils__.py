import subprocess as sp
import os
from typing import List, Dict

def gp_run(command: List[str]):
    env = os.environ.copy()
    if 'PYTHONPATH' not in env or env['PYTHONPATH'] == '':
        env['PYTHONPATH'] = os.path.join(env['GPHOME'], 'lib', 'python')
    sp.run(command, universal_newlines=True, env=env)

def db_exec(db: str, command: List[str]):
    gp_run(['time', 'psql', db], input=command, universal_newlines=True)

def db_clean(db: str):
    db_exec(db, 'DROP EXTENSION IF EXISTS diskquota;')
    gp_run(['dropdb', '--if-exists', db])
    gp_run(['createdb', db])

def db_enable_diskquota(db: str, guc: Dict[str, str]={}):
    gp_run(['gpconfig', '-c', 'shared_preload_libraries', '-v', 'diskquota'])
    gp_run(['gpstop', '-far'])
    for var in guc:
        gp_run('gpconfig', '-c', var, '-v', guc[var])
    gp_run(['gpstop', '-far'])
    db_exec(db, '''
        CREATE EXTENSION diskquota;
        SELECT diskquota.wait_for_worker_new_epoch();
    ''')
