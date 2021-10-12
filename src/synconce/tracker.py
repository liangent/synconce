import os
import fnmatch

import sqlite3
import paramiko

from .sync import do_sync

import logging
logger = logging.getLogger('synconce.tracker')


def init_db(db, cursor):
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS synchronized(
                        pathname TEXT,
                        size INTEGER,
                        datetime DATETIME DEFAULT CURRENT_TIMESTAMP
                   )
                   ''')
    cursor.execute('''
                   CREATE UNIQUE INDEX IF NOT EXISTS synchronized_pathname
                   ON synchronized(pathname)
                   ''')

def get_size(db, cursor, pathname):
    cursor.execute('SELECT size FROM synchronized WHERE pathname = ?',
                   (pathname,))
    size = cursor.fetchone()
    return size[0] if size else None


def set_size(db, cursor, pathname, size):
    cursor.execute('REPLACE INTO synchronized(pathname, size) VALUES (?, ?)',
                   (pathname, size))
    db.commit()


def maybe_sync(db, cursor, ssh, sftp, root, filename, local_base, min_free):
    logger.info(f'Checking {os.path.join(root, filename)}')
    full_pathname = os.path.join(root, filename)
    pathname = os.path.relpath(full_pathname, local_base)
    size = os.path.getsize(full_pathname)

    synchronized_size = get_size(db, cursor, pathname)
    logger.debug(f'{pathname}: size={size}, syncd_size={synchronized_size}')

    if size != synchronized_size:
        path = os.path.relpath(root, local_base)
        path = '' if path == '.' else path

        if do_sync(ssh, sftp, full_pathname, size, path, filename, min_free):
            logger.info(f'Synchronization of {pathname} complete, size {size}')
            set_size(db, cursor, pathname, size)

def execute(config):
    logger.info(f'Starting sync for {dict(config)}')

    db = sqlite3.connect(config['data'])
    try:
        cursor = db.cursor()
        init_db(db, cursor)

        key = paramiko.RSAKey.from_private_key_file(config['rsa_key'])
        ssh = paramiko.SSHClient()
        try:
            ssh.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())
            ssh.connect(config['host'], config.getint('port'),
                        username=config['user'], pkey=key)
            sftp = ssh.open_sftp()
            try:
                sftp.chdir(config['remote'])

                local_base = config['local']
                for root, dirs, files in os.walk(local_base):
                    for filename in files:
                        if fnmatch.fnmatch(filename, config['exclude']):
                            logger.info(
                                f'Skipping {os.path.join(root, filename)}'
                                f': matching exclusion {config["exclude"]}'
                            )
                            continue

                        maybe_sync(db, cursor, ssh, sftp,
                                   root, filename, local_base,
                                   config.getint('min_free'))
            finally:
                sftp.close()
        finally:
            ssh.close()
    finally:
        db.close()
