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
                   CREATE INDEX IF NOT EXISTS synchronized_pathname
                   ON synchronized(pathname)
                   ''')


def maybe_sync(db, cursor, ssh, sftp, root, filename, local_base):
    logger.info('Checking %s', os.path.join(root, filename))
    absolute_pathname = os.path.join(root, filename)
    pathname = os.path.relpath(absolute_pathname, local_base)
    size = os.path.getsize(absolute_pathname)

    cursor.execute('SELECT size FROM synchronized WHERE pathname = ?',
                   (pathname,))
    synchronized_size = cursor.fetchone()
    to_sync = synchronized_size is None or synchronized_size[0] != size
    to_insert = synchronized_size is None
    logger.debug('%s: size=%s, syncd_size=%s, sync=%s, insert=%s',
                 pathname, size, synchronized_size, to_sync, to_insert)

    if to_sync:
        path = os.path.relpath(root, local_base)
        path = '' if path == '.' else path
        result = do_sync(ssh, sftp, absolute_pathname, size, path, filename)

        if result:
            logger.info('Synchronization of %s complete, size %s',
                        pathname, size)
            if to_insert:
                cursor.execute('''
                               INSERT INTO synchronized(pathname, size)
                               VALUES (?, ?)
                               ''', (pathname, size))
            else:
                cursor.execute('''
                               UPDATE synchronized
                               SET size = ?, datetime = CURRENT_TIMESTAMP
                               WHERE pathname = ?
                               ''', (size, pathname))
            db.commit()

def execute(config):
    logger.info('Starting sync for %s', dict(config))
    db = sqlite3.connect(config['data'])
    cursor = db.cursor()
    init_db(db, cursor)

    key = paramiko.RSAKey.from_private_key_file(config['rsa_key'])
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())
    ssh.connect(config['host'], config.getint('port'),
                username=config['user'], pkey=key)
    sftp = ssh.open_sftp()
    sftp.chdir(config['remote'])

    local_base = config['local']
    for root, dirs, files in os.walk(local_base):
        for filename in files:
            if fnmatch.fnmatch(filename, config['exclude']):
                logger.info('Skipping %s: matching %s',
                            os.path.join(root, filename), config['exclude'])
                continue

            maybe_sync(db, cursor, ssh, sftp, root, filename, local_base)

    sftp.close()
    ssh.close()
    db.close()
