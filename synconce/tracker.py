import os
import fcntl
import fnmatch
from pathlib import Path

from .context import create_context

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


def get_size(context, pathname):
    context.cursor.execute(
        'SELECT size FROM synchronized WHERE pathname = ?', (str(pathname),))
    size = context.cursor.fetchone()
    return size[0] if size else None


def set_size(context, pathname, size):
    context.cursor.execute(
        'REPLACE INTO synchronized(pathname, size) VALUES (?, ?)',
        (str(pathname), size))
    context.db.commit()


def maybe_sync(context, root, filename):
    logger.info(f'Checking {root}//{filename}')
    local_base = context.config['local']
    full_pathname = root / filename
    pathname = full_pathname.relative_to(local_base)
    size = full_pathname.stat().st_size

    synchronized_size = get_size(context, pathname)
    logger.debug(f'{pathname}: size={size}, syncd_size={synchronized_size}')

    if size != synchronized_size:
        path = root.relative_to(local_base)

        if context.config.get('flatten') is not None:
            filename = str(path / filename)
            path = Path()
            filename = filename.replace(os.path.sep, context.config['flatten'])

        if context.do_sync(context, full_pathname, size, path, filename):
            logger.info(f'Synchronization of {pathname} complete, size {size}')
            set_size(context, pathname, size)

            return True

    return False


def execute_walk(context):
    config = context.config

    synced = False

    for root, dirs, files in os.walk(config['local']):
        for filename in files:
            if fnmatch.fnmatch(filename, config['exclude']):
                logger.info(f'Skipping {root}//{filename}'
                            f': matching exclusion {config["exclude"]}')
                continue

            if root == config['local'] and filename == config['lock_file']:
                logger.info(f'Skipping {root}//{filename}: is lock_file')
                continue

            this_synced = maybe_sync(context, Path(root), filename)
            synced = synced or this_synced  # short-circuit calculation

    return synced


def execute(config, do_sync=None, exec_command=None):
    logger.info(f'Starting sync for {dict(config)}')

    if config['lock_file']:
        local = Path(config['local'])
        try:
            fd = os.open(local / config['lock_file'], os.O_WRONLY | os.O_CREAT)
            fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            logger.error('Another synconce in progress')
            return
    else:
        fd = -1

    try:
        with create_context(config) as context:
            if do_sync:
                context.do_sync = do_sync

            if exec_command:
                context.remote.exec_command = exec_command

            init_db(context.db, context.cursor)
            synced = execute_walk(context)

            if synced and config['post_sync']:
                logger.info(f'Running post_sync: {config["post_sync"]}')
                out, err = context.remote.exec_command(config['post_sync'])
                logger.debug(f'post_sync out={repr(out)}, err={repr(err)}')
    finally:
        if fd != -1:
            fcntl.lockf(fd, fcntl.LOCK_UN)
            os.close(fd)
