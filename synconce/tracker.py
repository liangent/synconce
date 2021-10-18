import os
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


def execute_walk(context):
    config = context.config

    for root, dirs, files in os.walk(config['local']):
        for filename in files:
            if fnmatch.fnmatch(filename, config['exclude']):
                logger.info(
                    f'Skipping {os.path.join(root, filename)}'
                    f': matching exclusion {config["exclude"]}'
                )
                continue

            maybe_sync(context, Path(root), filename)


def execute(config):
    logger.info(f'Starting sync for {dict(config)}')

    with create_context(config) as context:
        init_db(context.db, context.cursor)

        execute_walk(context)
