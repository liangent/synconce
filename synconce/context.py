import contextlib

import sqlite3
import paramiko

from .remote import Remote
from .sync import do_sync

import logging
logger = logging.getLogger('synconce.context')


class Context(object):
    config = None
    db = None
    cursor = None
    ssh = None
    sftp = None
    do_sync = staticmethod(do_sync)


@contextlib.contextmanager
def create_context(config):
    context = Context()
    context.config = config

    with contextlib.closing(sqlite3.connect(config['data'])) as context.db:
        context.cursor = context.db.cursor()

        key = paramiko.RSAKey.from_private_key_file(config['rsa_key'])
        with paramiko.SSHClient() as context.ssh:
            context.ssh.set_missing_host_key_policy(
                paramiko.client.AutoAddPolicy())
            context.ssh.connect(config['host'], config.getint('port'),
                                username=config['user'], pkey=key)

            with context.ssh.open_sftp() as context.sftp:
                context.sftp.chdir(config['remote'])
                context.remote = Remote(context.ssh, context.sftp.getcwd())

                yield context
