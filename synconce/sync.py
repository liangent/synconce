import os
import stat
import shlex
import hashlib

from . import utils

import logging
logger = logging.getLogger('synconce.sync')


def confirm_dir(context, path):
    try:
        attr = context.sftp.stat(path)
        logger.debug(f'"{path}": {repr(attr)}')
        return stat.S_ISDIR(attr.st_mode)
    except FileNotFoundError:
        logger.debug(f'"{path}" does not exist')

    # path determined non-existent:

    # check parent of path
    parent = os.path.dirname(path)
    if parent == path:
        # at remote_base
        logger.warn(f'Remote base {context.sftp.getcwd()} does not exist')
        return False
    if not confirm_dir(context, parent):
        return False

    # create path
    logger.info(f'Creating remote directory {path}')
    context.sftp.mkdir(path)
    return True


def maybe_partial(context, src, src_size, dest, dest_size):
    logger.info(f'Attempting partial transferring {dest}'
                f' ({dest_size:,} bytes) from {src} ({src_size:,} bytes)')
    if dest_size > src_size:
        # dest larger than src
        logger.error(f'{dest} ({dest_size:,} bytes) is larger'
                     f' than local file {src} ({src_size:,} bytes)')
        return False

    remote_sha1sum, cancel_remote_sha1sum = utils.remote_hashsum(
        context.ssh, context.sftp.getcwd(), dest, 'sha1')

    with open(src, 'rb') as srcf:
        src_sha1 = utils.head_sha1(srcf, dest_size)
        logger.debug(f'Local head ({dest_size:,} bytes) SHA-1: {src_sha1}')

        if src_sha1 is None:
            logger.error(f'Local file {src} could not be read to {dest_size}')
            cancel_remote_sha1sum()
            return False

        dest_sha1 = remote_sha1sum()
        logger.debug(f'Remote SHA-1: {dest_sha1}')

        if src_sha1 != dest_sha1:
            # head of src != dest
            logger.error(f'Head of local {src} does not match remote {dest}')
            return False

        logger.info(f'Remote file matches head of local file. Transferring...')
        with context.sftp.open(dest, 'ab') as destf:
            destf.set_pipelined(True)
            transferred = utils.append_transfer(srcf, destf)
        logger.info(f'{transferred:,} bytes transferred.')

    # at this point, the remote file should be completely written
    attr = context.sftp.stat(dest)
    logger.info(f'Remote file {dest} after sync: {repr(attr)}')
    if attr.st_size == src_size:
        return True
    else:
        logger.warn(f'Incomplete transferred {dest} ({attr.st_size:,} bytes)'
                    f' from {src} ({src_size:,} bytes)')
        return False


def full_transfer(context, src, src_size, dest):
    with open(src, 'rb') as f:
        try:
            attr = context.sftp.putfo(f, dest, src_size)
        except IOError:
            # incomplete upload? but don't retry or resume here
            logger.warn(f'Failed/incomplete file {dest} from {src}')
            return False

        # further check? or already checked by sftp.putfo()
        logger.info(f'File transferred, attr={repr(attr)}')
        return True


def do_sync(context, fileloc, size, path, filename):
    min_free = context.config.getint('min_free')
    dest = os.path.join(path, filename)
    logger.info(f'Synchronizing {fileloc} ({size:,} bytes) to {dest}')

    get_space_free, cancel_space_free = utils.remote_space_free(
        context.ssh, context.sftp.getcwd(), path)

    if not confirm_dir(context, path):
        logger.error(f'Cannot make remote directory {path}')
        cancel_space_free()
        return False

    try:
        attr = context.sftp.stat(dest)
    except FileNotFoundError:
        space_free = get_space_free()
        logger.info(f'Remote file {dest} does not exist'
                    f', {space_free:,} bytes available at "{path}"'
                    f', sending {fileloc}')

        if space_free - size < min_free:
            logger.error(f'Space available ({space_free:,} bytes)'
                         f' is not enough to store {dest}'
                         f': min_free {min_free:,} bytes'
                         f', size {size:,} bytes'
                         f', space after transfer {space_free - size:,} bytes')
            return False

        return full_transfer(context, fileloc, size, dest)

    # file already there
    space_free = get_space_free()
    logger.info(f'Remote file {dest} exists: {repr(attr)}'
                f'; {space_free:,} bytes available at "{path}"')

    if not stat.S_ISREG(attr.st_mode):
        return False

    if space_free + attr.st_size - size < min_free:
        logger.error(f'Space available ({space_free:,} bytes)'
                     f' is not enough to send partial {dest}'
                     f': existing size {attr.st_size:,} bytes'
                     f', min_free {min_free:,} bytes'
                     f', size {size:,} bytes, space after transfer'
                     f' {space_free + attr.st_size- size:,} bytes')
        return False

    return maybe_partial(context, fileloc, size, dest, attr.st_size)
