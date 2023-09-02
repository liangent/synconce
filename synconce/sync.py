import stat

from . import utils

import logging
logger = logging.getLogger('synconce.sync')


def confirm_dir(context, path):
    try:
        attr = context.sftp.stat(str(path))
        logger.debug(f'"{path}": {repr(attr)}')
        return stat.S_ISDIR(attr.st_mode)
    except FileNotFoundError:
        logger.debug(f'"{path}" does not exist')

    # path determined non-existent:

    # check parent of path
    parent = path.parent
    if parent == path:
        # at remote_base
        logger.warn(f'Remote base {context.sftp.getcwd()} does not exist')
        return False
    if not confirm_dir(context, parent):
        return False

    # create path
    logger.info(f'Creating remote directory {path}')
    context.sftp.mkdir(str(path))
    return True


def maybe_partial(context, src, src_size, dest, dest_size):
    logger.info(f'Attempting partial transferring {dest}'
                f' ({dest_size:,} bytes) from {src} ({src_size:,} bytes)')
    if dest_size > src_size:
        # dest larger than src
        logger.error(f'{dest} ({dest_size:,} bytes) is larger'
                     f' than local file {src} ({src_size:,} bytes)')
        return False

    remote_sha1sum = context.remote.hashsum(str(dest), 'sha1')

    with open(src, 'rb') as srcf:
        src_sha1 = utils.head_sha1(srcf, dest_size)
        logger.debug(f'Local head ({dest_size:,} bytes) SHA-1: {src_sha1}')

        if src_sha1 is None:
            logger.error(f'Local file {src} could not be read to {dest_size}')
            return False

        dest_sha1 = remote_sha1sum()
        logger.debug(f'Remote SHA-1: {dest_sha1}')

        if src_sha1 != dest_sha1:
            # head of src != dest
            logger.error(f'Head of local {src} does not match remote {dest}')
            return False

        logger.info('Remote file matches head of local file. Transferring...')
        with context.sftp.open(str(dest), 'ab') as destf:
            destf.set_pipelined(True)
            transferred = utils.append_transfer(srcf, destf)
        logger.info(f'{transferred:,} bytes transferred.')

    # at this point, the remote file should be completely written
    attr = context.sftp.stat(str(dest))
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
            attr = context.sftp.putfo(f, str(dest), src_size)
        except IOError:
            # incomplete upload? but don't retry or resume here
            logger.warn(f'Failed/incomplete file {dest} from {src}')
            return False

        # further check? or already checked by sftp.putfo()
        logger.info(f'File transferred, attr={repr(attr)}')
        return True


def do_rename(context, src, dst):
    try:
        context.sftp.posix_rename(str(src), str(dst))
    except IOError:
        logger.warn(f'Failed moving {src} to {dst}')
        return False
    logger.info(f'Moved {src} to {dst}')
    return True


def do_sync(context, fileloc, size, path, filename):
    min_free = context.config.getint('min_free')
    filename_tmp = f'.{filename}.synconce'
    dest = path / filename
    dest_tmp = path / filename_tmp
    logger.info(f'Synchronizing {fileloc} ({size:,} bytes)'
                f' to {dest} (tmp = {dest_tmp})')

    get_space_free = context.remote.space_free(str(path))

    if not confirm_dir(context, path):
        logger.error(f'Cannot make remote directory {path}')
        return False

    try:
        attr = context.sftp.stat(str(dest))
    except FileNotFoundError:
        attr = None

    if attr:
        logger.info(f'Remote path {dest} exists: {repr(attr)}')

        if not stat.S_ISREG(attr.st_mode) or attr.st_size != size:
            logger.warn('Remote path is not a file or has different size')
            return False

        remote_sha1sum = context.remote.hashsum(str(dest), 'sha1')()
        with open(fileloc, 'rb') as f:
            local_sha1sum = utils.head_sha1(f, size)

        if remote_sha1sum == local_sha1sum:
            logger.info(f'Remote and local files match ({remote_sha1sum})')
            return True

        logger.warn(f'Remote ({remote_sha1sum}) and local ({local_sha1sum})'
                    f' files do not match; skipping')
        return False

    try:
        attr_tmp = context.sftp.stat(str(dest_tmp))
    except FileNotFoundError:
        attr_tmp = None

    if attr_tmp:
        # tmp file already there; might be a previous incomplete transfer
        space_free = get_space_free()
        logger.info(f'Remote tmp file {dest_tmp} exists: {repr(attr_tmp)}'
                    f'; {space_free:,} bytes available at "{path}"')

        if not stat.S_ISREG(attr_tmp.st_mode):
            return False

        if space_free + attr_tmp.st_size - size < min_free:
            logger.error(f'Space available ({space_free:,} bytes)'
                         f' is not enough to send partial {dest_tmp}'
                         f': existing size {attr_tmp.st_size:,} bytes'
                         f', min_free {min_free:,} bytes'
                         f', size {size:,} bytes, space after transfer'
                         f' {space_free + attr_tmp.st_size - size:,} bytes')
            return False

        # if maybe_partial fails, fall back to full_transfer
        if maybe_partial(context, fileloc, size, dest_tmp, attr_tmp.st_size):
            # if do_rename fails, redo full_transfer might not help
            return do_rename(context, dest_tmp, dest)

    # falling back or completely new file to sync
    space_free = get_space_free()
    logger.info(f'Remote file {dest} does not exist: doing full transfer'
                f', {space_free:,} bytes available at "{path}"'
                f', sending {fileloc}')

    if space_free - size < min_free:
        logger.error(f'Space available ({space_free:,} bytes)'
                     f' is not enough to store {dest}'
                     f': min_free {min_free:,} bytes'
                     f', size {size:,} bytes'
                     f', space after transfer {space_free - size:,} bytes')
        return False

    return (full_transfer(context, fileloc, size, dest_tmp)
            and do_rename(context, dest_tmp, dest))
