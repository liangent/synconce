import os
import stat
import shlex
import hashlib

import paramiko

import logging
logger = logging.getLogger('synconce.sync')

def confirm_dir(ssh, sftp, path):
    try:
        attr = sftp.stat(path)
        logger.debug(f'"{path}": {repr(attr)}')
        return stat.S_ISDIR(attr.st_mode)
    except FileNotFoundError:
        logger.debug(f'"{path}" does not exist')

    # path determined non-existent:

    # check parent of path
    parent = os.path.dirname(path)
    if parent == path:
        # at remote_base
        logger.warn(f'Remote base {sftp.getcwd()} does not exist')
        return False
    if not confirm_dir(ssh, sftp, parent):
        return False

    # create path
    logger.info(f'Creating remote directory {path}')
    sftp.mkdir(path)
    return True


def space_available(ssh, sftp, path):
    stdin, stdout, stderr = ssh.exec_command(shlex.join(
        ['df', '-k', os.path.join(sftp.getcwd(), path)]
    ))
    output = stdout.read()
    stdout.channel.shutdown(2)
    return int(output.splitlines()[-1].split()[3]) * 1024


def head_sha1(fileobj, head_size):
    sha1sum = hashlib.sha1()
    file_to_read = head_size

    while file_to_read > 0:
        data = fileobj.read(min(32768, file_to_read))
        sha1sum.update(data)
        file_to_read -= len(data)
        if len(data) == 0:
            break

    if file_to_read > 0:
        # file reading ends early. broken file?
        return None

    return sha1sum.hexdigest().encode('utf-8')


def append_transfer(srcf, destf):
    transferred = 0
    data = srcf.read(32768)
    while len(data) > 0:
        destf.write(data)
        transferred += len(data)
        data = srcf.read(32768)
    return transferred


def maybe_partial(ssh, sftp, src, src_size, dest, dest_size):
    logger.info(f'Attempting partial transferring {dest}'
                f' ({dest_size:,} bytes) from {src} ({src_size:,} bytes)')
    if dest_size > src_size:
        # dest larger than src
        logger.error(f'{dest} ({dest_size:,} bytes) is larger'
                     f' than local file {src} ({src_size:,} bytes)')
        return False

    stdin, stdout, stderr = ssh.exec_command(shlex.join(
        ['sha1sum', os.path.join(sftp.getcwd(), dest)]
    ))

    with open(src, 'rb') as srcf:
        src_sha1 = head_sha1(srcf, dest_size)
        logger.debug(f'Local head ({dest_size:,} bytes) SHA-1: {src_sha1}')

        if src_sha1 is None:
            logger.error(f'Local file {src} could not be read to {dest_size}')
            stdout.channel.shutdown(2)
            return False

        dest_sha1 = stdout.read(len(src_sha1))
        logger.debug(f'Remote SHA-1: {dest_sha1}')
        stdout.channel.shutdown(2)

        if src_sha1 != dest_sha1:
            # head of src != dest
            logger.error(f'Head of local {src} does not match remote {dest}')
            return False

        logger.info(f'Remote file matches head of local file. Transferring...')
        with sftp.open(dest, 'ab') as destf:
            destf.set_pipelined(True)
            transferred = append_transfer(srcf, destf)
        logger.info(f'{transferred:,} bytes transferred.')

    # at this point, the remote file should be completely written
    attr = sftp.stat(dest)
    logger.info(f'Remote file {dest} after sync: {repr(attr)}')
    if attr.st_size == src_size:
        return True
    else:
        logger.warn(f'Incomplete transferred {dest} ({attr.st_size:,} bytes)'
                    f' from {src} ({src_size:,} bytes)')
        return False


def full_transfer(ssh, sftp, src, src_size, dest):
    with open(src, 'rb') as f:
        try:
            attr = sftp.putfo(f, dest, src_size)
        except IOError:
            # incomplete upload? but don't retry or resume here
            logger.warn(f'Failed/incomplete file {dest} from {src}')
            return False

        # further check? or already checked by sftp.putfo()
        logger.info(f'File transferred, attr={repr(attr)}')
        return True


def do_sync(ssh, sftp, fileloc, size, path, filename, min_free):
    dest = os.path.join(path, filename)
    logger.info(f'Synchronizing {fileloc} ({size:,} bytes) to {dest}')
    if not confirm_dir(ssh, sftp, path):
        logger.error(f'Cannot make remote directory {path}')
        return False

    space_free = space_available(ssh, sftp, path)
    logger.info(f'{space_free:,} bytes available at "{path}"')

    try:
        attr = sftp.stat(dest)
    except FileNotFoundError:
        logger.info(f'Remote file {dest} does not exist. Sending {fileloc}')

        if space_free - size < min_free:
            logger.error(f'Space available ({space_free:,} bytes)'
                         f' is not enough to store {dest}'
                         f': min_free {min_free:,} bytes'
                         f', size {size:,} bytes'
                         f', space after transfer {space_free - size:,} bytes')
            return False

        return full_transfer(ssh, sftp, fileloc, size, dest)

    # file already there
    logger.info(f'Remote file {dest} exists: {repr(attr)}')
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

    return maybe_partial(ssh, sftp, fileloc, size, dest, attr.st_size)
