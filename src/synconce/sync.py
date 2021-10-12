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
        logger.debug('"%s": %s', path, repr(attr))
        return stat.S_ISDIR(attr.st_mode)
    except FileNotFoundError:
        logger.debug('"%s" does not exist', path)

    # path determined non-existent:

    # check parent of path
    parent = os.path.dirname(path)
    if parent == path:
        # at remote_base
        logger.warn('Remote base %s does not exist', sftp.getcwd())
        return False
    if not confirm_dir(ssh, sftp, parent):
        return False

    # create path
    logger.info('Creating remote directory %s', path)
    sftp.mkdir(path)
    return True


def maybe_partial(ssh, sftp, src, src_size, dest, dest_size):
    logger.info('Attempting partial transferring %s (%d B) from %s (%d B)',
                dest, dest_size, src, src_size)
    if dest_size > src_size:
        # dest larger than src
        logger.error('%s (%d B) is larger than local file %s (%d B)',
                     dest, dest_size, src, src_size)
        return False

    stdin, stdout, stderr = ssh.exec_command(shlex.join(
        ['sha1sum', os.path.join(sftp.getcwd(), dest)]
    ))
    channel = stdout.channel

    sha1sum = hashlib.sha1()
    src_to_read = dest_size
    with open(src, 'rb') as srcf:
        while src_to_read > 0:
            data = srcf.read(min(32768, src_to_read))
            sha1sum.update(data)
            src_to_read -= len(data)
            if len(data) == 0:
                break
        if src_to_read > 0:
            # src reading ends early. broken file?
            logger.error('Local file %s could not be read at %d',
                         src, dest_size - src_to_read)
            channel.shutdown(2)
            return False

        src_sha1 = sha1sum.hexdigest().encode('utf-8')
        logger.debug('Local head (%d B) SHA-1: %s', dest_size, src_sha1)
        dest_sha1 = stdout.read(len(src_sha1))
        logger.debug('Remote SHA-1: %s', dest_sha1)
        channel.shutdown(2)

        if src_sha1 != dest_sha1:
            # head of src != dest
            logger.error('Head of local file %s does not match remote %s',
                         src, dest)
            return False

        logger.info('Remote file matches head of local file. Transferring...')
        transferred = 0
        with sftp.open(dest, 'ab') as destf:
            destf.set_pipelined(True)
            data = srcf.read(32768)
            while len(data) > 0:
                destf.write(data)
                transferred += len(data)
                data = srcf.read(32768)

        logger.info('%d bytes transferred.', transferred)

    # at this point, the remote file should be completely written
    attr = sftp.stat(dest)
    logger.info('Remote file %s after sync: %s', dest, repr(attr))
    if attr.st_size == src_size:
        return True
    else:
        logger.warn('Incomplete transferred file %s (%d B) from %s (%d B)',
                    dest, attr.st_size, src, src_size)
        return False


def do_sync(ssh, sftp, fileloc, size, path, filename):
    logger.info('Synchronizing %s to %s',
                fileloc, os.path.join(path, filename))
    if not confirm_dir(ssh, sftp, path):
        logger.error('Cannot make remote directory %s', path)
        return False

    dest = os.path.join(path, filename)

    try:
        attr = sftp.stat(dest)
    except FileNotFoundError:
        logger.info('Remote file %s does not exist. Transferring from %s',
                    dest, fileloc)
        with open(fileloc, 'rb') as f:
            try:
                attr = sftp.putfo(f, dest, size)
            except IOError:
                # incomplete upload? but don't retry or resume here
                logger.warn('Failed/incomplete file %s from %s',
                            dest, fileloc)
                return False

            # further check? or already checked by sftp.putfo()
            logger.info('File transferred, attr=%s', repr(attr))
            return True

    # file already there
    logger.info('Remote file %s exists: %s', dest, repr(attr))
    if not stat.S_ISREG(attr.st_mode):
        return False

    return maybe_partial(ssh, sftp, fileloc, size, dest, attr.st_size)
