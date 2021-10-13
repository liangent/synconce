import os
import hashlib
import shlex

def remote_space_free(ssh, base, path):
    stdin, stdout, stderr = ssh.exec_command(shlex.join(
        ['df', '-k', os.path.join(base, path)]
    ))

    def close():
        stdout.channel.shutdown(2)

    def collect():
        output = stdout.read()
        close()
        return int(output.splitlines()[-1].split()[3]) * 1024

    return collect, close

def remote_hashsum(ssh, base, path, algo):
    stdin, stdout, stderr = ssh.exec_command(shlex.join(
        [f'{algo}sum', os.path.join(base, path)]
    ))
    size = hashlib.new(algo).digest_size

    def close():
        stdout.channel.shutdown(2)

    def collect():
        output = stdout.read(size * 2)
        close()
        return output.decode('ascii')

    return collect, close


def append_transfer(srcf, destf):
    transferred = 0
    while len(data := srcf.read(32768)) > 0:
        destf.write(data)
        transferred += len(data)
    return transferred


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

    return sha1sum.hexdigest()
