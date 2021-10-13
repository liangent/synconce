import os
import hashlib
import shlex


class Remote(object):
    def __init__(self, ssh, base):
        self.ssh = ssh
        self.base = base

    def space_free(self, path):
        stdin, stdout, stderr = self.ssh.exec_command(shlex.join(
            ['df', '-k', os.path.join(self.base, path)]
        ))

        def collect():
            output = stdout.read()
            stdout.channel.close()
            return int(output.splitlines()[-1].split()[3]) * 1024

        return collect

    def remote_hashsum(self, path, algo):
        stdin, stdout, stderr = self.ssh.exec_command(shlex.join(
            [f'{algo}sum', os.path.join(self.base, path)]
        ))
        size = hashlib.new(algo).digest_size

        def collect():
            output = stdout.read(size * 2)
            stdout.channel.close()
            return output.decode('ascii')

        return collect
