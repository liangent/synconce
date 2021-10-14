import unittest

import os
import configparser
import shutil
import tempfile
import base64
import subprocess
import getpass

from synconce import execute


DROPBEAR_BIN = '/usr/sbin/dropbear'
DROPBEAR_PORT = 0xdbea  # DropBEAr
DROPBEAR_HOSTKEY = '''
AAAAB3NzaC1yc2EAAAADAQABAAABAQCS77KkaQeBhZO+YMhlPr33Qq7Wy1BotExP3McXh6jPUo4T
udUoirSmi70Vck3oOOc8FtgwS2BNvdMWnrIoZesbLeIcDdQfnMP5PXixOc3YkyaRc5z7j0G4zedo
efxPEOisIJBfOGLelM5t3n4DG1etzyrs5XZcsib2TBcap7LX3g8v5ejLu9qBnDQa9HvNSuKjbiFQ
j5V37qNd9C9WcfFVEaEhMJgbLkrch98CitEgKrQ6iwhNQfbog2T4KsNYSkdh+hNlwVt+jbpkXRd5
iiMuW9FVVow2OvqkwSa8QW4roz4nLhKBuTSsrJwcmpgHZ3fucp7c2VfhKWbRNqHEw4UNAAABAEjc
l6czdcdiZ5r1/ylFbYnZBT3538ur7FOF1Svz11/HPjDF68+IyH/1tzOVVNCctv1zDo0UM5MZWD1V
QF8L+wOlQwRKTy/F2uAS/XBUi6Cjh3KE9AlCRoLPh7qlEtpaNiZ0l2LAYTaib6LHu5Dq1BWaL1z9
hC3/IZbssGBFXEfyUhs+ebquPzlf4Uvq/UynUBMvsQU9ubRW2/XV2pSlhkrmRhZyAdBB/dr3bU59
OM2IWgcMJecJoz84V3nKwTG/+7ScVjv8hLIL4gRIm53uP/8MgLT9nYcHhvruQ+fqx2RdfHuBH/dk
YgfFC704nx+S8JWUo+MAYqK1PPPr4lViqbkAAACBAK9DTxv3O9G4FpRhj+lG3ZgIFFVB+Ej3bUG+
2dQedO25ctBeXduP3UYzZbuX9o7dTCsMhAGU8dNLAeTK64Oc5v8zc3QGCqipeIifZKM5kJiYsVoL
7/F5mgpzOK6+B2C6+ALHT8qiy4NEDpIfhi7uSmaLquIbNRUHCVc5x1RypvPHAAAAgQDWn9e/4uJs
hKYi9qEr6rOVbCq8X6THCtwYXzPwWY+e+oHVp7VaCwLDXXwAbPAtkbaJsE15a+Paodd4H/7V4xy4
bhyqLdwIOBDUb8N4sEKo9ycarxRwYf+n+EUwZtle4iznNFdAGYCMVhzaeE+hU1O8YnCR9BIXP/ch
79AM46mYiw==
'''
# This has to be one of authorized_keys of the current user
DROPBEAR_CLIENT_KEYFILE = os.path.expanduser('~/.ssh/id_rsa')


class DropbearTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir_local = tempfile.mkdtemp()
        self.tmpdir_remote = tempfile.mkdtemp()
        hostkey_fd, self.hostkey = tempfile.mkstemp()
        with os.fdopen(hostkey_fd, 'wb') as f:
            f.write(base64.b64decode(DROPBEAR_HOSTKEY))
        self.dropbear = subprocess.Popen([
            DROPBEAR_BIN,
            '-F',  # Foreground instead of background
            '-E',  # stdErr instead of syslog
            '-r', self.hostkey,
            '-p', f'127.0.0.1:{DROPBEAR_PORT}',
        ])

        config = configparser.ConfigParser()
        config.read_dict({
            'sync_test': {
                'data': ':memory:',
                'local': self.tmpdir_local,
                'host': '127.0.0.1',
                'port': str(DROPBEAR_PORT),
                'user': getpass.getuser(),
                'rsa_key': DROPBEAR_CLIENT_KEYFILE,
                'remote': self.tmpdir_remote,
                'exclude': '',
                'min_free': str(1 << 32),
            }
        })

        self.config = config['sync_test']

    def write_file(self, base, content, *path):
        os.makedirs(os.path.join(base, *path[:-1]), exist_ok=True)
        with open(os.path.join(base, *path), 'w') as f:
            print(content, file=f)

    def test_sync_single(self):
        self.write_file(self.tmpdir_local, 'hello', 'world')
        execute(self.config)
        with open(os.path.join(self.tmpdir_remote, 'world')) as f:
            self.assertEqual(f.read(), 'hello\n')

    def test_sync_partial(self):
        self.write_file(self.tmpdir_local, 'my\nhello', 'world')
        self.write_file(self.tmpdir_remote, 'my', 'world')
        execute(self.config)
        with open(os.path.join(self.tmpdir_remote, 'world')) as f:
            self.assertEqual(f.read(), 'my\nhello\n')

    def tearDown(self):
        self.dropbear.terminate()
        os.unlink(self.hostkey)
        shutil.rmtree(self.tmpdir_local)
        shutil.rmtree(self.tmpdir_remote)


if __name__ == '__main__':
    unittest.main()
