import unittest
from unittest.mock import MagicMock

import os
import configparser
import shutil
import tempfile
import hashlib
from pathlib import Path

from synconce.context import Context
from synconce.remote import Remote
from synconce.sync import do_sync


class MockSFTP(object):
    def __init__(self, tmpdir):
        self.tmpdir = tmpdir
        self.bad_mode = set()

    def stat(self, path):
        if 'stat-not-found' in self.bad_mode:
            raise FileNotFoundError
        return (self.tmpdir / path).stat()

    def mkdir(self, path):
        return (self.tmpdir / path).mkdir()

    def getcwd(self):
        return '/'

    def open(self, path, *args, **kwargs):
        fileobj = open(self.tmpdir / path, *args, **kwargs)
        fileobj.set_pipelined = lambda *args, **kwargs: None
        if 'put-bogus' in self.bad_mode:
            fileobj.write(b'.')
        return fileobj

    def putfo(self, fileobj, path, size):
        with open(self.tmpdir / path, 'wb') as f:
            if 'put-bogus' in self.bad_mode:
                f.write(b'.')
            shutil.copyfileobj(fileobj, f)
        attr = (self.tmpdir / path).stat()
        if attr.st_size != size:
            raise IOError()
        return attr


class SyncTest(unittest.TestCase):
    def setUp(self):
        fd, self.tmpfile = tempfile.mkstemp()
        os.close(fd)
        self.tmpfile = Path(self.tmpfile)
        self.tmpdir = Path(tempfile.mkdtemp())
        config = configparser.ConfigParser()
        config.read_dict({
            'sync_test': {
                'data': ':memory:',
                'local': str(self.tmpdir),
                'host': '0.0.0.0',
                'port': '22',
                'user': 'nobody',
                'rsa_key': '/dev/null',
                'remote': '/',
                'exclude': '',
                'min_free': 1000000,
            }
        })
        self.context = Context()
        self.context.config = config['sync_test']

        self.context.remote = Remote(None, None)
        self.context.remote.space_free_mock = MagicMock(return_value=10000000)
        self.context.remote.space_free = lambda *args, **kwargs: \
            lambda: self.context.remote.space_free_mock(*args, **kwargs)
        self.context.remote.hashsum_mock = MagicMock(return_value=None)
        self.context.remote.hashsum = lambda *args, **kwargs: \
            lambda: self.context.remote.hashsum_mock(*args, **kwargs)

        self.context.sftp = MockSFTP(self.tmpdir)

    def write_file(self, content, *path):
        if path:
            path = self.tmpdir / Path(*path)
            path.parent.mkdir(parents=True, exist_ok=True)
        else:
            path = self.tmpfile
        with open(path, 'w') as f:
            print(content, file=f)

    def test_sync_single(self):
        self.write_file('hello')
        self.assertTrue(do_sync(self.context, self.tmpfile, 6,
                                Path(), 'world'))
        with open(self.tmpdir / 'world') as f:
            self.assertEqual(f.read(), 'hello\n')

    def test_sync_single_bogus(self):
        self.write_file('hello')
        self.context.sftp.bad_mode.add('put-bogus')
        self.assertFalse(do_sync(self.context, self.tmpfile, 6,
                                 Path(), 'world'))

    def test_sync_bad_root(self):
        self.write_file('hello')
        self.context.sftp.bad_mode.add('stat-not-found')
        self.assertFalse(do_sync(self.context, self.tmpfile, 6,
                                 Path('inner'), 'world'))
        self.assertFalse((self.tmpdir / 'world').exists())
        self.assertFalse((self.tmpdir / 'inner' / 'world').exists())

    def test_sync_deep(self):
        self.write_file('hello')
        self.assertTrue(do_sync(self.context, self.tmpfile, 6,
                                Path('in', 'ner'), 'world'))
        with open(self.tmpdir / 'in' / 'ner' / 'world') as f:
            self.assertEqual(f.read(), 'hello\n')

    def test_sync_inner_fail(self):
        self.write_file('hello')
        self.write_file('my', 'inner')
        self.assertFalse(do_sync(self.context, self.tmpfile, 6,
                                 Path('inner'), 'world'))
        with open(self.tmpdir / 'inner') as f:
            self.assertEqual(f.read(), 'my\n')
        self.assertFalse((self.tmpdir / 'world').exists())

    def test_sync_conflict_dir(self):
        self.write_file('hello')
        self.write_file('my', 'inner', 'world')
        self.assertFalse(do_sync(self.context, self.tmpfile, 6,
                                 Path(), 'inner'))
        with open(self.tmpdir / 'inner' / 'world') as f:
            self.assertEqual(f.read(), 'my\n')
        self.assertFalse((self.tmpdir / 'world').exists())

    def test_sync_conflict(self):
        self.write_file('hello')
        self.write_file('my', 'world')
        self.context.remote.hashsum_mock = MagicMock(
            return_value=hashlib.sha1(b'my\n').hexdigest())
        self.assertFalse(do_sync(self.context, self.tmpfile, 6,
                                 Path(), 'world'))
        with open(self.tmpdir / 'world') as f:
            self.assertEqual(f.read(), 'my\n')

    def test_sync_partial(self):
        self.write_file('my\nhello')
        self.write_file('my', 'world')
        self.context.remote.hashsum_mock = MagicMock(
            return_value=hashlib.sha1(b'my\n').hexdigest())
        self.assertTrue(do_sync(self.context, self.tmpfile, 9,
                                Path(), 'world'))
        with open(self.tmpdir / 'world') as f:
            self.assertEqual(f.read(), 'my\nhello\n')

    def test_sync_partial_bogus(self):
        self.write_file('my\nhello')
        self.write_file('my', 'world')
        self.context.sftp.bad_mode.add('put-bogus')
        self.context.remote.hashsum_mock = MagicMock(
            return_value=hashlib.sha1(b'my\n').hexdigest())
        self.assertFalse(do_sync(self.context, self.tmpfile, 9,
                                 Path(), 'world'))

    def test_sync_partial_underread(self):
        self.write_file('my')  # delibrate size mismatch
        self.write_file('mymy', 'world')
        self.context.remote.hashsum_mock = MagicMock(
            return_value=hashlib.sha1(b'mymy\n').hexdigest())
        self.assertFalse(do_sync(self.context, self.tmpfile, 9,
                                 Path(), 'world'))
        with open(self.tmpdir / 'world') as f:
            self.assertEqual(f.read(), 'mymy\n')

    def test_sync_identical(self):
        self.write_file('my')
        self.write_file('my', 'world')
        self.context.remote.hashsum_mock = MagicMock(
            return_value=hashlib.sha1(b'my\n').hexdigest())
        self.assertTrue(do_sync(self.context, self.tmpfile, 3,
                                Path(), 'world'))
        with open(self.tmpdir / 'world') as f:
            self.assertEqual(f.read(), 'my\n')

    def test_sync_over(self):
        self.write_file('my')
        self.write_file('my\nhello', 'world')
        self.context.remote.hashsum_mock = MagicMock(
            return_value=hashlib.sha1(b'my\nhello\n').hexdigest())
        self.assertFalse(do_sync(self.context, self.tmpfile, 3,
                                 Path(), 'world'))
        with open(self.tmpdir / 'world') as f:
            self.assertEqual(f.read(), 'my\nhello\n')

    def test_sync_full(self):
        self.write_file('hello')
        self.context.remote.space_free_mock = MagicMock(return_value=100000)
        self.assertFalse(do_sync(self.context, self.tmpfile, 6,
                                 Path(), 'world'))
        self.assertFalse((self.tmpdir / 'world').exists())

    def test_sync_full_after_write(self):
        self.write_file('hello')
        self.context.remote.space_free_mock = MagicMock(return_value=1000002)
        self.assertFalse(do_sync(self.context, self.tmpfile, 6,
                                 Path(), 'world'))
        self.assertFalse((self.tmpdir / 'world').exists())

    def test_sync_partial_full(self):
        self.write_file('my\nhello')
        self.write_file('my', 'world')
        self.context.remote.space_free_mock = MagicMock(return_value=1000002)
        self.context.remote.hashsum_mock = MagicMock(
            return_value=hashlib.sha1(b'my\n').hexdigest())
        self.assertFalse(do_sync(self.context, self.tmpfile, 9,
                                 Path(), 'world'))
        with open(self.tmpdir / 'world') as f:
            self.assertEqual(f.read(), 'my\n')

    def test_sync_partial_almost_full(self):
        self.write_file('my\nhello')
        self.write_file('my', 'world')
        self.context.remote.space_free_mock = MagicMock(return_value=1000007)
        self.context.remote.hashsum_mock = MagicMock(
            return_value=hashlib.sha1(b'my\n').hexdigest())
        self.assertTrue(do_sync(self.context, self.tmpfile, 9,
                                Path(), 'world'))
        with open(self.tmpdir / 'world') as f:
            self.assertEqual(f.read(), 'my\nhello\n')

    def tearDown(self):
        self.tmpfile.unlink()
        shutil.rmtree(self.tmpdir)


if __name__ == '__main__':
    unittest.main()
