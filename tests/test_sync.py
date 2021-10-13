import unittest
from unittest.mock import MagicMock

import os
import configparser
import shutil
import tempfile
import hashlib

from synconce.context import Context
from synconce.remote import Remote
from synconce.sync import do_sync


class MockSFTP(object):
    def __init__(self, tmpdir):
        self.tmpdir = tmpdir

    def stat(self, path):
        return os.stat(os.path.join(self.tmpdir, path))

    def mkdir(self, path):
        return os.mkdir(os.path.join(self.tmpdir, path))

    def getcwd(self):
        return '/'

    def open(self, path, *args, **kwargs):
        fileobj = open(os.path.join(self.tmpdir, path), *args, **kwargs)
        fileobj.set_pipelined = lambda *args, **kwargs: None
        return fileobj

    def putfo(self, fileobj, path, size):
        with open(os.path.join(self.tmpdir, path), 'wb') as f:
            shutil.copyfileobj(fileobj, f)
        attr = os.stat(os.path.join(self.tmpdir, path))
        if attr.st_size != size:
            raise IOError()
        return attr


class SyncTest(unittest.TestCase):
    def setUp(self):
        fd, self.tmpfile = tempfile.mkstemp()
        os.close(fd)
        self.tmpdir = tempfile.mkdtemp()
        config = configparser.ConfigParser()
        config.read_dict({
            'sync_test': {
                'data': ':memory:',
                'local': self.tmpdir,
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
            os.makedirs(os.path.join(self.tmpdir, *path[:-1]), exist_ok=True)
            pathname = os.path.join(self.tmpdir, *path)
        else:
            pathname = self.tmpfile
        with open(pathname, 'w') as f:
            print(content, file=f)

    def test_sync_single(self):
        self.write_file('hello')
        self.assertTrue(do_sync(self.context, self.tmpfile, 6, '', 'world'))
        with open(os.path.join(self.tmpdir, 'world')) as f:
            self.assertEqual(f.read(), 'hello\n')

    def test_sync_deep(self):
        self.write_file('hello')
        self.assertTrue(do_sync(self.context, self.tmpfile, 6,
                                os.path.join('in', 'ner'), 'world'))
        with open(os.path.join(self.tmpdir, 'in', 'ner', 'world')) as f:
            self.assertEqual(f.read(), 'hello\n')

    def test_sync_inner_fail(self):
        self.write_file('hello')
        self.write_file('my', 'inner')
        self.assertFalse(do_sync(self.context, self.tmpfile, 6,
                                 'inner', 'world'))
        with open(os.path.join(self.tmpdir, 'inner')) as f:
            self.assertEqual(f.read(), 'my\n')
        self.assertFalse(os.path.exists(os.path.join(self.tmpdir, 'world')))

    def test_sync_conflict(self):
        self.write_file('hello')
        self.write_file('my', 'world')
        self.context.remote.hashsum_mock = MagicMock(
            return_value=hashlib.sha1(b'my\n').hexdigest())
        self.assertFalse(do_sync(self.context, self.tmpfile, 6, '', 'world'))
        with open(os.path.join(self.tmpdir, 'world')) as f:
            self.assertEqual(f.read(), 'my\n')

    def test_sync_partial(self):
        self.write_file('my\nhello')
        self.write_file('my', 'world')
        self.context.remote.hashsum_mock = MagicMock(
            return_value=hashlib.sha1(b'my\n').hexdigest())
        self.assertTrue(do_sync(self.context, self.tmpfile, 9, '', 'world'))
        with open(os.path.join(self.tmpdir, 'world')) as f:
            self.assertEqual(f.read(), 'my\nhello\n')

    def test_sync_identical(self):
        self.write_file('my')
        self.write_file('my', 'world')
        self.context.remote.hashsum_mock = MagicMock(
            return_value=hashlib.sha1(b'my\n').hexdigest())
        self.assertTrue(do_sync(self.context, self.tmpfile, 3, '', 'world'))
        with open(os.path.join(self.tmpdir, 'world')) as f:
            self.assertEqual(f.read(), 'my\n')

    def test_sync_over(self):
        self.write_file('my')
        self.write_file('my\nhello', 'world')
        self.context.remote.hashsum_mock = MagicMock(
            return_value=hashlib.sha1(b'my\nhello\n').hexdigest())
        self.assertFalse(do_sync(self.context, self.tmpfile, 3, '', 'world'))
        with open(os.path.join(self.tmpdir, 'world')) as f:
            self.assertEqual(f.read(), 'my\nhello\n')

    def test_sync_full(self):
        self.write_file('hello')
        self.context.remote.space_free_mock = MagicMock(return_value=100000)
        self.assertFalse(do_sync(self.context, self.tmpfile, 6, '', 'world'))
        self.assertFalse(os.path.exists(os.path.join(self.tmpdir, 'world')))

    def test_sync_full_after_write(self):
        self.write_file('hello')
        self.context.remote.space_free_mock = MagicMock(return_value=1000002)
        self.assertFalse(do_sync(self.context, self.tmpfile, 6, '', 'world'))
        self.assertFalse(os.path.exists(os.path.join(self.tmpdir, 'world')))

    def test_sync_partial_full(self):
        self.write_file('my\nhello')
        self.write_file('my', 'world')
        self.context.remote.space_free_mock = MagicMock(return_value=1000002)
        self.context.remote.hashsum_mock = MagicMock(
            return_value=hashlib.sha1(b'my\n').hexdigest())
        self.assertFalse(do_sync(self.context, self.tmpfile, 9, '', 'world'))
        with open(os.path.join(self.tmpdir, 'world')) as f:
            self.assertEqual(f.read(), 'my\n')

    def test_sync_partial_almost_full(self):
        self.write_file('my\nhello')
        self.write_file('my', 'world')
        self.context.remote.space_free_mock = MagicMock(return_value=1000007)
        self.context.remote.hashsum_mock = MagicMock(
            return_value=hashlib.sha1(b'my\n').hexdigest())
        self.assertTrue(do_sync(self.context, self.tmpfile, 9, '', 'world'))
        with open(os.path.join(self.tmpdir, 'world')) as f:
            self.assertEqual(f.read(), 'my\nhello\n')

    def tearDown(self):
        os.unlink(self.tmpfile)
        shutil.rmtree(self.tmpdir)


if __name__ == '__main__':
    unittest.main()
