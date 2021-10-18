import unittest
from unittest.mock import MagicMock, call

import os
import configparser
import shutil
import tempfile
import contextlib

import sqlite3

from synconce.context import Context
from synconce.tracker import init_db, execute_walk


class TrackerTest(unittest.TestCase):
    def setUp(self):
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
                'exclude': '*.excluded',
                'min_free': str(1 << 32),
            }
        })
        self.context = Context()
        self.context.config = config['sync_test']

    def write_file(self, content, *path):
        if len(path) > 1:
            os.makedirs(os.path.join(self.tmpdir, *path[:-1]), exist_ok=True)
        with open(os.path.join(self.tmpdir, *path), 'w') as f:
            print(content, file=f)

    def test_tracker_blank(self):
        context = self.context
        context.do_sync = None  # shouldn't be called

        with contextlib.closing(sqlite3.connect(':memory:')) as context.db:
            context.cursor = context.db.cursor()

            init_db(context.db, context.cursor)
            execute_walk(context)

    def test_tracker_excluded(self):
        context = self.context
        context.do_sync = None  # shouldn't be called

        self.write_file('hello', 'world.excluded')

        with contextlib.closing(sqlite3.connect(':memory:')) as context.db:
            context.cursor = context.db.cursor()

            init_db(context.db, context.cursor)
            execute_walk(context)

    def test_tracker_once(self):
        context = self.context
        context.do_sync = MagicMock(return_value=True)

        self.write_file('hello', 'world')

        with contextlib.closing(sqlite3.connect(':memory:')) as context.db:
            context.cursor = context.db.cursor()

            init_db(context.db, context.cursor)
            execute_walk(context)
            execute_walk(context)

        context.do_sync.assert_called_once_with(
            context, os.path.join(self.tmpdir, 'world'), 6, '', 'world')

    def test_tracker_dir_once(self):
        context = self.context
        context.do_sync = MagicMock(return_value=True)

        self.write_file('hello', 'inner', 'world')

        with contextlib.closing(sqlite3.connect(':memory:')) as context.db:
            context.cursor = context.db.cursor()

            init_db(context.db, context.cursor)
            execute_walk(context)
            execute_walk(context)

        context.do_sync.assert_called_once_with(
            context, os.path.join(self.tmpdir, 'inner', 'world'), 6,
            'inner', 'world')

    def test_tracker_failed_twice(self):
        context = self.context
        context.do_sync = MagicMock(return_value=False)

        self.write_file('hello', 'world')

        with contextlib.closing(sqlite3.connect(':memory:')) as context.db:
            context.cursor = context.db.cursor()

            init_db(context.db, context.cursor)
            execute_walk(context)
            execute_walk(context)

        context.do_sync.assert_has_calls([call(
            context, os.path.join(self.tmpdir, 'world'), 6, '', 'world'
        )] * 2)

    def test_tracker_changed_twice(self):
        context = self.context
        context.do_sync = MagicMock(return_value=False)

        self.write_file('hello', 'world')

        with contextlib.closing(sqlite3.connect(':memory:')) as context.db:
            context.cursor = context.db.cursor()

            init_db(context.db, context.cursor)
            execute_walk(context)

            self.write_file('hello!', 'world')
            execute_walk(context)

        context.do_sync.assert_has_calls([
            call(context, os.path.join(self.tmpdir, 'world'), 6, '', 'world'),
            call(context, os.path.join(self.tmpdir, 'world'), 7, '', 'world'),
        ])

    def test_tracker_flatten(self):
        context = self.context
        context.config['flatten'] = '$'
        context.do_sync = MagicMock(return_value=True)

        self.write_file('hello', 'inner', 'world')

        with contextlib.closing(sqlite3.connect(':memory:')) as context.db:
            context.cursor = context.db.cursor()

            init_db(context.db, context.cursor)
            execute_walk(context)
            execute_walk(context)

        context.do_sync.assert_called_once_with(
            context, os.path.join(self.tmpdir, 'inner', 'world'), 6,
            '', 'inner$world')

    def tearDown(self):
        shutil.rmtree(self.tmpdir)


if __name__ == '__main__':
    unittest.main()
