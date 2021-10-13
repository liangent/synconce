import unittest

import io
import hashlib

from synconce.utils import head_sha1


class UtilsTest(unittest.TestCase):

    def test_head_sha1_partial(self):
        data = io.BytesIO(b'hello' * 10000)
        sha1 = head_sha1(data, 40000)
        expected = hashlib.sha1((b'hello' * 10000)[:40000]).hexdigest()
        self.assertEqual(sha1, expected)

    def test_head_sha1_overflow(self):
        data = io.BytesIO(b'hello' * 10000)
        sha1 = head_sha1(data, 60000)
        self.assertIsNone(sha1)

    def test_head_sha1_partial_seek(self):
        data = io.BytesIO(b'hello!!' * 10000)
        head_sha1(data, 40000)
        self.assertEqual(data.read(1), b'hello!!'[40000 % 7:][:1])

    def test_head_sha1_overflow_seek(self):
        data = io.BytesIO(b'hello' * 10000)
        sha1 = head_sha1(data, 60000)
        self.assertEqual(data.read(1), b'')


if __name__ == '__main__':
    unittest.main()
