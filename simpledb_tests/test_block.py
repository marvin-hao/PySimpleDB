from unittest import TestCase

from simpledb.plain_storage.file import Block


__author__ = 'Marvin'


class TestBlock(TestCase):
    def setUp(self):
        self.blk1 = Block("name1", 0)
        self.blk2 = Block("name2", 0)
        self.blk3 = Block("name2", 1)
        self.blk4 = Block("name1", 0)

    def test_file_name(self):
        self.assertEqual(self.blk1.file_name(), "name1")

    def test_number(self):
        self.assertEqual(self.blk2.number(), 0)

    def test_eq(self):
        self.assertTrue(self.blk1 == self.blk4)
        self.assertFalse(self.blk1 == self.blk2)
        self.assertFalse(self.blk1 != self.blk4)
        self.assertTrue(self.blk1 != self.blk3)

    def test_to_string(self):
        self.assertEqual(str(self.blk1), "[fileTest name1, block 0]")

    def test_hash_code(self):
        self.assertIsInstance(self.blk1.hash_code(), int)

