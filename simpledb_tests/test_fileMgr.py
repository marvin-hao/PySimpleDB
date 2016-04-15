from unittest import TestCase

from simpledb.plain_storage.file import FileMgr, Block
from simpledb_tests.utilities import remove_some_start_with


__author__ = 'Marvin'


class TestFileMgr(TestCase):
    def setUp(self):
        self.fm = FileMgr("test")
        self.blk0 = Block("temp_block_test_file1", 0)
        self.blk1 = Block("temp_block_test_file1", 1)
        self.blk2 = Block("temp_block_test_file", 0)
        self.bb_for_writing = bytearray("This is a sample", "utf-32-be")
        self.bb_for_writing.zfill(400)
        self.bb_for_reading = bytearray(400)

    def tearDown(self):
        remove_some_start_with("temp")

    def test_read(self):
        self.fm.write(self.blk0, self.bb_for_writing)
        self.fm.read(self.blk0, self.bb_for_reading)

    def test_write(self):
        self.fm.write(self.blk0, self.bb_for_writing)
        self.fm.write(self.blk1, self.bb_for_writing)
        self.fm.write(self.blk2, self.bb_for_writing)

    def test_size(self):
        self.fm.size("temp_block_test_file1")

    def test_append(self):
        self.fm.write(self.blk2, self.bb_for_writing)
        self.fm.append("temp_block_test_file", self.bb_for_writing)
        self.assertEqual(self.fm.size("temp_block_test_file"), 2)

    def test_is_new(self):
        self.fm.is_new()

    def test_get_file(self):
        self.fm.get_file("temp_block_test_file1")
