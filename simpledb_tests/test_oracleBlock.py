from unittest import TestCase

from simpledb.plain_storage.file import OracleBlockHeader


__author__ = 'Marvin'


class TestOracleBlock(TestCase):

    def test_header(self):
        blk = OracleBlockHeader()
        blk.add_row(100)
        blk.add_row(150)
        blk.delete_row(150)
        self.assertEqual(blk.row_dir[1], -150)
        output = blk.format_header()
        blk2 = OracleBlockHeader()
        blk2.read_header(output)
        self.assertEqual(blk2.body_offset, 10)
        self.assertEqual(len(blk2.row_dir), 2)
        self.assertEqual(blk2.row_dir[1], -150)
