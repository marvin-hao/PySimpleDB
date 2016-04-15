from unittest import TestCase

from simpledb.plain_storage.file import MaxPage, Block
from simpledb.shared_service.server import SimpleDB
from simpledb_tests.utilities import remove_some_start_with


__author__ = 'Marvin'


class TestPage(TestCase):

    def tearDown(self):
        remove_some_start_with("temp")

    def test_page(self):
        SimpleDB.init_file_mgr("test")
        page = MaxPage()
        page.set_int(0, 99999)
        page.set_string(4, "This is the sample string.")
        page.append("temp_sample_file")
        self.assertEqual(len(page._contents), 400)
        self.assertEqual(SimpleDB.fm.size("temp_sample_file"), 1)
        blk1 = Block("temp_sample_file", 1)
        page.write(blk1)
        page.clear_contents()
        page.append("temp_sample_file")
        self.assertEqual(SimpleDB.fm.size("temp_sample_file"), 3)
        blk2 = Block("temp_sample_file", 1)
        page.read(blk2)
        self.assertEqual(page.get_int(0), 99999)
        self.assertEqual(page.get_string(4), "This is the sample string.")
        self.assertEqual(page.get_int(4), 26*4)
