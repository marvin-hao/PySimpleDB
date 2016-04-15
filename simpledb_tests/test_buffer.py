__author__ = 'Marvin'
import unittest

from simpledb.plain_storage.bufferslot import *
from simpledb.formatted_storage.log import *
from simpledb_tests.utilities import remove_some_start_with


class TempFormatter(PageFormatter):
    def format(self, page):
        assert isinstance(page, MaxPage)
        page.set_string(0, "".join(["5"]*99))


class TestBuffer(unittest.TestCase):
    def setUp(self):
        SimpleDB.init_file_log_and_buffer_mgr("test")
        self.fmtr = TempFormatter()

    def tearDown(self):
        remove_some_start_with("buffer")

    def test_buffer(self):
        buff_mgr = SimpleDB.buffer_mgr()
        self.assertIsInstance(buff_mgr, BufferMgr)
        self.assertEqual(buff_mgr.available(), 8)
        buffer_pool = []
        for i in range(8):
            buffer_pool.append(buff_mgr.pin_new("buffer"+str(i), self.fmtr))
        self.assertEqual(buff_mgr.available(), 0)
        self.assertIsNotNone(buff_mgr._buffer_mgr.pin(Block("buffer5", 0)))
        self.assertIsNone(buff_mgr._buffer_mgr.pin(Block("buffer5", 1)))

        def test_unpin(num):
            time.sleep(0.1)
            buff_mgr.unpin(buffer_pool[num])

        t = threading.Thread(target=test_unpin, args=(0,))
        t.start()
        self.assertEqual(buff_mgr.available(), 0)
        buff_mgr.pin_new("buffer001", self.fmtr)
        t = threading.Thread(target=test_unpin, args=(1,))
        t.start()
        self.assertEqual(buff_mgr.available(), 0)
        buff_mgr.pin_new("buffer002", self.fmtr)




