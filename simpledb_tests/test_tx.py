__author__ = 'Marvin'
import unittest

from simpledb.plain_storage.bufferslot import *
from simpledb.formatted_storage.log import *
from simpledb.formatted_storage.recovery import LogRecord
from simpledb_tests.utilities import remove_some_start_with


class TempFormatter(PageFormatter):
    def format(self, page):
        assert isinstance(page, MaxPage)
        page.set_int(0, 0)


class TestTransaction(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        remove_some_start_with("tx")
        remove_some_start_with("simple")

    def test_tx(self):
        SimpleDB.BUFFER_SIZE = 1
        SimpleDB.init_file_log_and_buffer_mgr("test")
        self.fmtr = TempFormatter()
        from simpledb.formatted_storage.tx import Transaction
        # simpledb_tests for Transaction.size() and Transaction.append()
        self.assertIsNotNone(LogRecord.log_mgr)
        tx1 = Transaction()
        self.assertEqual(tx1.size("tx_shared"), 0)
        tx1.append("tx_shared", self.fmtr)
        self.assertEqual(tx1.size("tx_shared"), 1)
        # simpledb_tests of manipulation on int and string objects
        tx2 = Transaction()
        tx3 = Transaction()
        blk = Block("tx_shared", 0)
        self.assertEqual(SimpleDB.buffer_mgr().available(), 1)

        def tx1_proc():
            self.assertEqual(SimpleDB.buffer_mgr().available(), 0)
            tx1.pin(blk)
            tx1.set_int(blk, 32, 32)
            tx1.set_string(blk, 36, "sample")
            self.assertEqual(tx1.get_int(blk, 32), 32)  # 2PL guarantees the consistency
            self.assertEqual(tx1.get_string(blk, 36), "sample")
            tx1.commit()

        def tx2_proc():
            self.assertEqual(SimpleDB.buffer_mgr().available(), 0)
            tx2.pin(blk)
            time.sleep(0.2)
            tx2.set_int(blk, 32, 16)
            tx2.set_string(blk, 64, "sample")
            self.assertEqual(tx2.get_int(blk, 32), 16)
            self.assertEqual(tx2.get_string(blk, 64), "sample")
            tx2.commit()

        def tx3_proc():
            self.assertEqual(SimpleDB.buffer_mgr().available(), 1)
            tx3.pin(blk)
            time.sleep(0.2)
            tx3.set_int(blk, 32, 8)
            tx3.set_string(blk, 92, "sample")
            self.assertEqual(SimpleDB.buffer_mgr().available(), 0)
            self.assertEqual(tx3.get_int(blk, 32), 8)
            self.assertEqual(tx3.get_string(blk, 92), "sample")
            tx3.commit()
            #self.assertEqual(SimpleDB.buffer_mgr().available(), 0)
            # still pinned by tx2, in debugging mode, this behaviour is unpredictable

        tx1_thread = threading.Thread(target=tx1_proc)
        tx2_thread = threading.Thread(target=tx2_proc)
        tx3_thread = threading.Thread(target=tx3_proc)

        tx3_thread.start()
        tx2_thread.start()
        tx1_thread.start()

        # simpledb_tests on recovery will be set afterwards






