from unittest import TestCase

from simpledb.formatted_storage.log import LogIterator, BasicLogRecord, LogMgr
from simpledb.plain_storage.file import MaxPage
from simpledb.shared_service.server import SimpleDB
from simpledb_tests.utilities import remove_some_start_with


__author__ = 'Marvin'


class TestLogMgrAndIter(TestCase):

    def setUp(self):
        remove_some_start_with("simple")
        SimpleDB.init_file_and_log_mgr("test")
        self.page = MaxPage()

    def tearDown(self):
        remove_some_start_with("simple")

    def test_test_log_mgr_and_iter(self):
        logmgr = SimpleDB.log_mgr()
        self.assertIsInstance(logmgr, LogMgr)

        for i in range(2):
            lsn = logmgr.append(["This is a very looooooooooooo" +
                                    "ooooooooooooooooooooooooooooo" +
                                    "ooooooooooooooooooooong record "+str(i)+"."])
            logmgr.flush(lsn)

        for i in range(4):
            logmgr.append([i+1])

        self.assertEqual(SimpleDB.file_mgr().size("simpledb.log"), 3)

        self.assertEqual(logmgr._currentblk._blknum, 2)
        logiter = logmgr.iterator()
        self.assertIsInstance(logiter, LogIterator)
        for i in range(4):
            self.assertEqual(logiter.next().next_int(), 4-i)
        self.assertEqual(logiter.next().next_string(), "This is a very looooooooooooo" +
                                    "ooooooooooooooooooooooooooooo" +
                                    "ooooooooooooooooooooong record 1.")

        self.assertEqual(logmgr._currentblk._blknum, 2)
        logiter = logmgr.iterator()
        count = 0
        for record in logiter.generator():
            self.assertIsInstance(record, BasicLogRecord)
            count += 1
        self.assertEqual(count, 6)
