from unittest import TestCase

__author__ = 'Marvin'

from simpledb.plain_storage.file import MaxPage
from simpledb.shared_service.server import SimpleDB
from simpledb.formatted_storage.log import BasicLogRecord


class TestBasicLogRecord(TestCase):

    def setUp(self):
        SimpleDB.init_file_mgr("test")
        self.pg = MaxPage()
        self.pg.set_string(4, "Sample string")
        self.pg.set_int(0, 99999)
        self.basic_log_record = BasicLogRecord(self.pg, 0)

    def test_next_int(self):
        self.assertEqual(self.basic_log_record.next_int(), 99999)

    def test_next_string(self):
        self.basic_log_record.next_int()
        self.assertEqual(self.basic_log_record.next_string(), "Sample string")

