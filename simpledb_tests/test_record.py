__author__ = 'Marvin'
import unittest

from simpledb.formatted_storage.record import *
from simpledb_tests.utilities import remove_some_start_with
from simpledb.shared_service.server import SimpleDB


class TestRecord(unittest.TestCase):

    def test_schema(self):
        schema = Schema()
        schema.add_int_field("IntField")
        schema.add_string_field("StringField", 8)
        another_schema = Schema()
        self.assertTrue(schema.has_field("IntField"))
        another_schema.add("IntField", schema)
        self.assertFalse(another_schema.has_field("StringField"))
        for fld in schema.fields():
            another_schema.add(fld, schema)
        self.assertTrue((another_schema.has_field("StringField")))

        another_new_schema = Schema()
        another_new_schema.add_all(another_schema)
        self.assertTrue(another_new_schema.has_field("IntField"))
        self.assertTrue(another_new_schema.has_field("StringField"))

    def test_tableinfor(self):
        schema = Schema()
        schema.add_int_field("IntField")
        schema.add_string_field("StringField", 8)
        ti1 = TableInfo("table1", schema)
        ti2 = TableInfo("table2", ti1.schema(), ti1._offset, ti1.record_length())
        self.assertEqual(ti2.file_name(), "table2.tbl")
        self.assertEqual(ti1.offset("IntField"), ti2.offset("IntField"))
        remove_some_start_with("table")

    def test_recordformatter(self):
        SimpleDB.init_file_log_and_buffer_mgr("test")
        schema = Schema()
        schema.add_int_field("IntField")
        schema.add_string_field("StringField", 8)
        ti1 = TableInfo("table1", schema)
        fmtr = RecordFormatter(ti1)
        page = MaxPage()
        page.read(Block(ti1.file_name(), 0))
        fmtr.format(page)
        page.write(Block(ti1.file_name(), 0))
        remove_some_start_with("table")

    def test_recordpage(self):
        SimpleDB.init_file_log_and_buffer_mgr("test")
        schema = Schema()
        schema.add_int_field("IntField")
        schema.add_string_field("StringField", 8)
        ti1 = TableInfo("table1", schema)
        blk = Block(ti1.file_name(), 0)
        tx = Transaction()
        rp = RecordPage(blk, ti1, tx)
        self.assertFalse(rp.next())
        self.assertTrue(rp.insert())
        rp.set_int("IntField", 256)
        rp.set_string("StringField", "Sample")
        ID = rp.current_id()
        self.assertFalse(rp.next())
        rp.move_to_id(ID-1)
        self.assertTrue(rp.next())
        rp.move_to_id(ID)
        self.assertEqual(rp.get_int("IntField"), 256)
        self.assertEqual(rp.get_string("StringField"), "Sample")
        tx.commit()
        #keep("table")
        #keep("simple")
        tx.pin(blk)
        rp.delete()
        rp.move_to_id(ID-1)
        self.assertFalse(rp.next())
        rp.close()
        self.assertIsNone(rp._blk)
        tx.commit()
        remove_some_start_with("table")
        remove_some_start_with("simple")

    def test_rid(self):
        rid1 = RID(0, 0)
        rid2 = RID(0, 1)
        rid3 = RID(1, 0)
        rid4 = RID(0, 0)
        rid5 = "rid5"
        self.assertTrue(rid1 == rid4)
        self.assertFalse(rid1 == rid2)
        self.assertTrue(rid1 != rid2)
        self.assertTrue(rid1 != rid5)
        self.assertFalse(rid1 != rid4)
        self.assertEqual(rid3.block_number(), 1)
        self.assertEqual(rid3.id(), 0)
        self.assertEqual(str(rid3), "[1, 0]")

    def test_recordfile(self):
        SimpleDB.init_file_log_and_buffer_mgr("test")
        schema = Schema()
        schema.add_int_field("IntField")
        schema.add_string_field("StringField", 8)
        ti = TableInfo("table", schema)
        tx = Transaction()
        rf = RecordFile(ti, tx)

        self.assertFalse(rf.next())
        rf.before_first()
        rf.insert()
        rf.set_int("IntField", 256)
        rf.set_string("StringField", "Sample1")
        rid1 = rf.current_rid()
        self.assertEqual(rid1.block_number(), 0)
        self.assertEqual(rid1.id(), 0)
        rf.insert()
        rf.set_int("IntField", 128)
        rf.set_string("StringField", "Sample2")
        rid2 = rf.current_rid()
        self.assertTrue(rid1 != rid2)
        rf.move_to_rid(rid1)
        rf.delete()
        rf.before_first()
        count = 0
        while rf.next():
            count += 1
        self.assertEqual(count, 1)
        rf.move_to_rid(rid1)
        self.assertEqual(rf.get_int("IntField"), 256)
        self.assertEqual(rf.get_string("StringField"), "Sample1")  # deleted record is still there

        rf.before_first()
        rf.insert()
        rf.set_int("IntField", 64)
        rf.set_string("StringField", "Sample3")
        rid3 = rf.current_rid()
        self.assertTrue(rid1 == rid3)
        self.assertEqual(rf.get_int("IntField"), 64)
        self.assertEqual(rf.get_string("StringField"), "Sample3")  # now record1 is rewritten
        rf.insert()
        rf.set_string("StringField", "Loooooooooooooooooooooooooooooooong record")
        rf.insert()
        rf.set_string("StringField", "Loooooooooooooooooooooooooooooooong record")
        rid4 = rf.current_rid()
        rf.insert()
        rf.set_string("StringField", "Loooooooooooooooooooooooooooooooong record")
        rid5 = rf.current_rid()
        self.assertNotEqual(rid4.block_number(), rid5.block_number())
        rf.move_to_rid(rid4)
        self.assertTrue(rf.next())
        rf.close()
        tx.commit()
        #keep("table")
        #keep("simple")








