__author__ = 'Marvin'
import unittest

from simpledb_tests.utilities import remove_db
from simpledb.query_prosessor.query import *
from simpledb.formatted_storage.metadata import *


class TestMetadataMgr(unittest.TestCase):
    def tearDown(self):
        remove_db()

    def test_all(self):
        remove_db()
        SimpleDB.init("test")
        mdm = SimpleDB.md_mgr()
        assert isinstance(mdm, MetaDataMgr)
        tx = Transaction()
        sch = Schema()
        sch.add_int_field("student_num")
        sch.add_string_field("student_name", 10)
        mdm.create_table("student", sch, tx)
        ti = mdm.get_table_info("student", tx)
        self.assertTrue(ti.schema().has_field("student_num"))
        self.assertTrue(ti.schema().has_field("student_name"))
        self.assertFalse(ti.schema().has_field("student"))

        mdm.create_index("student_name_idx", "student", "student_name", tx)
        idx_info = mdm.get_index_info("student", tx)
        self.assertIsInstance(idx_info, dict)
        self.assertEqual(len(idx_info), 1)


        mdm.create_view("student_view", "student_view_def", tx)
        self.assertEqual(mdm.get_view_def("student_view", tx), "student_view_def")


class TestQuery(unittest.TestCase):
    def tearDown(self):
        remove_db()

    def test_all(self):
        remove_db()
        SimpleDB.init("test")
        mdm = SimpleDB.md_mgr()
        tx = Transaction()
        sch = Schema()
        sch.add_int_field("student_num")
        sch.add_string_field("student_name", 10)
        mdm.create_table("student", sch, tx)
        ti = mdm.get_table_info("student", tx)
        assert isinstance(ti, TableInfo)
        mdm.create_index("student_name_idx", "student", "student_name", tx)
        idx_info = mdm.get_index_info("student", tx)
        mdm.create_view("student_view", "student_view_def", tx)

        p = TablePlan(ti.file_name(), tx)
        us = p.open()
        us.insert()
        student_num = IntConstant(1000000)
        student_name = StringConstant("Marvin")
        us.set_int("student_num", student_num)
        us.set_string("student_name", student_name)

        field_name = FieldNameExpression("student_num")
        field_val = ConstantExpression(student_num)
        term = Term(field_name, field_val)
        pred = Predicate(t=term)
        sp = SelectPlan(p, pred)
        us = sp.open()
        self.assertIsInstance(us, UpdateScan)
        count = 0
        while us.next():
            us.delete()
            count += 1
        us.close()
        self.assertEqual(count, 1)





