__author__ = 'Marvin'
import unittest

from simpledb.query_prosessor.planner import *
from simpledb_tests.utilities import remove_db


class TestPlanner(unittest.TestCase):
    def tearDown(self):
        remove_db()

    def test_query(self):
        SimpleDB.init("test")
        tx = Transaction()
        cmd = "create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)"
        result = SimpleDB.planner().execute_update(cmd, tx)
        s = "insert into STUDENT(SId, SName, MajorId, GradYear) values "
        studvals = ["(1, 'joe', 10, 2004)",
                    "(2, 'amy', 20, 2004)",
                    "(3, 'max', 10, 2005)",
                    "(4, 'sue', 20, 2005)",
                    "(5, 'bob', 30, 2003)",
                    "(6, 'kim', 20, 2001)",
                    "(7, 'art', 30, 2004)",
                    "(8, 'pat', 20, 2001)",
                    "(9, 'lee', 10, 2004)"]
        for val in studvals:
            SimpleDB.planner().execute_update(s + val, tx)

        qry = "select sname, gradyear from student where gradyear = 2001 "
        plan = SimpleDB.planner().create_query_plan(qry, tx)
        s = plan.open()
        sch = plan.schema()
        self.assertTrue(s.next())
        self.assertEqual(s.get_int("gradyear"), 2001)
        self.assertEqual(s.get_string("sname"), "kim")
        self.assertTrue(s.next())
        self.assertEqual(s.get_string("sname"), "pat")
        self.assertFalse(s.next())