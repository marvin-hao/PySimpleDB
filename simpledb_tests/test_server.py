__author__ = 'Marvin'
import unittest

from simpledb.shared_service.server import Startup
from simpledb_tests.utilities import remove_db


class TestServer(unittest.TestCase):

    def setUp(self):
        remove_db()

    def test_server(self):
        Startup.main()