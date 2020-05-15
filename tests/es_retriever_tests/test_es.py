import unittest
from datetime import datetime

from sparktestingbase.sqltestcase import SQLTestCase
from es_retriever.es import index_date_formatter


class TestEs(unittest.TestCase):

    def test_index_date_formatter(self):
        day = datetime(2018, 1, 1)
        base = 'test'
        actual_index = index_date_formatter(base, day)
        expected_index = 'test-2018.01.01'

        self.assertEqual(actual_index, expected_index)


class TestEsStorage(SQLTestCase):
    pass
