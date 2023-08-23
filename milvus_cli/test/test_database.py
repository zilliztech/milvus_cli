import unittest
import sys
import os
from tabulate import tabulate

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
from Connection import MilvusConnection
from Database import MilvusDatabase

uri = "http://localhost:19530"
dbName = "test_db"
milvusConnection = MilvusConnection()
database = MilvusDatabase()


class TestDatabase(unittest.TestCase):
    def setUp(self):
        milvusConnection.connect(uri=uri)

    def tearDown(self):
        database.drop_database(dbName=dbName)
        milvusConnection.disconnect()

    def test_create_database(self):
        database.create_database(dbName=dbName)
        databaseList = database.list_databases()
        self.assertIn(dbName, databaseList)


if __name__ == "__main__":
    unittest.main()
