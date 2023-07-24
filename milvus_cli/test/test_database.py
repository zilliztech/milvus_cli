import unittest;
import sys
import os
from tabulate import tabulate

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from Connection import MilvusConnection
from Database import Database

defaultAddress = 'localhost'
defaultPort = 19530
tempAlias = "zilliz"
dbName="test_db"
milvusConnection = MilvusConnection()
database = Database()

class TestDatabase(unittest.TestCase):

  def setUp(self):
    milvusConnection.connect(host=defaultAddress,port=defaultPort,alias=tempAlias)
    
  def tearDown(self):
    database.drop_database(dbName=dbName,alias=tempAlias)
    milvusConnection.disconnect(alias=tempAlias)

  def test_create_database(self):
    database.create_database(dbName=dbName,alias=tempAlias)
    databaseList = database.list_databases(alias=tempAlias)
    self.assertIn(dbName,databaseList)
   

if __name__ == '__main__':
  unittest.main()