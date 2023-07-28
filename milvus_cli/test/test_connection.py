import unittest;
import sys
import os
from tabulate import tabulate

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from Connection import MilvusConnection

uri = "http://localhost:19530"
tempAlias = "zilliz"
milvusConnection = MilvusConnection()

class TestConnection(unittest.TestCase):

  def setUp(self):
    milvusConnection.connect(uri=uri ,alias=tempAlias)

  def tearDown(self):
    milvusConnection.disconnect(alias=tempAlias)

  def test_show_connection(self):
    res = milvusConnection.showConnection(alias=tempAlias)
    self.assertIn('localhost:19530', res)

  def test_disconnect(self):
    res = milvusConnection.disconnect(alias=tempAlias)
    expectRes = f"Disconnect from {tempAlias} successfully!"
    self.assertEqual(res, expectRes)
  
  def test_check_connection(self):
    res = milvusConnection.checkConnection(alias=tempAlias)
    self.assertEqual(len(res), 0)


if __name__ == '__main__':
  unittest.main()