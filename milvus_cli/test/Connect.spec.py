import unittest;
import sys
import os

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from Connection import MilvusConnection

class TestConnection(unittest.TestCase):

  def test_connect():
    milvusConnection = MilvusConnection()
    res = milvusConnection.connect(host='127.0.0.1',port=19530)
    print(res)

if __name__ == '__main__':
  unittest.main()