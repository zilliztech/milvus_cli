import unittest;
import sys
import os

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from Connection import MilvusConnection

defaultAddress = '127.0.0.1'
defaultPort = 19530
class TestConnection(unittest.TestCase):

  def test_connect(self):
    milvusConnection = MilvusConnection()
    milvusConnection.connect(host='127.0.0.1',port=19530)
    res = milvusConnection.showConnection(showAll=True)
    # expectRes = tabulate(
    #         [["Address", defaultAddress], ["User", ''], ["Alias", tempAlias]],
    #         tablefmt="pretty",
    #     )
    
    print(res)

if __name__ == '__main__':
  unittest.main()