import unittest
import sys
import os

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
from Connection import MilvusConnection
from User import MilvusUser

uri = "http://localhost:19530"
tempAlias = "zilliz2"
username = "test_user"
pwd = "123456"


milvusConnection = MilvusConnection()
user = MilvusUser()


class TestUser(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        milvusConnection.connect(uri=uri, alias=tempAlias)

    @classmethod
    def tearDownClass(cls):
        milvusConnection.disconnect(alias=tempAlias)

    def test_create_user(self):
        user.create_user(username=username, password=pwd, alias=tempAlias)
        res = user.list_users(alias=tempAlias)
        self.assertIn(username, res)

    def test_delete_user(self):
        user.delete_user(username=username, alias=tempAlias)
        res = user.list_users(alias=tempAlias)
        self.assertNotIn(username, res)


if __name__ == "__main__":
    unittest.main()
