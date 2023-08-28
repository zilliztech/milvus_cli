import unittest
import sys
import os

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from Connection import MilvusConnection

uri = "http://localhost:19530"
milvusConnection = MilvusConnection()


class TestConnection(unittest.TestCase):
    def setUp(self):
        milvusConnection.connect(
            uri=uri,
        )

    def tearDown(self):
        milvusConnection.disconnect()

    def test_show_connection(self):
        res = milvusConnection.showConnection()
        self.assertIn("localhost:19530", res)

    def test_disconnect(self):
        res = milvusConnection.disconnect()
        expectRes = f"Disconnect from default successfully!"
        self.assertEqual(res, expectRes)


if __name__ == "__main__":
    unittest.main()
