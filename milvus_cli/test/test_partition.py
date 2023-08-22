import unittest
import sys
import os

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
from Connection import MilvusConnection
from Collection import MilvusCollection
from Partition import MilvusPartition

uri = "http://localhost:19530"
tempAlias = "zilliz2"
collectionName = "test_collection"

milvusConnection = MilvusConnection()
collection = MilvusCollection()
partition = MilvusPartition()


class TestCollection(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        milvusConnection.connect(uri=uri, alias=tempAlias)
        fields = [
            "id:VARCHAR:128",
            "title:VARCHAR:512",
            "title_vector:FLOAT_VECTOR:768",
        ]
        result = collection.create_collection(
            collectionName=collectionName,
            fields=fields,
            alias=tempAlias,
            autoId=False,
            description="this is a test collection",
            primaryField="id",
            isDynamic=True,
            consistencyLevel="Strong",
        )

    @classmethod
    def tearDownClass(cls):
        collection.drop_collection(collectionName=collectionName, alias=tempAlias)
        milvusConnection.disconnect(alias=tempAlias)

    def test_create_partition(self):
        result = partition.create_partition(
            collectionName=collectionName,
            partitionName="test_partition",
            description="this is a test partition",
            alias=tempAlias,
        )
        self.assertEqual(result.name, "test_partition")

    def test_describe_partition(self):
        result = partition.describe_partition(
            collectionName=collectionName,
            partitionName="test_partition",
            alias=tempAlias,
        )
        self.assertEqual(result.name, "test_partition")

    def test_load_partition(self):
        result = partition.load_partition(
            collectionName=collectionName,
            partitionName="test_partition",
            alias=tempAlias,
        )
        self.assertEqual(result, f"Load partition test_partition successfully!")

    def test_partitions(self):
        result = partition.list_partition_names(
            collectionName=collectionName, alias=tempAlias
        )
        self.assertIn("test_partition", result)
        result = partition.delete_partition(
            collectionName=collectionName,
            partitionName="test_partition",
            alias=tempAlias,
        )
        self.assertNotIn("test_partition", result)


if __name__ == "__main__":
    unittest.main()
