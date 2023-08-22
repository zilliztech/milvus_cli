import unittest
import sys
import os

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
from Connection import MilvusConnection
from Collection import MilvusCollection
from Data import MilvusData
from Index import MilvusIndex

uri = "http://localhost:19530"
collectionName = "test_collection"
vectorName = "title_vector"
indexName = "vec_index"

milvusConnection = MilvusConnection()
collection = MilvusCollection()
milvusIndex = MilvusIndex()
milvusData = MilvusData()


class TestIndex(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        milvusConnection.connect(uri=uri)
        fields = [
            "name:VARCHAR:128",
            "title:VARCHAR:512",
        ]
        fields.append(f"{vectorName}:FLOAT_VECTOR:4")

        collection.create_collection(
            collectionName=collectionName,
            fields=fields,
            autoId=False,
            description="this is a test collection",
            primaryField="name",
            isDynamic=True,
            consistencyLevel="Strong",
        )
        milvusIndex.create_index(
            collectionName=collectionName,
            metricType="L2",
            indexName=indexName,
            fieldName=vectorName,
            indexType="IVF_FLAT",
            params=["nlist:128"],
        )
        collection.load_collection(collectionName=collectionName)

    @classmethod
    def tearDownClass(cls):
        collection.drop_collection(collectionName)
        milvusConnection.disconnect()

    def test_insert(self):
        data = [
            ["1", "2"],
            [
                "this is a test title1",
                "this is a test title2",
            ],
            [
                [1, 2, 3, 4],
                [0.1, 0.2, 0.3, 0.4],
            ],
        ]
        res = milvusData.insert(collectionName=collectionName, data=data)
        # print(res)
        self.assertEqual(res.insert_count, 2)
        self.assertEqual(res.succ_count, 2)

    def test_query(self):
        queryParameters = {
            "expr": "name in ['1','2']",
            "output_fields": ["name", "title", vectorName],
            "partition_names": None,
            "timeout": None,
            "alias": None,
        }
        res = milvusData.query(
            collectionName=collectionName,
            queryParameters=queryParameters,
        )
        # print(res)
        self.assertEqual(len(res), 2)

    def test_search(self):
        searchParameters = {
            "data": [[1, 2, 3, 4]],
            "anns_field": vectorName,
            "param": {"nprobe": 16},
            "limit": 10,
            "round_decimal": 4,
        }
        res = milvusData.search(
            collectionName=collectionName,
            searchParameters=searchParameters,
        )
        self.assertIsInstance(res, str)

    def test_delete_entities(self):
        res = milvusData.delete_entities(
            collectionName=collectionName,
            expr="name in ['1']",
        )
        self.assertEqual(res.delete_count, 1)


if __name__ == "__main__":
    unittest.main()
