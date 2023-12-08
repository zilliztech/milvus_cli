import unittest
import sys
import os

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
from Connection import MilvusConnection
from Collection import MilvusCollection
from Index import MilvusIndex

uri = "http://localhost:19530"
tempAlias = "zilliz2"
collectionName = "test_collection"
vectorName = "title_vector"
indexName = "vec_index"

milvusConnection = MilvusConnection()
collection = MilvusCollection()
milvusIndex = MilvusIndex()


class TestIndex(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        milvusConnection.connect(uri=uri)
        fields = [
            "id:VARCHAR:128",
            "title:VARCHAR:512",
        ]
        fields.append(f"{vectorName}:FLOAT_VECTOR:768")

        collection.create_collection(
            collectionName=collectionName,
            fields=fields,
            autoId=False,
            description="this is a test collection",
            primaryField="id",
            isDynamic=True,
            consistencyLevel="Strong",
        )

    @classmethod
    def tearDownClass(cls):
        collection.drop_collection(tempAlias, collectionName)
        milvusConnection.disconnect(alias=tempAlias)

    def test_create_index(self):
        params = [f"nlist:128"]
        res = milvusIndex.create_index(
            collectionName=collectionName,
            fieldName=vectorName,
            indexName=indexName,
            metricType="IP",
            indexType="IVF_SQ8",
            params=params,
            alias=tempAlias,
        )
        self.assertEqual(res.code, 0)
        print(milvusIndex.list_indexes(collectionName, tempAlias))

    def test_describe_index(self):
        indexDetail = milvusIndex.get_index_details(
            collectionName, indexName, tempAlias
        )
        self.assertIn(indexName, indexDetail)

    def test_has_index(self):
        res = milvusIndex.has_index(collectionName, indexName, tempAlias)
        self.assertTrue(res)

    def test_drop_index(self):
        res = milvusIndex.drop_index(collectionName, vectorName, tempAlias)
        self.assertIsInstance(res, str)


if __name__ == "__main__":
    unittest.main()
