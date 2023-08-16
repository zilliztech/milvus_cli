import unittest
import sys
import os

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
from Connection import MilvusConnection
from Collection import MilvusCollection

uri = "http://localhost:19530"
tempAlias = "zilliz2"
collectionName = "test_collection"
newCollectionName = "test_collection2"

milvusConnection = MilvusConnection()
collection = MilvusCollection()


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
        print(result)

    @classmethod
    def tearDownClass(cls):
        collection.drop_collection(collectionName=newCollectionName, alias=tempAlias)
        milvusConnection.disconnect(alias=tempAlias)

    def test_has_collection(self):
        result = collection.has_collection(
            collectionName=collectionName, alias=tempAlias
        )
        self.assertTrue(result)

    def test_load_collection(self):
        try:
            collection.load_collection(collectionName=collectionName, alias=tempAlias)
        except Exception as e:
            self.assertIn("Load collection error", str(e))

    def release_collection(self):
        result = collection.release_collection(
            collectionName=collectionName, alias=tempAlias
        )
        self.assertEqual(result, f"Release collection {collectionName} successfully!")

    def test_list_collection(self):
        result = collection.list_collections(alias=tempAlias)
        self.assertIsInstance(result, list)

    def test_rename_collection(self):
        result = collection.rename_collection(
            alias=tempAlias, collectionName=collectionName, newName=newCollectionName
        )
        self.assertEqual(
            result,
            f"Rename collection {collectionName} to {newCollectionName} successfully!",
        )

    def test_get_data_count(self):
        result = collection.get_entities_count(collectionName, tempAlias)
        self.assertEqual(result, 0)

    def test_loading_progress(self):
        try:
            collection.show_loading_progress(collectionName, tempAlias)
        except Exception as e:
            self.assertIn("Show loading progress error", str(e))

    def test_list_fields(self):
        result = collection.list_field_names(collectionName, tempAlias)
        self.assertIn("id", result)


if __name__ == "__main__":
    unittest.main()
