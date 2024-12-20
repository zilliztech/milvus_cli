import unittest
import sys
import os

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
from Connection import MilvusConnection
from Collection import MilvusCollection
from Alias import MilvusAlias
from pymilvus import FieldSchema, DataType

uri = "http://localhost:19530"
collectionName = "test_collection"
collectionName2 = "test_collection2"
colAlias = "collection_alias"

milvusConnection = MilvusConnection()
collection = MilvusCollection()
alias = MilvusAlias()


class TestAlias(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        milvusConnection.connect(uri=uri)
        fields = [
            FieldSchema(name="id", dtype=DataType.VARCHAR, max_length=128),
            FieldSchema(name="title", dtype=DataType.VARCHAR, max_length=512),
            FieldSchema(name="title_vector", dtype=DataType.FLOAT_VECTOR, dim=768),
        ]
        collection.create_collection(
            collectionName=collectionName,
            fields=fields,
            autoId=False,
            description="this is a test collection",
            primaryField="id",
            isDynamic=True,
            consistencyLevel="Strong",
        )

        collection.create_collection(
            collectionName=collectionName2,
            fields=fields,
            autoId=False,
            description="this is a test collection",
            primaryField="id",
            isDynamic=True,
            consistencyLevel="Strong",
        )

    @classmethod
    def tearDownClass(cls):
        collection.drop_collection(
            collectionName=collectionName,
        )
        collection.drop_collection(
            collectionName=collectionName2,
        )

        milvusConnection.disconnect()

    def test_create_alias(self):
        result = alias.create_alias(
            collectionName=collectionName,
            aliasName=colAlias,
        )
        print(result)
        aliasList = alias.list_aliases(collectionName)
        self.assertIn(colAlias, aliasList)

    def test_drop_alias(self):
        alias.drop_alias(
            aliasName=colAlias,
        )
        aliasList = alias.list_aliases(collectionName)
        self.assertNotIn(colAlias, aliasList)
