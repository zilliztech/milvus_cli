import unittest
import sys
import os

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
from Connection import MilvusConnection
from Collection import MilvusCollection
from Alias import MilvusAlias

uri = "http://localhost:19530"
tempAlias = "zilliz2"
collectionName = "test_collection"
collectionName2 = "test_collection2"
colAlias = "collection_alias"

milvusConnection = MilvusConnection()
collection = MilvusCollection()
alias = MilvusAlias()


class TestAlias(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        milvusConnection.connect(uri=uri, alias=tempAlias)
        fields = [
            "id:VARCHAR:128",
            "title:VARCHAR:512",
            "title_vector:FLOAT_VECTOR:768",
        ]
        collection.create_collection(
            collectionName=collectionName,
            fields=fields,
            alias=tempAlias,
            autoId=False,
            description="this is a test collection",
            primaryField="id",
            isDynamic=True,
            consistencyLevel="Strong",
        )

        collection.create_collection(
            collectionName=collectionName2,
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
        collection.drop_collection(collectionName=collectionName2, alias=tempAlias)

        milvusConnection.disconnect(alias=tempAlias)

    def test_create_alias(self):
        result = alias.create_alias(
            collectionName=collectionName, aliasName=colAlias, alias=tempAlias
        )
        print(result)
        aliasList = alias.list_aliases(collectionName, tempAlias)
        self.assertIn(colAlias, aliasList)

    def test_drop_alias(self):
        alias.drop_alias(aliasName=colAlias, alias=tempAlias)
        aliasList = alias.list_aliases(collectionName, tempAlias)
        self.assertNotIn(colAlias, aliasList)
