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
colAlias2 = "collection_alias2"

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

    def test_list_aliases_for_collection(self):
        """Test listing aliases for a specific collection"""
        # Create multiple aliases for the same collection
        alias.create_alias(
            collectionName=collectionName,
            aliasName=colAlias,
        )
        alias.create_alias(
            collectionName=collectionName,
            aliasName=colAlias2,
        )

        # List aliases for the collection
        aliasList = alias.list_aliases(collectionName)
        print(f"Aliases for {collectionName}: {aliasList}")
        self.assertIn(colAlias, aliasList)
        self.assertIn(colAlias2, aliasList)

        # Clean up
        alias.drop_alias(aliasName=colAlias)
        alias.drop_alias(aliasName=colAlias2)

    def test_list_all_aliases(self):
        """Test listing all aliases without specifying collection"""
        # Create aliases for different collections
        alias.create_alias(
            collectionName=collectionName,
            aliasName=colAlias,
        )
        alias.create_alias(
            collectionName=collectionName2,
            aliasName=colAlias2,
        )

        # List all aliases (no collection name specified)
        allAliases = alias.list_aliases()
        print(f"All aliases: {allAliases}")
        self.assertIn(colAlias, allAliases)
        self.assertIn(colAlias2, allAliases)

        # Clean up
        alias.drop_alias(aliasName=colAlias)
        alias.drop_alias(aliasName=colAlias2)
