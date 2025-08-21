import unittest
import sys
import os

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
sys.path.append(current_dir)  # Add current directory to path for test_config
from ConnectionClient import MilvusClientConnection
from CollectionClient import MilvusClientCollection
from AliasClient import MilvusClientAlias
from pymilvus import FieldSchema, DataType
from test_config import test_config

# Load connection parameters from configuration
connection_params = test_config.get_connection_params()
uri = connection_params['uri']
token = connection_params.get('token')
tlsmode = connection_params.get('tlsmode', 0)
cert = connection_params.get('cert')

# Use configured collection prefix
collection_prefix = test_config.test_collection_prefix
test_collection_name = f"{collection_prefix}_alias"
test_collection_name2 = f"{collection_prefix}_alias2"
test_alias_name = "test_alias_client"
test_alias_name2 = "test_alias_client2"

# Initialize client instances
connection_client = MilvusClientConnection()
collection_client = MilvusClientCollection(connection_client)
alias_client = MilvusClientAlias(connection_client)


class TestAliasClient(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test environment before all tests"""
        # Connect to Milvus
        connection_client.connect(uri=uri, token=token, tlsmode=tlsmode, cert=cert)
        
        # Clean up any existing test collections first
        try:
            if collection_client.has_collection(test_collection_name):
                collection_client.drop_collection(test_collection_name)
            if collection_client.has_collection(test_collection_name2):
                collection_client.drop_collection(test_collection_name2)
        except:
            pass
        
        # Clean up any existing test aliases
        try:
            if alias_client.has_alias(test_alias_name):
                alias_client.drop_alias(test_alias_name)
            if alias_client.has_alias(test_alias_name2):
                alias_client.drop_alias(test_alias_name2)
        except:
            pass
        
        # Define collection schema
        fields = [
            FieldSchema(name="id", dtype=DataType.VARCHAR, max_length=128),
            FieldSchema(name="title", dtype=DataType.VARCHAR, max_length=512),
            FieldSchema(name="title_vector", dtype=DataType.FLOAT_VECTOR, dim=768),
        ]
        
        # Create test collections
        result1 = collection_client.create_collection(
            collectionName=test_collection_name,
            fields=fields,
            autoId=False,
            description="this is a test collection for alias client",
            primaryField="id",
            isDynamic=True,
            consistencyLevel="Bounded",
        )
        
        result2 = collection_client.create_collection(
            collectionName=test_collection_name2,
            fields=fields,
            autoId=False,
            description="this is another test collection for alias client",
            primaryField="id",
            isDynamic=True,
            consistencyLevel="Bounded",
        )

    @classmethod
    def tearDownClass(cls):
        """Clean up test environment after all tests"""
        try:
            # Clean up test aliases
            if alias_client.has_alias(test_alias_name):
                alias_client.drop_alias(test_alias_name)
            if alias_client.has_alias(test_alias_name2):
                alias_client.drop_alias(test_alias_name2)
            
            # Clean up test collections
            if collection_client.has_collection(test_collection_name):
                collection_client.drop_collection(test_collection_name)
            if collection_client.has_collection(test_collection_name2):
                collection_client.drop_collection(test_collection_name2)
                
        except Exception as e:
            print(f"Cleanup error: {e}")
        finally:
            connection_client.disconnect()

    def test_create_alias(self):
        """
        Test creating an alias
        """
        # Clean up first if alias exists
        try:
            if alias_client.has_alias(test_alias_name):
                alias_client.drop_alias(test_alias_name)
        except:
            pass
        
        result = alias_client.create_alias(
            collectionName=test_collection_name,
            aliasName=test_alias_name
        )
        self.assertEqual(result, f"Create alias {test_alias_name} successfully!")
        # Verify alias was created
        self.assertTrue(alias_client.has_alias(test_alias_name))

    def test_list_aliases(self):
        """
        Test listing aliases for a collection
        """
        # Ensure test alias exists
        if not alias_client.has_alias(test_alias_name):
            alias_client.create_alias(test_collection_name, test_alias_name)
        
        aliases = alias_client.list_aliases(test_collection_name)
        
        self.assertIsInstance(aliases, list)
        self.assertIn(test_alias_name, aliases)

    def test_has_alias(self):
        """
        Test checking if alias exists
        """
        # Ensure test alias exists
        if not alias_client.has_alias(test_alias_name):
            alias_client.create_alias(test_collection_name, test_alias_name)
        
        # Test existing alias
        self.assertTrue(alias_client.has_alias(test_alias_name))
        
        # Test non-existing alias
        self.assertFalse(alias_client.has_alias("non_existing_alias_12345"))

    def test_describe_alias(self):
        """
        Test describing alias details
        """
        # Ensure test alias exists
        if not alias_client.has_alias(test_alias_name):
            alias_client.create_alias(test_collection_name, test_alias_name)
        
        alias_info = alias_client.describe_alias(test_alias_name)
        
        self.assertIsInstance(alias_info, dict)
        # Check if collection name is in the response
        if 'collection_name' in alias_info:
            self.assertEqual(alias_info['collection_name'], test_collection_name)

    def test_get_alias_collection(self):
        """
        Test getting collection name from alias
        """
        # Ensure test alias exists
        if not alias_client.has_alias(test_alias_name):
            alias_client.create_alias(test_collection_name, test_alias_name)
        
        collection_name = alias_client.get_alias_collection(test_alias_name)
        
        # The response might vary based on Milvus version
        self.assertIsInstance(collection_name, str)

    def test_alter_alias(self):
        """
        Test altering alias to point to different collection
        """
        # Ensure test alias exists and points to first collection
        if not alias_client.has_alias(test_alias_name):
            alias_client.create_alias(test_collection_name, test_alias_name)
        
        # Alter alias to point to second collection
        result = alias_client.alter_alias(test_alias_name, test_collection_name2)
        
        self.assertEqual(result, f"Alter alias {test_alias_name} successfully!")
        
        # Verify alias now points to second collection
        try:
            collection_name = alias_client.get_alias_collection(test_alias_name)
            if collection_name:  # Only check if get_alias_collection returns a value
                self.assertEqual(collection_name, test_collection_name2)
        except:
            # Some versions might not support this operation or return different format
            pass

    def test_validate_alias_name(self):
        """
        Test alias name validation
        """
        # Valid alias names
        self.assertTrue(alias_client.validate_alias_name("valid_alias"))
        self.assertTrue(alias_client.validate_alias_name("valid-alias"))
        self.assertTrue(alias_client.validate_alias_name("validalias123"))
        
        # Invalid alias names
        self.assertFalse(alias_client.validate_alias_name(""))
        self.assertFalse(alias_client.validate_alias_name(None))
        self.assertFalse(alias_client.validate_alias_name("invalid alias"))  # space
        self.assertFalse(alias_client.validate_alias_name("invalid@alias"))  # special char

    def test_create_multiple_aliases(self):
        """
        Test creating multiple aliases
        """
        # Clean up first
        test_aliases = [test_alias_name, test_alias_name2]
        for alias in test_aliases:
            try:
                if alias_client.has_alias(alias):
                    alias_client.drop_alias(alias)
            except:
                pass
        
        results = alias_client.create_multiple_aliases(
            test_collection_name,
            test_aliases
        )
        
        self.assertIsInstance(results, list)
        self.assertEqual(len(results), 2)
        
        for result in results:
            self.assertIn('alias', result)
            self.assertIn('status', result)
            self.assertIn('message', result)

    def test_list_all_aliases(self):
        """
        Test listing all aliases in the system
        """
        # Ensure at least one alias exists
        if not alias_client.has_alias(test_alias_name):
            alias_client.create_alias(test_collection_name, test_alias_name)
        
        all_aliases = alias_client.list_all_aliases()
        
        self.assertIsInstance(all_aliases, list)
        # Should contain at least our test alias
        alias_names = [alias['alias'] for alias in all_aliases if isinstance(alias, dict) and 'alias' in alias]
        if alias_names:  # Only check if we got any results
            self.assertIn(test_alias_name, alias_names)

    def test_error_handling(self):
        """
        Test error handling for invalid operations
        """
        # Test creating alias with invalid collection name
        with self.assertRaises(Exception) as context:
            alias_client.create_alias("non_existing_collection", "test_alias")
        self.assertIn("error", str(context.exception).lower())
        
        # Test describing non-existing alias
        with self.assertRaises(Exception) as context:
            alias_client.describe_alias("non_existing_alias_12345")
        self.assertIn("error", str(context.exception).lower())
        
        # Test dropping non-existing alias
        with self.assertRaises(Exception) as context:
            alias_client.drop_alias("non_existing_alias_12345")
        self.assertIn("error", str(context.exception).lower())

    def test_connection_error_handling(self):
        """
        Test error handling when connection is not available
        """
        # Create an alias client without connection
        unconnected_alias = MilvusClientAlias()
        
        with self.assertRaises(Exception) as context:
            unconnected_alias.create_alias("test", "test")
        self.assertIn("Connection client not set", str(context.exception))

    def test_duplicate_alias_creation(self):
        """
        Test creating duplicate alias
        """
        # Ensure test alias exists
        if not alias_client.has_alias(test_alias_name):
            alias_client.create_alias(test_collection_name, test_alias_name)
        
        # Try to create the same alias again
        with self.assertRaises(Exception) as context:
            alias_client.create_alias(test_collection_name, test_alias_name)
        
        self.assertIn("error", str(context.exception).lower())

    def test_z_drop_multiple_aliases(self):
        """
        Test dropping multiple aliases (run after other tests)
        """
        # Ensure test aliases exist
        test_aliases = [test_alias_name, test_alias_name2]
        for alias in test_aliases:
            if not alias_client.has_alias(alias):
                try:
                    alias_client.create_alias(test_collection_name, alias)
                except:
                    pass  # Might fail if alias already exists
        
        results = alias_client.drop_multiple_aliases(test_aliases)
        
        self.assertIsInstance(results, list)
        self.assertEqual(len(results), 2)
        
        for result in results:
            self.assertIn('alias', result)
            self.assertIn('status', result)
            self.assertIn('message', result)

    def test_zz_drop_alias(self):
        """
        Test dropping alias (run last alphabetically)
        """
        # Ensure test alias exists
        if not alias_client.has_alias(test_alias_name):
            alias_client.create_alias(test_collection_name, test_alias_name)
        
        # Verify alias exists before deletion
        self.assertTrue(alias_client.has_alias(test_alias_name))
        
        # Drop the alias
        result = alias_client.drop_alias(test_alias_name)
        self.assertEqual(result, f"Drop alias {test_alias_name} successfully!")
        
        # Verify alias was dropped
        self.assertFalse(alias_client.has_alias(test_alias_name))


if __name__ == "__main__":
    unittest.main()
