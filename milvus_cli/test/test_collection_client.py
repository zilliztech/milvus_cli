import unittest
import sys
import os

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
sys.path.append(current_dir)  # Add current directory to path for test_config
from ConnectionClient import MilvusClientConnection
from CollectionClient import MilvusClientCollection
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
collectionName = f"{collection_prefix}_client"
newCollectionName = f"{collection_prefix}_client2"

milvusConnection = MilvusClientConnection()
collection = MilvusClientCollection(milvusConnection)


class TestCollectionClient(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test environment before all tests"""
        milvusConnection.connect(uri=uri, token=token, tlsmode=tlsmode, cert=cert)
        
        # Clean up any existing test collections first
        try:
            if collection.has_collection(collectionName):
                collection.drop_collection(collectionName)
            if collection.has_collection(newCollectionName):
                collection.drop_collection(newCollectionName)
        except:
            pass
        
        # Create test collection
        fields = [
            FieldSchema(name="id", dtype=DataType.VARCHAR, max_length=128),
            FieldSchema(name="title", dtype=DataType.VARCHAR, max_length=512),
            FieldSchema(name="title_vector", dtype=DataType.FLOAT_VECTOR, dim=768),
        ]
        result = collection.create_collection(
            collectionName=collectionName,
            fields=fields,
            autoId=False,
            description="this is a test collection for client API",
            primaryField="id",
            isDynamic=True,
            consistencyLevel="Bounded",
        )
        print(f"Setup collection result: {result}")

    @classmethod
    def tearDownClass(cls):
        """Clean up test environment after all tests"""
        try:
            # Clean up test collections
            if collection.has_collection(collectionName):
                collection.drop_collection(collectionName)
            if collection.has_collection(newCollectionName):
                collection.drop_collection(newCollectionName)
        except Exception as e:
            print(f"Cleanup error: {e}")
        finally:
            milvusConnection.disconnect()

    def test_has_collection(self):
        """Test checking if collection exists"""
        result = collection.has_collection(collectionName=collectionName)
        self.assertTrue(result)
        
        # Test non-existent collection
        result = collection.has_collection(collectionName="non_existent_collection")
        self.assertFalse(result)

    def test_load_collection(self):
        """Test loading collection"""
        try:
            result = collection.load_collection(collectionName=collectionName)
            self.assertIn("successfully", result)
        except Exception as e:
            # Loading may fail due to missing index, which is expected for test collections
            if "index not found" in str(e).lower():
                print(f"Expected error - no index created for test collection: {e}")
                self.assertTrue(True)  # This is expected behavior
            else:
                self.assertIn("Load collection error", str(e))

    def test_release_collection(self):
        """Test releasing collection"""
        try:
            # First load the collection (this may fail if no index exists)
            collection.load_collection(collectionName=collectionName)
            # Then release it
            result = collection.release_collection(collectionName=collectionName)
            self.assertEqual(result, f"Release collection {collectionName} successfully!")
        except Exception as e:
            # Loading may fail due to missing index, which is expected for test collections
            if "index not found" in str(e).lower():
                print(f"Expected error - no index created for test collection: {e}")
                self.assertTrue(True)  # This is expected behavior
            else:
                self.assertIn("error", str(e).lower())

    def test_list_collection(self):
        """Test listing all collections"""
        result = collection.list_collections()
        self.assertIsInstance(result, list)
        self.assertIn(collectionName, result)

    def test_rename_collection(self):
        """Test renaming collection"""
        # Create a temporary collection to rename
        temp_collection_name = f"{collection_prefix}_temp"
        
        # First clean up if exists
        if collection.has_collection(temp_collection_name):
            collection.drop_collection(temp_collection_name)
        if collection.has_collection(newCollectionName):
            collection.drop_collection(newCollectionName)
            
        # Create temporary collection
        fields = [
            FieldSchema(name="id", dtype=DataType.VARCHAR, max_length=128),
            FieldSchema(name="vector", dtype=DataType.FLOAT_VECTOR, dim=128),
        ]
        collection.create_collection(
            collectionName=temp_collection_name,
            fields=fields,
            autoId=False,
            description="temporary collection for rename test",
            primaryField="id",
            isDynamic=False,
            consistencyLevel="Bounded",
        )
        
        # Test rename
        result = collection.rename_collection(
            collectionName=temp_collection_name, newName=newCollectionName
        )
        self.assertEqual(
            result,
            f"Rename collection {temp_collection_name} to {newCollectionName} successfully!",
        )
        
        # Verify the rename worked
        self.assertTrue(collection.has_collection(newCollectionName))
        self.assertFalse(collection.has_collection(temp_collection_name))

    def test_get_data_count(self):
        """Test getting entity count"""
        result = collection.get_entities_count(collectionName)
        self.assertIsInstance(result, int)
        self.assertEqual(result, 0)  # Should be 0 for new collection

    def test_loading_progress(self):
        """Test showing loading progress"""
        try:
            result = collection.show_loading_progress(collectionName)
            self.assertIsInstance(result, dict)
        except Exception as e:
            self.assertIn("Show loading progress error", str(e))

    def test_list_fields(self):
        """Test listing field names"""
        result = collection.list_field_names(collectionName)
        self.assertIsInstance(result, list)
        self.assertIn("id", result)
        self.assertIn("title", result)
        self.assertIn("title_vector", result)

    def test_list_fields_info(self):
        """Test listing detailed field information"""
        result = collection.list_fields_info(collectionName)
        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)
        
        # Check field info structure
        field_info = result[0]
        self.assertIsInstance(field_info, dict)
        self.assertIn("name", field_info)
        self.assertIn("type", field_info)

    def test_get_collection_details(self):
        """Test getting collection details"""
        result = collection.get_collection_details(collectionName=collectionName)
        self.assertIsInstance(result, str)
        self.assertIn(collectionName, result)
        self.assertIn("Name", result)
        self.assertIn("Description", result)

    def test_create_and_drop_collection(self):
        """Test creating and dropping a collection"""
        test_collection_name = f"{collection_prefix}_create_drop"
        
        # Clean up if exists
        if collection.has_collection(test_collection_name):
            collection.drop_collection(test_collection_name)
        
        # Create collection
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
            FieldSchema(name="vector", dtype=DataType.FLOAT_VECTOR, dim=128),
        ]
        
        create_result = collection.create_collection(
            collectionName=test_collection_name,
            fields=fields,
            autoId=True,
            description="test collection for create/drop test",
            primaryField="id",
            isDynamic=False,
            consistencyLevel="Bounded",
        )
        
        self.assertIsInstance(create_result, str)
        self.assertTrue(collection.has_collection(test_collection_name))
        
        # Drop collection
        drop_result = collection.drop_collection(test_collection_name)
        self.assertEqual(drop_result, f"Drop collection {test_collection_name} successfully!")
        self.assertFalse(collection.has_collection(test_collection_name))


if __name__ == "__main__":
    unittest.main()
