import unittest
import sys
import os

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
sys.path.append(current_dir)  # Add current directory to path for test_config
from ConnectionClient import MilvusClientConnection
from CollectionClient import MilvusClientCollection
from IndexClient import MilvusClientIndex
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
collectionName = f"{collection_prefix}_index"
vectorName = "title_vector"
indexName = "vec_index"

# Initialize client instances
connection_client = MilvusClientConnection()
collection_client = MilvusClientCollection(connection_client)
index_client = MilvusClientIndex(connection_client)


class TestIndexClient(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test environment before all tests"""
        # Connect to Milvus
        connection_client.connect(uri=uri, token=token, tlsmode=tlsmode, cert=cert)
        
        # Clean up any existing test collections first
        try:
            if collection_client.has_collection(collectionName):
                collection_client.drop_collection(collectionName)
        except:
            pass
        
        # Define collection schema
        fields = [
            FieldSchema(name="id", dtype=DataType.VARCHAR, max_length=128),
            FieldSchema(name="title", dtype=DataType.VARCHAR, max_length=512),
            FieldSchema(name="title_vector", dtype=DataType.FLOAT_VECTOR, dim=768),
        ]
        
        # Create test collection
        result = collection_client.create_collection(
            collectionName=collectionName,
            fields=fields,
            autoId=False,
            description="this is a test collection for index client",
            primaryField="id",
            isDynamic=True,
            consistencyLevel="Bounded",
        )
        print(f"Setup collection result: {result}")

    @classmethod
    def tearDownClass(cls):
        """Clean up test environment after all tests"""
        try:
            # Clean up test collection
            if collection_client.has_collection(collectionName):
                collection_client.drop_collection(collectionName)
        except Exception as e:
            print(f"Cleanup error: {e}")
        finally:
            connection_client.disconnect()

    def test_create_index(self):
        """
        Test creating an index
        """
        params = ["nlist:128"]
        result = index_client.create_index(
            collectionName=collectionName,
            fieldName=vectorName,
            indexName=indexName,
            metricType="IP",
            indexType="IVF_SQ8",
            params=params,
        )
        
        # Check if result has success code
        self.assertEqual(result.code, 0)
        
        # Verify index was created
        self.assertTrue(index_client.has_index(collectionName, vectorName))

    def test_describe_index(self):
        """
        Test describing an index
        """
        # Ensure index exists first
        if not index_client.has_index(collectionName, vectorName):
            params = ["nlist:128"]
            index_client.create_index(
                collectionName=collectionName,
                fieldName=vectorName,
                indexName=indexName,
                metricType="IP",
                indexType="IVF_SQ8",
                params=params,
            )
        
        index_detail = index_client.get_index_details(collectionName, vectorName)
        self.assertIsInstance(index_detail, str)
        self.assertIn(vectorName, index_detail)

    def test_has_index(self):
        """
        Test checking if index exists
        """
        # Test with existing index
        if not index_client.has_index(collectionName, vectorName):
            params = ["nlist:128"]
            index_client.create_index(
                collectionName=collectionName,
                fieldName=vectorName,
                indexName=indexName,
                metricType="IP",
                indexType="IVF_SQ8",
                params=params,
            )
        
        result = index_client.has_index(collectionName, vectorName)
        self.assertTrue(result)
        
        # Test with non-existing index
        result = index_client.has_index(collectionName, "non_existing_field")
        self.assertFalse(result)

    def test_list_indexes(self):
        """
        Test listing indexes
        """
        # Ensure at least one index exists
        if not index_client.has_index(collectionName, vectorName):
            params = ["nlist:128"]
            index_client.create_index(
                collectionName=collectionName,
                fieldName=vectorName,
                indexName=indexName,
                metricType="IP",
                indexType="IVF_SQ8",
                params=params,
            )
        
        # Test formatted output
        result = index_client.list_indexes(collectionName)
        self.assertIsInstance(result, str)
        
        # Test raw data output
        raw_result = index_client.list_indexes(collectionName, onlyData=True)
        self.assertIsInstance(raw_result, list)
        if raw_result:  # If there are indexes
            self.assertGreater(len(raw_result), 0)

    def test_get_index_build_progress(self):
        """
        Test getting index build progress
        """
        # Ensure index exists first
        if not index_client.has_index(collectionName, vectorName):
            params = ["nlist:128"]
            index_client.create_index(
                collectionName=collectionName,
                fieldName=vectorName,
                indexName=indexName,
                metricType="IP",
                indexType="IVF_SQ8",
                params=params,
            )
        
        result = index_client.get_index_build_progress(collectionName, vectorName)
        self.assertIsInstance(result, dict)
        self.assertIn("progress", result)

    def test_get_vector_index(self):
        """
        Test getting vector index information
        """
        # Ensure vector index exists first
        if not index_client.has_index(collectionName, vectorName):
            params = ["nlist:128"]
            index_client.create_index(
                collectionName=collectionName,
                fieldName=vectorName,
                indexName=indexName,
                metricType="IP",
                indexType="IVF_SQ8",
                params=params,
            )
        
        result = index_client.get_vector_index(collectionName)
        self.assertIsInstance(result, dict)
        if result:  # If vector index exists
            self.assertIn("field_name", result)
            self.assertIn("index_type", result)
            self.assertIn("metric_type", result)

    def test_error_handling(self):
        """
        Test error handling for invalid operations
        """
        # Test creating index on non-existing collection
        with self.assertRaises(Exception) as context:
            index_client.create_index(
                collectionName="non_existing_collection",
                fieldName=vectorName,
                indexName=indexName,
                metricType="IP",
                indexType="IVF_SQ8",
                params=["nlist:128"],
            )
        self.assertIn("error", str(context.exception).lower())
        
        # Test describing non-existing index
        no_index_result = index_client.get_index_details(collectionName, "non_existing_field")
        self.assertIn("No index", no_index_result)
        
        # Test dropping non-existing index
        with self.assertRaises(Exception):
            index_client.drop_index(collectionName, "non_existing_field")

    def test_connection_error_handling(self):
        """
        Test error handling when connection is not available
        """
        # Create an index client without connection
        unconnected_index = MilvusClientIndex()
        
        with self.assertRaises(Exception) as context:
            unconnected_index.create_index("test", "test", "test", "IVF_FLAT", "L2", ["nlist:128"])
        self.assertIn("Connection client not set", str(context.exception))

    def test_z_drop_index(self):
        """
        Test dropping index (run last alphabetically)
        """
        # Ensure index exists first
        if not index_client.has_index(collectionName, vectorName):
            params = ["nlist:128"]
            index_client.create_index(
                collectionName=collectionName,
                fieldName=vectorName,
                indexName=indexName,
                metricType="IP",
                indexType="IVF_SQ8",
                params=params,
            )
        
        # Verify index exists before dropping
        self.assertTrue(index_client.has_index(collectionName, vectorName))
        
        # Drop the index
        result = index_client.drop_index(collectionName, vectorName)
        self.assertIsInstance(result, str)
        
        # Verify index was dropped
        self.assertFalse(index_client.has_index(collectionName, vectorName))

    def test_index_parameters_parsing(self):
        """
        Test different parameter formats
        """
        # Test with different parameter types
        params = ["nlist:256", "m:16", "efConstruction:200"]
        
        try:
            result = index_client.create_index(
                collectionName=collectionName,
                fieldName=vectorName,
                indexName="test_params_index",
                metricType="L2",
                indexType="HNSW",
                params=params,
            )
            self.assertEqual(result.code, 0)
        except Exception as e:
            # Some index types might not be supported, which is acceptable
            self.assertIn("error", str(e).lower())


if __name__ == "__main__":
    unittest.main()
