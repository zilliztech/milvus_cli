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
from DataClient import MilvusClientData
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
collectionName = f"{collection_prefix}_data_client"
vectorName = "title_vector"
indexName = "vec_index"

milvusConnection = MilvusClientConnection()
collection = MilvusClientCollection(milvusConnection)
milvusIndex = MilvusClientIndex(milvusConnection)
milvusData = MilvusClientData(milvusConnection)


class TestDataClient(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test environment before all tests"""
        milvusConnection.connect(uri=uri, token=token, tlsmode=tlsmode, cert=cert)

        # Clean up any existing test collection first
        try:
            if collection.has_collection(collectionName):
                collection.drop_collection(collectionName)
        except:
            pass

        # Create test collection
        fields = [
            FieldSchema(name="name", dtype=DataType.VARCHAR, max_length=128, is_primary=True),
            FieldSchema(name="title", dtype=DataType.VARCHAR, max_length=512),
            FieldSchema(name=vectorName, dtype=DataType.FLOAT_VECTOR, dim=4),
        ]
        collection.create_collection(
            collectionName=collectionName,
            fields=fields,
            autoId=False,
            description="this is a test collection for data client API",
            primaryField="name",
            isDynamic=True,
            consistencyLevel="Strong",
        )

        # Create index
        milvusIndex.create_index(
            collectionName=collectionName,
            metricType="L2",
            indexName=indexName,
            fieldName=vectorName,
            indexType="IVF_FLAT",
            params=["nlist:128"],
        )

        # Load collection
        collection.load_collection(collectionName=collectionName)

    @classmethod
    def tearDownClass(cls):
        """Clean up test environment after all tests"""
        try:
            # Clean up test collection
            if collection.has_collection(collectionName):
                collection.drop_collection(collectionName)
        except Exception as e:
            print(f"Cleanup error: {e}")
        finally:
            milvusConnection.disconnect()

    def test_insert(self):
        """Test inserting data"""
        # Test data in list of lists format (original format)
        data = [
            ["1", "2"],  # name field
            [
                "this is a test title1",
                "this is a test title2",
            ],  # title field
            [
                [1, 2, 3, 4],
                [0.1, 0.2, 0.3, 0.4],
            ],  # vector field
        ]
        
        result = milvusData.insert(collectionName=collectionName, data=data)
        
        # Verify insert result
        self.assertIsNotNone(result)
        # For MilvusClient, result format may be different
        if hasattr(result, 'insert_count'):
            self.assertEqual(result.insert_count, 2)
        elif isinstance(result, dict) and 'insert_count' in result:
            self.assertEqual(result['insert_count'], 2)

    def test_insert_dict_format(self):
        """Test inserting data in dict format"""
        data = [
            {
                "name": "3",
                "title": "this is a test title3",
                vectorName: [0.3, 0.4, 0.5, 0.6]
            },
            {
                "name": "4", 
                "title": "this is a test title4",
                vectorName: [0.7, 0.8, 0.9, 1.0]
            }
        ]
        
        result = milvusData.insert(collectionName=collectionName, data=data)
        
        # Verify insert result
        self.assertIsNotNone(result)

    def test_query(self):
        """Test querying data"""
        queryParameters = {
            "expr": "name in ['1','2']",
            "output_fields": ["name", "title", vectorName],
            "partition_names": None,
            "timeout": None,
        }
        
        result = milvusData.query(
            collectionName=collectionName,
            queryParameters=queryParameters,
        )
        
        # Verify query result
        self.assertIsInstance(result, list)
        self.assertGreaterEqual(len(result), 0)

    def test_search(self):
        """Test searching vectors"""
        searchParameters = {
            "data": [[1, 2, 3, 4]],
            "anns_field": vectorName,
            "param": {"nprobe": 16},
            "limit": 10,
            "round_decimal": 4,
            "output_fields": ["name", "title"]
        }
        
        # Test with prettier format (default)
        result = milvusData.search(
            collectionName=collectionName,
            searchParameters=searchParameters,
        )
        self.assertIsInstance(result, str)
        
        # Test without prettier format
        result_raw = milvusData.search(
            collectionName=collectionName,
            searchParameters=searchParameters,
            prettierFormat=False
        )
        self.assertIsNotNone(result_raw)

    def test_get_entity_count(self):
        """Test getting entity count"""
        count = milvusData.get_entity_count(collectionName=collectionName)
        self.assertIsInstance(count, int)
        self.assertGreaterEqual(count, 0)

    def test_upsert(self):
        """Test upserting data"""
        # Upsert data to update existing entity
        data = [
            {
                "name": "1",  # Existing entity
                "title": "updated test title1",
                vectorName: [1.1, 2.1, 3.1, 4.1]
            }
        ]
        
        result = milvusData.upsert(collectionName=collectionName, data=data)
        self.assertIsNotNone(result)

    def test_delete_entities(self):
        """Test deleting entities"""
        # Insert a specific entity to delete
        data = [
            {
                "name": "delete_me",
                "title": "entity to be deleted",
                vectorName: [9, 9, 9, 9]
            }
        ]
        
        # Insert the entity
        milvusData.insert(collectionName=collectionName, data=data)
        
        # Delete the entity
        result = milvusData.delete_entities(
            collectionName=collectionName,
            expr="name in ['delete_me']",
        )
        
        # Verify delete result
        self.assertIsNotNone(result)
        if hasattr(result, 'delete_count'):
            self.assertGreaterEqual(result.delete_count, 0)
        elif isinstance(result, dict) and 'delete_count' in result:
            self.assertGreaterEqual(result['delete_count'], 0)

    def test_query_with_partition(self):
        """Test querying data with partition specification"""
        queryParameters = {
            "expr": "name in ['1','2','3','4']",
            "output_fields": ["name", "title"],
            "partition_names": ["_default"],  # Use default partition
            "timeout": 30,
        }
        
        result = milvusData.query(
            collectionName=collectionName,
            queryParameters=queryParameters,
        )
        
        # Verify query result
        self.assertIsInstance(result, list)

    def test_search_edge_cases(self):
        """Test search with edge cases"""
        # Test search with no results
        searchParameters = {
            "data": [[999, 999, 999, 999]],  # Vector likely to have no close matches
            "anns_field": vectorName,
            "param": {"nprobe": 16},
            "limit": 1,
            "round_decimal": 4,
        }
        
        result = milvusData.search(
            collectionName=collectionName,
            searchParameters=searchParameters,
        )
        self.assertIsInstance(result, str)
        
        # Test search with multiple vectors
        searchParameters = {
            "data": [[1, 2, 3, 4], [0.1, 0.2, 0.3, 0.4]],
            "anns_field": vectorName,
            "param": {"nprobe": 16},
            "limit": 5,
            "round_decimal": 2,
        }
        
        # This should only return results for the first vector in MilvusClient
        result = milvusData.search(
            collectionName=collectionName,
            searchParameters=searchParameters,
        )
        self.assertIsInstance(result, str)

    def test_error_handling(self):
        """Test error handling for invalid operations"""
        # Test query with invalid expression
        # Note: MilvusClient may handle invalid fields differently
        try:
            queryParameters = {
                "expr": "invalid_field == 'value'",
                "output_fields": ["name"],
            }
            result = milvusData.query(
                collectionName=collectionName,
                queryParameters=queryParameters,
            )
            # If no exception is raised, result should be empty or error should be in result
            self.assertIsInstance(result, list)
        except Exception as e:
            # If exception is raised, it should contain error message
            self.assertIn("error", str(e).lower())

        # Test search with invalid field
        with self.assertRaises(Exception) as context:
            searchParameters = {
                "data": [[1, 2, 3, 4]],
                "anns_field": "invalid_vector_field",
                "param": {"nprobe": 16},
                "limit": 10,
            }
            milvusData.search(
                collectionName=collectionName,
                searchParameters=searchParameters,
            )
        self.assertIn("error", str(context.exception).lower())

        # Test with invalid collection name
        with self.assertRaises(Exception) as context:
            queryParameters = {
                "expr": "name == '1'",
                "output_fields": ["name"],
            }
            milvusData.query(
                collectionName="non_existent_collection_12345",
                queryParameters=queryParameters,
            )
        self.assertIn("error", str(context.exception).lower())


if __name__ == "__main__":
    unittest.main()
