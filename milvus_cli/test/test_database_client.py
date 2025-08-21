import unittest
import sys
import os

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
sys.path.append(current_dir)  # Add current directory to path for test_config
from ConnectionClient import MilvusClientConnection
from DatabaseClient import MilvusClientDatabase
from test_config import test_config

# Load connection parameters from configuration
connection_params = test_config.get_connection_params()
uri = connection_params['uri']
token = connection_params.get('token')
tlsmode = connection_params.get('tlsmode', 0)
cert = connection_params.get('cert')

# Use configured database prefix
database_prefix = test_config.test_collection_prefix
dbName = f"{database_prefix}_db"

milvusConnection = MilvusClientConnection()
database = MilvusClientDatabase(milvusConnection)


class TestDatabaseClient(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test environment before all tests"""
        milvusConnection.connect(uri=uri, token=token, tlsmode=tlsmode, cert=cert)
        
        # Clean up any existing test database first
        try:
            if database.has_database(dbName):
                database.drop_database(dbName)
        except:
            pass

    @classmethod
    def tearDownClass(cls):
        """Clean up test environment after all tests"""
        try:
            # Clean up test database
            if database.has_database(dbName):
                database.drop_database(dbName)
        except Exception as e:
            print(f"Cleanup error: {e}")
        finally:
            milvusConnection.disconnect()

    def test_create_database(self):
        """Test creating database"""
        result = database.create_database(dbName=dbName)
        self.assertEqual(result, f"Create database {dbName} successfully!")
        
        # Verify database exists
        self.assertTrue(database.has_database(dbName))

    def test_list_databases(self):
        """Test listing all databases"""
        # Ensure test database exists
        if not database.has_database(dbName):
            database.create_database(dbName=dbName)
        
        database_list = database.list_databases()
        self.assertIsInstance(database_list, list)
        self.assertIn(dbName, database_list)
        self.assertIn("default", database_list)  # Default database should always exist

    def test_has_database(self):
        """Test checking if database exists"""
        # Ensure test database exists
        if not database.has_database(dbName):
            database.create_database(dbName=dbName)
        
        # Test existing database
        result = database.has_database(dbName=dbName)
        self.assertTrue(result)
        
        # Test non-existent database
        result = database.has_database(dbName="non_existent_database")
        self.assertFalse(result)

    def test_describe_database(self):
        """Test getting database information"""
        # Ensure test database exists
        if not database.has_database(dbName):
            database.create_database(dbName=dbName)
        
        # Test existing database
        result = database.describe_database(dbName=dbName)
        self.assertIsInstance(result, dict)
        self.assertEqual(result["database_name"], dbName)
        self.assertTrue(result["exists"])
        
        # Test non-existent database
        result = database.describe_database(dbName="non_existent_database")
        self.assertIsInstance(result, dict)
        self.assertEqual(result["database_name"], "non_existent_database")
        self.assertFalse(result["exists"])

    def test_using_database(self):
        """Test switching to a database"""
        # Ensure test database exists
        if not database.has_database(dbName):
            database.create_database(dbName=dbName)
        
        # Test using database
        result = database.using_database(dbName=dbName)
        self.assertEqual(result, f"Using database {dbName} successfully!")
        
        # Switch back to default database
        result = database.using_database(dbName="default")
        self.assertEqual(result, "Using database default successfully!")

    def test_drop_database(self):
        """Test dropping database"""
        temp_db_name = f"{database_prefix}_temp_db"
        
        # Clean up if exists
        if database.has_database(temp_db_name):
            database.drop_database(temp_db_name)
        
        # Create temporary database
        database.create_database(dbName=temp_db_name)
        self.assertTrue(database.has_database(temp_db_name))
        
        # Drop database
        result = database.drop_database(dbName=temp_db_name)
        self.assertEqual(result, f"Drop database {temp_db_name} successfully!")
        
        # Verify database was dropped
        self.assertFalse(database.has_database(temp_db_name))

    def test_create_database_error_handling(self):
        """Test error handling when creating existing database"""
        # Ensure test database exists
        if not database.has_database(dbName):
            database.create_database(dbName=dbName)
        
        # Try to create the same database again
        with self.assertRaises(Exception) as context:
            database.create_database(dbName=dbName)
        
        self.assertIn("Create database error", str(context.exception))

    def test_drop_nonexistent_database_error_handling(self):
        """Test error handling when dropping non-existent database"""
        non_existent_db = "definitely_non_existent_database_12345"
        
        # Ensure the database doesn't exist
        self.assertFalse(database.has_database(non_existent_db))
        
        # Try to drop non-existent database
        # Note: MilvusClient may not raise exception for dropping non-existent database
        try:
            result = database.drop_database(dbName=non_existent_db)
            # If no exception is raised, the operation should still complete successfully
            self.assertIn("successfully", result)
        except Exception as e:
            # If exception is raised, it should contain error message
            self.assertIn("Drop database error", str(e))


if __name__ == "__main__":
    unittest.main()
