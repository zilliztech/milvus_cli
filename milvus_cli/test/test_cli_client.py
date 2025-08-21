import unittest
import sys
import os

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
sys.path.append(current_dir)  # Add current directory to path for test_config
from CliClient import MilvusClientCli
from test_config import test_config

# Load connection parameters from configuration
connection_params = test_config.get_connection_params()
uri = connection_params['uri']
token = connection_params.get('token')
tlsmode = connection_params.get('tlsmode', 0)
cert = connection_params.get('cert')

cli_client = MilvusClientCli()


class TestCliClient(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test environment before all tests"""
        # Note: We don't connect here to allow testing connection methods
        pass

    @classmethod
    def tearDownClass(cls):
        """Clean up test environment after all tests"""
        try:
            if cli_client.is_connected():
                cli_client.disconnect()
        except:
            pass

    def test_initialization(self):
        """Test CLI client initialization"""
        # Test that all components are properly initialized
        self.assertIsNotNone(cli_client.connection)
        self.assertIsNotNone(cli_client.database)
        self.assertIsNotNone(cli_client.collection)
        self.assertIsNotNone(cli_client.index)
        self.assertIsNotNone(cli_client.data)
        self.assertIsNotNone(cli_client.user)
        self.assertIsNotNone(cli_client.role)
        self.assertIsNotNone(cli_client.alias)
        self.assertIsNotNone(cli_client.partition)

    def test_connection_methods(self):
        """Test connection-related methods"""
        # Test initial connection state
        self.assertFalse(cli_client.is_connected())
        
        # Test connection
        result = cli_client.connect(uri=uri, token=token, tlsmode=tlsmode, cert=cert)
        self.assertIsNotNone(result)
        self.assertTrue(cli_client.is_connected())
        
        # Test connection info
        conn_info = cli_client.get_connection_info()
        self.assertIsInstance(conn_info, dict)
        self.assertIn('uri', conn_info)
        self.assertIn('is_connected', conn_info)
        self.assertTrue(conn_info['is_connected'])
        
        # Test show connection
        show_result = cli_client.show_connection()
        self.assertIsNotNone(show_result)
        
        # Test show all connections
        show_all_result = cli_client.show_connection(showAll=True)
        self.assertIsNotNone(show_all_result)
        
        # Test disconnection
        disconnect_result = cli_client.disconnect()
        self.assertIsNotNone(disconnect_result)
        self.assertFalse(cli_client.is_connected())

    def test_reconnection(self):
        """Test reconnection functionality"""
        # Ensure disconnected state
        if cli_client.is_connected():
            cli_client.disconnect()
        
        # Connect
        cli_client.connect(uri=uri, token=token, tlsmode=tlsmode, cert=cert)
        self.assertTrue(cli_client.is_connected())
        
        # Disconnect
        cli_client.disconnect()
        self.assertFalse(cli_client.is_connected())
        
        # Reconnect
        cli_client.connect(uri=uri, token=token, tlsmode=tlsmode, cert=cert)
        self.assertTrue(cli_client.is_connected())

    def test_get_version(self):
        """Test getting pymilvus version"""
        version = cli_client.get_version()
        self.assertIsInstance(version, str)
        self.assertGreater(len(version), 0)
        
        # Version should contain digits
        import re
        self.assertTrue(re.search(r'\d+\.\d+', version))

    def test_shared_connection(self):
        """Test that all clients share the same connection"""
        # Connect through CLI
        cli_client.connect(uri=uri, token=token, tlsmode=tlsmode, cert=cert)
        
        # Verify all clients have access to the connection
        self.assertIsNotNone(cli_client.database._get_client())
        self.assertIsNotNone(cli_client.collection._get_client())
        self.assertIsNotNone(cli_client.index._get_client())
        self.assertIsNotNone(cli_client.data._get_client())
        self.assertIsNotNone(cli_client.user._get_client())
        self.assertIsNotNone(cli_client.role._get_client())
        self.assertIsNotNone(cli_client.alias._get_client())
        self.assertIsNotNone(cli_client.partition._get_client())
        
        # Verify they all use the same client instance
        base_client = cli_client.connection.get_client()
        self.assertEqual(cli_client.database._get_client(), base_client)
        self.assertEqual(cli_client.collection._get_client(), base_client)
        self.assertEqual(cli_client.index._get_client(), base_client)
        self.assertEqual(cli_client.data._get_client(), base_client)
        self.assertEqual(cli_client.user._get_client(), base_client)
        self.assertEqual(cli_client.role._get_client(), base_client)
        self.assertEqual(cli_client.alias._get_client(), base_client)
        self.assertEqual(cli_client.partition._get_client(), base_client)

    def test_client_attributes_type(self):
        """Test that all client attributes are of correct type"""
        from ConnectionClient import MilvusClientConnection
        from DatabaseClient import MilvusClientDatabase
        from CollectionClient import MilvusClientCollection
        from IndexClient import MilvusClientIndex
        from DataClient import MilvusClientData
        from UserClient import MilvusClientUser
        from RoleClient import MilvusClientRole
        from AliasClient import MilvusClientAlias
        from PartitionClient import MilvusClientPartition
        
        self.assertIsInstance(cli_client.connection, MilvusClientConnection)
        self.assertIsInstance(cli_client.database, MilvusClientDatabase)
        self.assertIsInstance(cli_client.collection, MilvusClientCollection)
        self.assertIsInstance(cli_client.index, MilvusClientIndex)
        self.assertIsInstance(cli_client.data, MilvusClientData)
        self.assertIsInstance(cli_client.user, MilvusClientUser)
        self.assertIsInstance(cli_client.role, MilvusClientRole)
        self.assertIsInstance(cli_client.alias, MilvusClientAlias)
        self.assertIsInstance(cli_client.partition, MilvusClientPartition)

    def test_database_operations_integration(self):
        """Test basic database operations through CLI"""
        # Connect first
        cli_client.connect(uri=uri, token=token, tlsmode=tlsmode, cert=cert)
        
        # Test database listing (should work without creating databases)
        try:
            db_list = cli_client.database.list_databases()
            self.assertIsInstance(db_list, list)
            self.assertIn("default", db_list)  # Default database should exist
        except Exception as e:
            self.fail(f"Database listing failed: {e}")

    def test_collection_operations_integration(self):
        """Test basic collection operations through CLI"""
        # Connect first
        cli_client.connect(uri=uri, token=token, tlsmode=tlsmode, cert=cert)
        
        # Test collection listing (should work without creating collections)
        try:
            collection_list = cli_client.collection.list_collections()
            self.assertIsInstance(collection_list, list)
        except Exception as e:
            self.fail(f"Collection listing failed: {e}")

    def test_connection_error_handling(self):
        """Test error handling for connection issues"""
        # Ensure disconnected state
        if cli_client.is_connected():
            cli_client.disconnect()
        
        # Test operations on disconnected client
        with self.assertRaises(Exception):
            cli_client.database.list_databases()
        
        with self.assertRaises(Exception):
            cli_client.collection.list_collections()

    def test_invalid_connection_parameters(self):
        """Test connection with invalid parameters"""
        # Ensure disconnected state
        if cli_client.is_connected():
            cli_client.disconnect()
        
        # Test connection with invalid URI
        with self.assertRaises(Exception):
            cli_client.connect(uri="invalid_uri:123456")


if __name__ == "__main__":
    unittest.main()
