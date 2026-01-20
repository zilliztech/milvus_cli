import unittest
import sys
import os

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
sys.path.append(currentdir)  # Add current directory to path for test_config
from ConnectionClient import MilvusClientConnection
from test_config import test_config

# Load connection parameters from configuration
connection_params = test_config.get_connection_params()
uri = connection_params['uri']
token = connection_params.get('token')
tlsmode = connection_params.get('tlsmode', 0)
cert = connection_params.get('cert')

milvusConnection = MilvusClientConnection()


class TestConnectionClient(unittest.TestCase):
    def setUp(self):
        """Set up test connection before each test"""
        milvusConnection.connect(
            uri=uri,
            token=token,
            tlsmode=tlsmode,
            cert=cert,
        )

    def tearDown(self):
        """Clean up connection after each test"""
        milvusConnection.disconnect()

    def test_show_connection(self):
        """Test showing connection information"""
        res = milvusConnection.showConnection()
        self.assertIsNotNone(res)
        # Check if connection info contains expected elements
        if isinstance(res, list):
            self.assertGreater(len(res), 0)
        else:
            self.assertNotEqual(res, "Connection not found!")

    def test_disconnect(self):
        """Test disconnection functionality"""
        res = milvusConnection.disconnect()
        expectRes = f"Disconnect from default successfully!"
        self.assertEqual(res, expectRes)

    def test_connection_status(self):
        """Test connection status checking"""
        # Should be connected after setUp
        self.assertTrue(milvusConnection.is_connected())
        
        # Disconnect and check status
        milvusConnection.disconnect()
        self.assertFalse(milvusConnection.is_connected())
        
        # Reconnect for tearDown
        milvusConnection.connect(uri=uri, token=token, tlsmode=tlsmode, cert=cert)

    def test_get_client(self):
        """Test getting MilvusClient instance"""
        client = milvusConnection.get_client()
        self.assertIsNotNone(client)
        # Verify client type
        from pymilvus import MilvusClient
        self.assertIsInstance(client, MilvusClient)

    def test_connection_info(self):
        """Test getting connection parameter information"""
        info = milvusConnection.get_connection_info()
        self.assertIsInstance(info, dict)
        self.assertIn("uri", info)
        self.assertIn("alias", info)
        self.assertIn("is_connected", info)
        self.assertTrue(info["is_connected"])
        self.assertEqual(info["uri"], uri)

    def test_show_all_connections(self):
        """Test showing all connections"""
        res = milvusConnection.showConnection(showAll=True)
        self.assertIsInstance(res, list)
        if len(res) > 0:
            # Each connection should be a tuple with (alias, client, uri)
            connection_info = res[0]
            self.assertIsInstance(connection_info, tuple)
            self.assertEqual(len(connection_info), 3)

    def test_reconnect(self):
        """Test reconnection functionality"""
        # First disconnect
        milvusConnection.disconnect()
        self.assertFalse(milvusConnection.is_connected())
        
        # Then reconnect
        milvusConnection.connect(uri=uri, token=token, tlsmode=tlsmode, cert=cert)
        self.assertTrue(milvusConnection.is_connected())


if __name__ == "__main__":
    unittest.main()
