from pymilvus import MilvusClient
from Types import ConnectException


class MilvusClientConnection(object):
    """
    Connection management class based on MilvusClient API
    Used to replace the original connection method based on connections module
    """
    
    def __init__(self):
        self.client = None
        self.uri = "127.0.0.1:19530"
        self.alias = "default"
        self.connection_params = {}
        self._is_connected = False

    def connect(self, uri=None, token=None, tlsmode=0, cert=None):
        """
        Establish connection using MilvusClient
        
        Args:
            uri: Milvus server address, format like "127.0.0.1:19530"
            token: Authentication token, format like "username:password" or API key
            tlsmode: TLS mode (0: no encryption, 1: one-way encryption, 2: two-way encryption)
            cert: Certificate file path
        
        Returns:
            MilvusClient instance
        """
        if uri:
            self.uri = uri
        
        trimToken = None if token is None else token.strip()
        trimcert = None if cert is None else cert.strip()
        
        try:
            # Build connection parameters
            connection_params = {
                "uri": self.uri
            }
            
            # Add authentication token
            if trimToken:
                connection_params["token"] = trimToken
            
            # Handle TLS configuration
            if tlsmode == 0:
                # No encryption
                pass
            elif tlsmode == 1:
                # One-way encryption
                connection_params["secure"] = True
                if trimcert:
                    connection_params["server_pem_path"] = trimcert
            elif tlsmode == 2:
                # Two-way encryption - not implemented yet
                raise NotImplementedError("two-way encryption (tlsmode == 2) is not implemented yet")
            
            # Create MilvusClient instance
            self.client = MilvusClient(**connection_params)
            self.connection_params = connection_params
            self._is_connected = True
            
            return self.client
            
        except Exception as e:
            self._is_connected = False
            raise ConnectException(f"Connect to Milvus error!{str(e)}")

    def get_client(self):
        """
        Get current MilvusClient instance
        
        Returns:
            MilvusClient instance, returns None if not connected
        """
        if self._is_connected and self.client:
            return self.client
        return None

    def showConnection(self, showAll=False):
        """
        Show connection information
        
        Args:
            showAll: Whether to show all connection information
            
        Returns:
            Connection information
        """
        try:
            if not self._is_connected or not self.client:
                return "Connection not found!"
            
            if showAll:
                # Return all connection information (for MilvusClient, usually only one connection)
                return [(self.alias, self.client, self.uri)]
            
            # Return current connection details
            return [self.uri, self.alias, self.uri]
            
        except Exception as e:
            raise Exception(f"Show connection error!{str(e)}")

    def disconnect(self):
        """
        Disconnect from Milvus
        
        Returns:
            Disconnection message
        """
        try:
            if self.client:
                self.client.close()
            
            self.client = None
            self._is_connected = False
            self.connection_params = {}
            
            return f"Disconnect from {self.alias} successfully!"
            
        except Exception as e:
            raise Exception(f"Disconnect from {self.alias} error!{str(e)}")

    def is_connected(self):
        """
        Check if connected
        
        Returns:
            bool: Connection status
        """
        return self._is_connected and self.client is not None

    def get_connection_info(self):
        """
        Get connection parameter information
        
        Returns:
            dict: Connection parameters
        """
        return {
            "uri": self.uri,
            "alias": self.alias,
            "is_connected": self._is_connected,
            "connection_params": self.connection_params
        }
