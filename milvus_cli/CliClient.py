from ConnectionClient import MilvusClientConnection
from DatabaseClient import MilvusClientDatabase
from CollectionClient import MilvusClientCollection
from IndexClient import MilvusClientIndex
from DataClient import MilvusClientData
from UserClient import MilvusClientUser
from AliasClient import MilvusClientAlias
from PartitionClient import MilvusClientPartition
from RoleClient import MilvusClientRole
from OutputFormatter import OutputFormatter
from pymilvus import __version__


class MilvusClientCli(object):
    """
    Main CLI class based on MilvusClient API
    Used to replace the original CLI based on ORM API
    
    This class provides unified access to all Milvus operations
    using the MilvusClient API instead of the ORM API.
    """
    
    def __init__(self):
        """
        Initialize CLI client with connection management
        
        All other clients will use the shared connection client
        to ensure consistent connection state across operations.
        """
        # Create shared connection client
        self.connection = MilvusClientConnection()
        
        # Initialize all operation clients with shared connection
        self.database = MilvusClientDatabase(self.connection)
        self.collection = MilvusClientCollection(self.connection)
        self.index = MilvusClientIndex(self.connection)
        self.data = MilvusClientData(self.connection)
        self.user = MilvusClientUser(self.connection)
        self.role = MilvusClientRole(self.connection)
        self.alias = MilvusClientAlias(self.connection)
        self.partition = MilvusClientPartition(self.connection)
        self.formatter = OutputFormatter()

    def connect(self, uri=None, token=None, tlsmode=0, cert=None):
        """
        Establish connection to Milvus
        
        Args:
            uri: Milvus server address
            token: Authentication token
            tlsmode: TLS mode (0: no encryption, 1: one-way, 2: two-way)
            cert: Certificate file path
            
        Returns:
            Connection result
        """
        return self.connection.connect(uri=uri, token=token, tlsmode=tlsmode, cert=cert)

    def disconnect(self):
        """
        Disconnect from Milvus
        
        Returns:
            Disconnection result
        """
        return self.connection.disconnect()

    def is_connected(self):
        """
        Check if connected to Milvus
        
        Returns:
            bool: Connection status
        """
        return self.connection.is_connected()

    def get_connection_info(self):
        """
        Get connection information
        
        Returns:
            dict: Connection details
        """
        return self.connection.get_connection_info()

    def show_connection(self, showAll=False):
        """
        Show connection information
        
        Args:
            showAll: Whether to show all connection details
            
        Returns:
            Connection information
        """
        return self.connection.showConnection(showAll=showAll)

    def get_version(self):
        """
        Get pymilvus version
        
        Returns:
            str: Version string
        """
        return __version__
