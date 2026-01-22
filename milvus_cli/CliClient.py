from ConnectionClient import MilvusClientConnection
from DatabaseClient import MilvusClientDatabase
from CollectionClient import MilvusClientCollection
from IndexClient import MilvusClientIndex
from DataClient import MilvusClientData
from UserClient import MilvusClientUser
from AliasClient import MilvusClientAlias
from PartitionClient import MilvusClientPartition
from RoleClient import MilvusClientRole
from ResourceGroup import MilvusResourceGroup
from PrivilegeGroup import MilvusPrivilegeGroup
from pymilvus import __version__
from tabulate import tabulate
import json
import csv
import io


class OutputFormatter:
    """
    Formatter class for CLI output in different formats (table, json, csv)
    """

    def __init__(self, format="table"):
        self.format = format

    def format_output(self, data, headers=None):
        """Format data based on current format setting."""
        if not data:
            return "No data found."

        if self.format == "json":
            return json.dumps(data, indent=2, default=str)
        elif self.format == "csv":
            return self._to_csv(data)
        else:  # table
            return self._to_table(data, headers)

    def format_list(self, items, header="Item"):
        """Format a simple list of items."""
        if not items:
            return "No items found."

        if self.format == "json":
            return json.dumps(items, indent=2, default=str)
        elif self.format == "csv":
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow([header])
            for item in items:
                writer.writerow([item])
            return output.getvalue()
        else:  # table
            table_data = [[item] for item in items]
            return tabulate(table_data, headers=[header], tablefmt="pretty")

    def _to_table(self, data, headers=None):
        """Convert data to ASCII table."""
        if isinstance(data, list) and len(data) > 0:
            if isinstance(data[0], dict):
                headers = headers or list(data[0].keys())
                table_data = [[row.get(h, "") for h in headers] for row in data]
                return tabulate(table_data, headers=headers, tablefmt="pretty")
            else:
                return tabulate([[item] for item in data], tablefmt="pretty")
        elif isinstance(data, dict):
            return tabulate([[k, v] for k, v in data.items()], headers=["Key", "Value"], tablefmt="pretty")
        else:
            return str(data)

    def _to_csv(self, data):
        """Convert data to CSV format."""
        output = io.StringIO()
        if isinstance(data, list) and len(data) > 0:
            if isinstance(data[0], dict):
                writer = csv.DictWriter(output, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
            else:
                writer = csv.writer(output)
                for item in data:
                    writer.writerow([item])
        elif isinstance(data, dict):
            writer = csv.writer(output)
            writer.writerow(["Key", "Value"])
            for k, v in data.items():
                writer.writerow([k, v])
        return output.getvalue()


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

        # Resource and privilege group clients
        self.resource_group = MilvusResourceGroup(self.connection)
        self.privilege_group = MilvusPrivilegeGroup(self.connection)

        # Output formatter
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
