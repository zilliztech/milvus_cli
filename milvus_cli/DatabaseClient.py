class MilvusClientDatabase:
    """
    Database operations class based on MilvusClient API
    Used to replace the original Database operations based on ORM API
    """
    
    def __init__(self, connection_client=None):
        """
        Initialize Database client
        
        Args:
            connection_client: MilvusClientConnection instance
        """
        self.connection_client = connection_client

    def _get_client(self):
        """
        Get MilvusClient instance
        
        Returns:
            MilvusClient instance
            
        Raises:
            Exception: If not connected or connection is invalid
        """
        if not self.connection_client:
            raise Exception("Connection client not set!")
        
        client = self.connection_client.get_client()
        if not client:
            raise Exception("Not connected to Milvus! Please connect first.")
        
        return client

    def create_database(self, dbName=None):
        """
        Create database
        
        Args:
            dbName: Database name
            
        Returns:
            Success message
        """
        try:
            client = self._get_client()
            client.create_database(db_name=dbName)
            return f"Create database {dbName} successfully!"
        except Exception as e:
            raise Exception(f"Create database error!{str(e)}")

    def list_databases(self):
        """
        List all databases
        
        Returns:
            List of database names
        """
        try:
            client = self._get_client()
            return client.list_databases()
        except Exception as e:
            raise Exception(f"List database error!{str(e)}")

    def drop_database(self, dbName=None):
        """
        Drop database
        
        Args:
            dbName: Database name
            
        Returns:
            Success message
        """
        try:
            client = self._get_client()
            
            # Check if database exists before dropping
            database_list = client.list_databases()
            if dbName not in database_list:
                # For consistency, we can either silently succeed or raise an error
                # Here we choose to silently succeed like MilvusClient behavior
                return f"Drop database {dbName} successfully!"
            
            client.drop_database(db_name=dbName)
            return f"Drop database {dbName} successfully!"
        except Exception as e:
            raise Exception(f"Drop database error!{str(e)}")

    def using_database(self, dbName=None):
        """
        Use specific database
        
        Args:
            dbName: Database name
            
        Returns:
            Success message
        """
        try:
            client = self._get_client()
            client.using_database(db_name=dbName)
            return f"Using database {dbName} successfully!"
        except Exception as e:
            raise Exception(f"Using database error!{str(e)}")

    def has_database(self, dbName=None):
        """
        Check if database exists
        
        Args:
            dbName: Database name
            
        Returns:
            bool: Whether database exists
        """
        try:
            client = self._get_client()
            database_list = client.list_databases()
            return dbName in database_list
        except Exception as e:
            raise Exception(f"Check database existence error!{str(e)}")

    def describe_database(self, dbName=None):
        """
        Get database information

        Args:
            dbName: Database name

        Returns:
            Database information dict
        """
        try:
            client = self._get_client()
            # Try to use describe_database if available
            try:
                result = client.describe_database(db_name=dbName)
                return result
            except AttributeError:
                # Fallback: Return basic information
                database_list = client.list_databases()
                if dbName in database_list:
                    return {
                        "database_name": dbName,
                        "exists": True
                    }
                else:
                    return {
                        "database_name": dbName,
                        "exists": False
                    }
        except Exception as e:
            raise Exception(f"Describe database error!{str(e)}")

    def alter_database(self, dbName=None, properties=None):
        """
        Alter database properties

        Args:
            dbName: Database name
            properties: Dictionary of properties to set

        Returns:
            Success message
        """
        try:
            client = self._get_client()
            client.alter_database_properties(db_name=dbName, properties=properties)
            return f"Alter database {dbName} successfully!"
        except Exception as e:
            raise Exception(f"Alter database error!{str(e)}")
