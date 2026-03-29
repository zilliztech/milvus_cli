from __future__ import annotations

try:
    from .BaseClient import BaseMilvusClient
except ImportError:
    from BaseClient import BaseMilvusClient


class MilvusClientDatabase(BaseMilvusClient):
    """Database operations based on MilvusClient API."""

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
            raise RuntimeError(f"Create database error: {e}") from e

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
            raise RuntimeError(f"List database error: {e}") from e

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
            raise RuntimeError(f"Drop database error: {e}") from e

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
            raise RuntimeError(f"Using database error: {e}") from e

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
            raise RuntimeError(f"Check database existence error: {e}") from e

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
            raise RuntimeError(f"Describe database error: {e}") from e

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
            raise RuntimeError(f"Alter database error: {e}") from e
