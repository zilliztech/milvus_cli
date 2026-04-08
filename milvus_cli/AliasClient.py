from __future__ import annotations

from pymilvus import MilvusClient
try:
    from .BaseClient import BaseMilvusClient
except ImportError:
    from BaseClient import BaseMilvusClient


class MilvusClientAlias(BaseMilvusClient):
    """Alias operations based on MilvusClient API."""

    def __init__(self, connection_client=None) -> None:
        super().__init__(connection_client)
        self.alias = "default"

    def create_alias(self, collectionName, aliasName):
        """
        Create alias for a collection
        
        Args:
            collectionName: Name of the collection
            aliasName: Name of the alias to create
            
        Returns:
            Success message
        """
        try:
            client = self._get_client()
            
            # Check if alias already exists
            if self.has_alias(aliasName):
                raise ValueError(f"Alias '{aliasName}' already exists")
            
            # Create alias using MilvusClient API
            client.create_alias(
                collection_name=collectionName,
                alias=aliasName
            )
            
            return f"Create alias {aliasName} successfully!"
            
        except Exception as e:
            raise RuntimeError(f"Create alias error: {e}") from e

    def list_aliases(self, collectionName=None):
        """
        List aliases for a collection or all aliases in the database

        Args:
            collectionName: Name of the collection (optional)
                           If not provided, returns all aliases

        Returns:
            List of aliases
        """
        try:
            client = self._get_client()

            if collectionName:
                # List aliases for specific collection
                aliases = client.list_aliases(collection_name=collectionName)

                # Extract alias names from the response
                if isinstance(aliases, dict) and 'aliases' in aliases:
                    return aliases['aliases']
                elif isinstance(aliases, list):
                    return aliases
                else:
                    return aliases if aliases else []
            else:
                # List all aliases in the database
                return self.list_all_aliases()

        except Exception as e:
            raise RuntimeError(f"List alias error: {e}") from e

    def drop_alias(self, aliasName):
        """
        Drop an alias
        
        Args:
            aliasName: Name of the alias to drop
            
        Returns:
            Success message
        """
        try:
            client = self._get_client()
            
            # Check if alias exists first
            if not self.has_alias(aliasName):
                raise ValueError(f"Alias '{aliasName}' does not exist")
            
            # Drop alias using MilvusClient API
            client.drop_alias(alias=aliasName)
            
            return f"Drop alias {aliasName} successfully!"
            
        except Exception as e:
            raise RuntimeError(f"Drop alias error: {e}") from e

    def alter_alias(self, aliasName, collectionName):
        """
        Alter alias to point to a different collection
        
        Args:
            aliasName: Name of the alias to alter
            collectionName: New collection name for the alias
            
        Returns:
            Success message
        """
        try:
            client = self._get_client()
            
            # Alter alias using MilvusClient API
            client.alter_alias(
                alias=aliasName,
                collection_name=collectionName
            )
            
            return f"Alter alias {aliasName} successfully!"
            
        except Exception as e:
            raise RuntimeError(f"Alter alias error: {e}") from e

    def describe_alias(self, aliasName):
        """
        Describe alias details
        
        Args:
            aliasName: Name of the alias to describe
            
        Returns:
            Alias information dictionary
        """
        try:
            client = self._get_client()
            
            # Describe alias using MilvusClient API
            alias_info = client.describe_alias(alias=aliasName)
            
            return alias_info
            
        except Exception as e:
            raise RuntimeError(f"Describe alias error: {e}") from e

    def has_alias(self, aliasName):
        """
        Check if alias exists
        
        Args:
            aliasName: Name of the alias to check
            
        Returns:
            bool: Whether alias exists
        """
        try:
            self.describe_alias(aliasName)
            return True
        except Exception:
            return False

    def get_alias_collection(self, aliasName):
        """
        Get the collection name that an alias points to
        
        Args:
            aliasName: Name of the alias
            
        Returns:
            Collection name
        """
        try:
            alias_info = self.describe_alias(aliasName)
            if isinstance(alias_info, dict):
                return alias_info.get('collection_name', '')
            return ''
            
        except Exception as e:
            raise RuntimeError(f"Get alias collection error: {e}") from e

    def list_all_aliases(self):
        """
        List all aliases in the system
        
        Returns:
            List of all aliases
        """
        try:
            client = self._get_client()
            
            # Get all collections first
            collections = client.list_collections()
            
            all_aliases = []
            for collection in collections:
                try:
                    aliases = self.list_aliases(collection)
                    if aliases:
                        for alias in aliases:
                            all_aliases.append({
                                'alias': alias,
                                'collection': collection
                            })
                except Exception:
                    continue  # Skip collections without aliases or with errors
            
            return all_aliases
            
        except Exception as e:
            raise RuntimeError(f"List all aliases error: {e}") from e

    def validate_alias_name(self, aliasName):
        """
        Validate alias name format
        
        Args:
            aliasName: Name of the alias to validate
            
        Returns:
            bool: Whether alias name is valid
        """
        if not aliasName or not isinstance(aliasName, str):
            return False
        
        # Basic validation rules
        if len(aliasName) == 0 or len(aliasName) > 255:
            return False
        
        # Check for valid characters (letters, numbers, underscores, hyphens)
        import re
        if not re.match(r'^[a-zA-Z0-9_-]+$', aliasName):
            return False
        
        return True

    def create_multiple_aliases(self, collectionName, aliasNames):
        """
        Create multiple aliases for a collection
        
        Args:
            collectionName: Name of the collection
            aliasNames: List of alias names to create
            
        Returns:
            List of results for each alias creation
        """
        results = []
        
        for aliasName in aliasNames:
            try:
                result = self.create_alias(collectionName, aliasName)
                results.append({
                    'alias': aliasName,
                    'status': 'success',
                    'message': result
                })
            except Exception as e:
                results.append({
                    'alias': aliasName,
                    'status': 'error',
                    'message': str(e)
                })
        
        return results

    def drop_multiple_aliases(self, aliasNames):
        """
        Drop multiple aliases
        
        Args:
            aliasNames: List of alias names to drop
            
        Returns:
            List of results for each alias deletion
        """
        results = []
        
        for aliasName in aliasNames:
            try:
                result = self.drop_alias(aliasName)
                results.append({
                    'alias': aliasName,
                    'status': 'success',
                    'message': result
                })
            except Exception as e:
                results.append({
                    'alias': aliasName,
                    'status': 'error',
                    'message': str(e)
                })
        
        return results
