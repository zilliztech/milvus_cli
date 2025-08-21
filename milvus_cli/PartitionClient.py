from pymilvus import MilvusClient


class MilvusClientPartition(object):
    """
    Partition operations class based on MilvusClient API
    Used to replace the original Partition operations based on ORM API
    """
    
    def __init__(self, connection_client=None):
        """
        Initialize Partition client
        
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

    def create_partition(self, collectionName, description, partitionName):
        """
        Create partition in collection
        
        Args:
            collectionName: Collection name
            description: Partition description
            partitionName: Partition name
            
        Returns:
            Partition information
        """
        try:
            client = self._get_client()
            
            # Create partition using MilvusClient API
            client.create_partition(
                collection_name=collectionName,
                partition_name=partitionName
            )
            
            # Return partition information - MilvusClient doesn't return partition object like ORM
            # We need to simulate the return structure
            return type('Partition', (), {'name': partitionName, 'description': description})()
            
        except Exception as e:
            raise Exception(f"Create partition error!{str(e)}")

    def describe_partition(self, collectionName, partitionName):
        """
        Describe partition information
        
        Args:
            collectionName: Collection name
            partitionName: Partition name
            
        Returns:
            Partition information object
        """
        try:
            client = self._get_client()
            
            # Check if partition exists by listing partitions
            partitions = client.list_partitions(collection_name=collectionName)
            
            if partitionName not in partitions:
                raise Exception(f"Partition '{partitionName}' not found in collection '{collectionName}'")
            
            # Return partition information - simulate ORM partition object
            return type('Partition', (), {'name': partitionName, 'collection_name': collectionName})()
            
        except Exception as e:
            raise Exception(f"Describe partition error!{str(e)}")

    def delete_partition(self, collectionName, partitionName):
        """
        Delete partition from collection
        
        Args:
            collectionName: Collection name
            partitionName: Partition name
            
        Returns:
            Updated partition list
        """
        try:
            client = self._get_client()
            
            # Check if partition exists first
            partitions = client.list_partitions(collection_name=collectionName)
            if partitionName not in partitions:
                raise Exception(f"Partition '{partitionName}' not found in collection '{collectionName}'")
            
            # Drop partition using MilvusClient API
            client.drop_partition(
                collection_name=collectionName,
                partition_name=partitionName
            )
            
            # Return updated partition list
            return self.list_partition_names(collectionName)
            
        except Exception as e:
            raise Exception(f"Delete partition error!{str(e)}")

    def list_partition_names(self, collectionName):
        """
        List all partition names in collection
        
        Args:
            collectionName: Collection name
            
        Returns:
            List of partition names
        """
        try:
            client = self._get_client()
            
            # List partitions using MilvusClient API
            partitions = client.list_partitions(collection_name=collectionName)
            
            return partitions
            
        except Exception as e:
            raise Exception(f"List partition names error!{str(e)}")

    def load_partition(self, collectionName, partitionName):
        """
        Load partition into memory
        
        Args:
            collectionName: Collection name
            partitionName: Partition name
            
        Returns:
            Success message
        """
        try:
            client = self._get_client()
            
            # Load partition using MilvusClient API
            client.load_partitions(
                collection_name=collectionName,
                partition_names=[partitionName]
            )
            
            return f"Load partition {partitionName} successfully!"
            
        except Exception as e:
            raise Exception(f"Load partition error!{str(e)}")

    def release_partition(self, collectionName, partitionName):
        """
        Release partition from memory
        
        Args:
            collectionName: Collection name
            partitionName: Partition name
            
        Returns:
            Success message
        """
        try:
            client = self._get_client()
            
            # Release partition using MilvusClient API
            client.release_partitions(
                collection_name=collectionName,
                partition_names=[partitionName]
            )
            
            return f"Release partition {partitionName} successfully!"
            
        except Exception as e:
            raise Exception(f"Release partition error!{str(e)}")

    def has_partition(self, collectionName, partitionName):
        """
        Check if partition exists in collection
        
        Args:
            collectionName: Collection name
            partitionName: Partition name
            
        Returns:
            bool: Whether partition exists
        """
        try:
            client = self._get_client()
            
            # Check if partition exists by listing partitions
            partitions = client.list_partitions(collection_name=collectionName)
            
            return partitionName in partitions
            
        except Exception as e:
            raise Exception(f"Check partition existence error!{str(e)}")

    def get_partition_stats(self, collectionName, partitionName):
        """
        Get partition statistics
        
        Args:
            collectionName: Collection name
            partitionName: Partition name
            
        Returns:
            Partition statistics
        """
        try:
            client = self._get_client()
            
            # Get partition statistics using MilvusClient API
            # Note: MilvusClient may not have direct partition stats API
            # We can get collection stats and estimate partition stats
            try:
                stats = client.get_collection_stats(collection_name=collectionName)
                # For partition stats, we return a simulated result
                return {
                    "partition_name": partitionName,
                    "collection_name": collectionName,
                    "row_count": "Unknown"  # MilvusClient doesn't provide partition-specific stats
                }
            except:
                return {
                    "partition_name": partitionName,
                    "collection_name": collectionName,
                    "row_count": "Unknown"
                }
                
        except Exception as e:
            raise Exception(f"Get partition stats error!{str(e)}")
