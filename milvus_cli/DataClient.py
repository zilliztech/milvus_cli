from pymilvus import MilvusClient
from tabulate import tabulate


class MilvusClientData:
    """
    Data operations class based on MilvusClient API
    Used to replace the original Data operations based on ORM API
    """
    
    def __init__(self, connection_client=None):
        """
        Initialize Data client
        
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

    def insert(self, collectionName, data, partitionName=None, timeout=None):
        """
        Insert data into collection
        
        Args:
            collectionName: Collection name
            data: Data to insert (list of dicts or list of lists)
            partitionName: Partition name
            timeout: Timeout value
            
        Returns:
            Insert result
        """
        try:
            client = self._get_client()
            
            # Handle different data formats
            if data and isinstance(data[0], list):
                # Convert list of lists to list of dicts format
                # This requires field schema information
                collection_info = client.describe_collection(collection_name=collectionName)
                fields = collection_info.get("fields", [])
                field_names = [field.get("name", "") for field in fields]
                
                # Convert data format
                formatted_data = []
                for i in range(len(data[0])):  # Number of entities
                    entity = {}
                    for j, field_name in enumerate(field_names):
                        if j < len(data):
                            entity[field_name] = data[j][i]
                    formatted_data.append(entity)
                data = formatted_data
            
            # Insert data
            result = client.insert(
                collection_name=collectionName,
                data=data,
                partition_name=partitionName,
                timeout=timeout
            )
            
            return result
            
        except Exception as e:
            raise Exception(f"Insert data error!{str(e)}")

    def query(self, collectionName, queryParameters):
        """
        Query data from collection
        
        Args:
            collectionName: Collection name
            queryParameters: Query parameters dict
            
        Returns:
            Query results
        """
        try:
            client = self._get_client()
            
            # Extract parameters
            expr = queryParameters.get("expr")
            output_fields = queryParameters.get("output_fields", ["*"])
            partition_names = queryParameters.get("partition_names")
            timeout = queryParameters.get("timeout")
            
            # Perform query
            result = client.query(
                collection_name=collectionName,
                filter=expr,
                output_fields=output_fields,
                partition_names=partition_names,
                timeout=timeout
            )
            
            return result
            
        except Exception as e:
            raise Exception(f"Query data error!{str(e)}")

    def delete_entities(self, expr, collectionName, partition_name=None):
        """
        Delete entities from collection
        
        Args:
            expr: Delete expression
            collectionName: Collection name
            partition_name: Partition name
            
        Returns:
            Delete result
        """
        try:
            client = self._get_client()
            
            result = client.delete(
                collection_name=collectionName,
                filter=expr,
                partition_name=partition_name
            )
            
            return result
            
        except Exception as e:
            raise Exception(f"Delete entities error!{str(e)}")

    def search(self, collectionName, searchParameters, prettierFormat=True):
        """
        Search vectors in collection
        
        Args:
            collectionName: Collection name
            searchParameters: Search parameters dict
            prettierFormat: Whether to format output as table
            
        Returns:
            Search results (formatted table if prettierFormat=True)
        """
        try:
            client = self._get_client()
            
            # Extract parameters
            data = searchParameters.get("data")
            anns_field = searchParameters.get("anns_field")
            search_params = searchParameters.get("param", {})
            limit = searchParameters.get("limit", 10)
            output_fields = searchParameters.get("output_fields", ["*"])
            round_decimal = searchParameters.get("round_decimal", 4)
            
            # Perform search
            result = client.search(
                collection_name=collectionName,
                data=data,
                anns_field=anns_field,
                search_params=search_params,
                limit=limit,
                output_fields=output_fields
            )
            
            if not prettierFormat:
                return result
                
            # Format results as table
            if result and len(result) > 0:
                hits = result[0]  # Only support search by single vector
                
                # Build table rows
                table_rows = []
                for hit in hits:
                    # Handle MilvusClient result format
                    if isinstance(hit, dict):
                        entity_id = hit.get("id", "N/A")
                        distance = hit.get("distance", "N/A")
                        entity_data = {k: v for k, v in hit.items() if k not in ["id", "distance"]}
                        entity_str = str(entity_data) if entity_data else "{}"
                    else:
                        # Handle old format for backward compatibility
                        entity_str = str(getattr(hit, "entity", {}))
                        if hasattr(hit, 'id'):
                            entity_id = hit.id
                        elif hasattr(hit, 'get') and 'id' in hit:
                            entity_id = hit['id']
                        else:
                            entity_id = "N/A"
                        
                        if hasattr(hit, 'distance'):
                            distance = hit.distance
                        elif hasattr(hit, 'get') and 'distance' in hit:
                            distance = hit['distance']
                        else:
                            distance = "N/A"
                    
                    # Round distance if it's a number
                    if isinstance(distance, (int, float)):
                        distance = round(distance, round_decimal)
                    
                    table_rows.append([entity_id, distance, entity_str])
                
                return tabulate(
                    table_rows,
                    headers=["ID", "Distance", "Entity"],
                    tablefmt="grid",
                )
            else:
                return tabulate([], headers=["ID", "Distance", "Entity"], tablefmt="grid")
                
        except Exception as e:
            raise Exception(f"Search data error!{str(e)}")

    def get_entity_count(self, collectionName, partitionName=None):
        """
        Get entity count in collection
        
        Args:
            collectionName: Collection name
            partitionName: Partition name
            
        Returns:
            Entity count
        """
        try:
            client = self._get_client()
            
            if partitionName:
                # Get partition stats
                stats = client.get_partition_stats(
                    collection_name=collectionName,
                    partition_name=partitionName
                )
            else:
                # Get collection stats
                stats = client.get_collection_stats(collection_name=collectionName)
            
            raw_count = stats.get("row_count", 0)
            
            # Handle cases where row_count might be string 'null' or other unexpected values
            if isinstance(raw_count, str):
                if raw_count.lower() == 'null' or raw_count == '':
                    return 0
                else:
                    try:
                        return int(raw_count)
                    except ValueError:
                        return 0
            
            return raw_count if raw_count is not None else 0
            
        except Exception as e:
            raise Exception(f"Get entity count error!{str(e)}")

    def upsert(self, collectionName, data, partitionName=None, timeout=None):
        """
        Upsert data into collection
        
        Args:
            collectionName: Collection name
            data: Data to upsert
            partitionName: Partition name
            timeout: Timeout value
            
        Returns:
            Upsert result
        """
        try:
            client = self._get_client()
            
            # Handle different data formats (similar to insert)
            if data and isinstance(data[0], list):
                # Convert list of lists to list of dicts format
                collection_info = client.describe_collection(collection_name=collectionName)
                fields = collection_info.get("fields", [])
                field_names = [field.get("name", "") for field in fields]
                
                # Convert data format
                formatted_data = []
                for i in range(len(data[0])):  # Number of entities
                    entity = {}
                    for j, field_name in enumerate(field_names):
                        if j < len(data):
                            entity[field_name] = data[j][i]
                    formatted_data.append(entity)
                data = formatted_data
            
            # Upsert data
            result = client.upsert(
                collection_name=collectionName,
                data=data,
                partition_name=partitionName,
                timeout=timeout
            )
            
            return result
            
        except Exception as e:
            raise Exception(f"Upsert data error!{str(e)}")
