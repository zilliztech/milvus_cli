from __future__ import annotations

from tabulate import tabulate
try:
    from .BaseClient import BaseMilvusClient
    from .utils import safe_int
except ImportError:
    from BaseClient import BaseMilvusClient
    from utils import safe_int


class MilvusClientData(BaseMilvusClient):
    """Data operations based on MilvusClient API."""

    def _normalize_data(self, client, collectionName: str, data) -> list[dict]:
        """Normalize input data to list of dicts, handling auto_id fields."""
        if isinstance(data, dict):
            return [data]
        if isinstance(data, list) and data and isinstance(data[0], list):
            collection_info = client.describe_collection(collection_name=collectionName)
            fields = collection_info.get("fields", [])
            # Exclude auto_id fields — user data won't contain them
            field_names = [
                f.get("name", "") for f in fields
                if not f.get("auto_id", False)
            ]
            num_entities = len(data[0])
            formatted = []
            for i in range(num_entities):
                entity = {}
                for j, name in enumerate(field_names):
                    if j < len(data):
                        entity[name] = data[j][i]
                formatted.append(entity)
            return formatted
        return data

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
            data = self._normalize_data(client, collectionName, data)
            
            # Insert data
            result = client.insert(
                collection_name=collectionName,
                data=data,
                partition_name=partitionName,
                timeout=timeout
            )
            
            return result
            
        except Exception as e:
            raise RuntimeError(f"Insert data error: {e}") from e

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
            output_fields = queryParameters.get("output_fields")
            partition_names = queryParameters.get("partition_names")
            timeout = queryParameters.get("timeout")

            # Handle output_fields - use default if None or empty
            if not output_fields:
                output_fields = ["*"]
            else:
                # Clean up field names in case user entered brackets/quotes
                cleaned_fields = []
                for field in output_fields:
                    # Strip brackets, quotes, and whitespace
                    field = str(field).strip().strip("[]'\"")
                    if field:
                        cleaned_fields.append(field)
                output_fields = cleaned_fields if cleaned_fields else ["*"]

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
            raise RuntimeError(f"Query data error: {e}") from e

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
            raise RuntimeError(f"Delete entities error: {e}") from e

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
            output_fields = searchParameters.get("output_fields")
            round_decimal = searchParameters.get("round_decimal", 4)

            # Handle output_fields - use default if None or empty
            if not output_fields:
                output_fields = ["*"]
            else:
                # Clean up field names in case user entered brackets/quotes
                cleaned_fields = []
                for field in output_fields:
                    # Strip brackets, quotes, and whitespace
                    field = str(field).strip().strip("[]'\"")
                    if field:
                        cleaned_fields.append(field)
                output_fields = cleaned_fields if cleaned_fields else ["*"]
            
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
            raise RuntimeError(f"Search data error: {e}") from e

    def get_entity_count(self, collectionName: str, partitionName: str | None = None) -> int:
        """Get entity count in collection or partition."""
        try:
            client = self._get_client()
            if partitionName:
                stats = client.get_partition_stats(
                    collection_name=collectionName,
                    partition_name=partitionName
                )
            else:
                stats = client.get_collection_stats(collection_name=collectionName)
            return safe_int(stats.get("row_count", 0))
        except Exception as e:
            raise RuntimeError(f"Get entity count error: {e}") from e

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
            data = self._normalize_data(client, collectionName, data)
            
            # Upsert data
            result = client.upsert(
                collection_name=collectionName,
                data=data,
                partition_name=partitionName,
                timeout=timeout
            )
            
            return result

        except Exception as e:
            raise RuntimeError(f"Upsert data error: {e}") from e

    def get_by_ids(self, collectionName, ids, output_fields=None):
        """
        Get entities by IDs

        Args:
            collectionName: Collection name
            ids: List of entity IDs
            output_fields: Fields to return

        Returns:
            Query results
        """
        try:
            client = self._get_client()
            result = client.get(
                collection_name=collectionName,
                ids=ids,
                output_fields=output_fields
            )
            return result
        except Exception as e:
            raise RuntimeError(f"Get by IDs error: {e}") from e

    def delete_by_ids(self, collectionName, ids, partition_name=None):
        """
        Delete entities by IDs

        Args:
            collectionName: Collection name
            ids: List of entity IDs
            partition_name: Partition name

        Returns:
            Delete result
        """
        try:
            client = self._get_client()
            result = client.delete(
                collection_name=collectionName,
                ids=ids,
                partition_name=partition_name
            )
            return result
        except Exception as e:
            raise RuntimeError(f"Delete by IDs error: {e}") from e

    def bulk_insert(self, collectionName, files, partition_name=None):
        """
        Bulk insert data from files

        Args:
            collectionName: Collection name
            files: List of file paths
            partition_name: Partition name

        Returns:
            Task ID
        """
        try:
            client = self._get_client()
            task_id = client.bulk_insert(
                collection_name=collectionName,
                partition_name=partition_name,
                files=files
            )
            return task_id
        except Exception as e:
            raise RuntimeError(f"Bulk insert error: {e}") from e

    def get_bulk_insert_state(self, task_id):
        """
        Get bulk insert task state

        Args:
            task_id: Task ID

        Returns:
            Task state
        """
        try:
            client = self._get_client()
            state = client.get_bulk_insert_state(task_id)
            return state
        except Exception as e:
            raise RuntimeError(f"Get bulk insert state error: {e}") from e

    def list_bulk_insert_tasks(self, limit=None, collectionName=None):
        """
        List bulk insert tasks

        Args:
            limit: Maximum number of tasks to return
            collectionName: Filter by collection name

        Returns:
            List of tasks
        """
        try:
            client = self._get_client()
            tasks = client.list_bulk_insert_tasks(
                limit=limit,
                collection_name=collectionName
            )
            return tasks
        except Exception as e:
            raise RuntimeError(f"List bulk insert tasks error: {e}") from e

    def hybrid_search(self, collectionName, requests, rerank, limit, output_fields=None):
        """
        Perform hybrid search (multi-vector search) with reranking.

        Args:
            collectionName: Collection name
            requests: List of AnnSearchRequest objects
            rerank: Reranker object (WeightedRanker or RRFRanker)
            limit: Maximum number of results to return
            output_fields: List of fields to return

        Returns:
            Formatted search results
        """
        try:
            client = self._get_client()

            # Handle output_fields - use default if None or empty
            if not output_fields:
                output_fields = ["*"]
            else:
                # Clean up field names in case user entered brackets/quotes
                cleaned_fields = []
                for field in output_fields:
                    # Strip brackets, quotes, and whitespace
                    field = str(field).strip().strip("[]'\"")
                    if field:
                        cleaned_fields.append(field)
                output_fields = cleaned_fields if cleaned_fields else ["*"]

            # Perform hybrid search using MilvusClient API
            results = client.hybrid_search(
                collection_name=collectionName,
                reqs=requests,
                ranker=rerank,
                limit=limit,
                output_fields=output_fields
            )

            # Format results as table
            if results and len(results) > 0:
                table_rows = []
                for hit in results[0]:
                    if isinstance(hit, dict):
                        entity_id = hit.get("id", "N/A")
                        distance = hit.get("distance", "N/A")
                        fields = {k: v for k, v in hit.items() if k not in ["id", "distance"]}
                    else:
                        entity_id = hit.id if hasattr(hit, 'id') else "N/A"
                        distance = hit.distance if hasattr(hit, 'distance') else "N/A"
                        fields = hit.fields if hasattr(hit, 'fields') else {}
                    if isinstance(distance, (int, float)):
                        distance = round(distance, 4)
                    table_rows.append([entity_id, distance, str(fields)])

                return tabulate(
                    table_rows,
                    headers=["ID", "Distance", "Fields"],
                    tablefmt="grid",
                )
            else:
                return tabulate([], headers=["ID", "Distance", "Fields"], tablefmt="grid")

        except Exception as e:
            raise RuntimeError(f"Hybrid search error: {e}") from e
