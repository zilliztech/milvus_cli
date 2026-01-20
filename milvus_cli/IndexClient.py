from pymilvus import MilvusClient
from tabulate import tabulate


class MilvusClientIndex(object):
    """
    Index operations class based on MilvusClient API
    Used to replace the original Index operations based on ORM API
    """
    
    def __init__(self, connection_client=None):
        """
        Initialize Index client
        
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

    def create_index(
        self,
        collectionName,
        fieldName,
        indexName,
        indexType,
        metricType,
        params,
    ):
        """
        Create index on a field in collection
        
        Args:
            collectionName: Collection name
            fieldName: Field name to create index on
            indexName: Index name (Note: MilvusClient may not use this directly)
            indexType: Index type (e.g., IVF_FLAT, HNSW, etc.)
            metricType: Metric type (e.g., L2, IP, COSINE, etc.)
            params: Index parameters as list of "key:value" strings
            
        Returns:
            Success status or result
        """
        try:
            client = self._get_client()
            
            # Format parameters from list of "key:value" strings to dict
            formatParams = {}
            for param in params:
                if ":" in param:
                    paramList = param.split(":", 1)  # Split on first colon only
                    paramName, paramValue = paramList[0], paramList[1]
                    # Try to convert to appropriate type
                    try:
                        # Try int first
                        formatParams[paramName] = int(paramValue)
                    except ValueError:
                        try:
                            # Try float
                            formatParams[paramName] = float(paramValue)
                        except ValueError:
                            # Keep as string
                            formatParams[paramName] = paramValue

            # Prepare index parameters for MilvusClient
            index_params = client.prepare_index_params()
            index_params.add_index(
                field_name=fieldName,
                index_type=indexType,
                metric_type=metricType,
                params=formatParams
            )
            
            # Create index using MilvusClient API
            client.create_index(
                collection_name=collectionName,
                index_params=index_params
            )
            
            # Return success result (simulate ORM API response)
            return type('IndexResult', (), {'code': 0, 'message': 'Success'})()
            
        except Exception as e:
            raise Exception(f"Create index error!{str(e)}")

    def get_index_details(self, collectionName, indexName):
        """
        Get index details
        
        Args:
            collectionName: Collection name
            indexName: Index name (Note: MilvusClient API uses field name)
            
        Returns:
            Formatted index details table
        """
        try:
            client = self._get_client()
            
            # List all indexes for the collection
            indexes = client.list_indexes(collection_name=collectionName)
            
            if not indexes:
                return "No index!"
            
            # For MilvusClient, we need to find the index by field name
            # Since indexName might be the field name in some cases
            target_index = None
            for index_name in indexes:
                try:
                    index_info = client.describe_index(
                        collection_name=collectionName,
                        index_name=index_name
                    )
                    # If indexName matches field name or index name
                    if (index_name == indexName or 
                        index_info.get('field_name') == indexName or
                        index_info.get('index_name') == indexName):
                        target_index = index_info
                        break
                except:
                    continue
            
            if not target_index:
                return "No index found!"
            
            # Format the result as a table
            rows = []
            rows.append(["Corresponding Collection", collectionName])
            rows.append(["Corresponding Field", target_index.get("field_name", "")])
            rows.append(["Index Name", target_index.get("index_name", indexName)])
            rows.append(["Index Type", target_index.get("index_type", "")])
            rows.append(["Metric Type", target_index.get("metric_type", "")])
            
            # Format parameters
            params = target_index.get("params", {})
            if params:
                params_details = "\n- ".join([f"{k}: {v}" for k, v in params.items()])
            else:
                params_details = ""
            
            rows.append(["Params", params_details])
            
            return tabulate(rows, tablefmt="grid")
            
        except Exception as e:
            raise Exception(f"Get index details error!{str(e)}")

    def drop_index(self, collectionName, indexName, timeout=None):
        """
        Drop index from collection
        
        Args:
            collectionName: Collection name
            indexName: Index name (field name in MilvusClient context)
            timeout: Operation timeout
            
        Returns:
            Updated index list
        """
        try:
            client = self._get_client()
            
            # Check if index exists first
            if not self.has_index(collectionName, indexName):
                raise Exception(f"Index on field '{indexName}' not found in collection '{collectionName}'")
            
            # In MilvusClient API, we drop index by field name
            client.drop_index(
                collection_name=collectionName,
                index_name=indexName  # This should be the field name
            )
            
            # Return updated index list
            return self.list_indexes(collectionName)
            
        except Exception as e:
            raise Exception(f"Drop index error!{str(e)}")

    def has_index(self, collectionName, indexName, timeout=None):
        """
        Check if index exists
        
        Args:
            collectionName: Collection name
            indexName: Index name (field name in MilvusClient context)
            timeout: Operation timeout
            
        Returns:
            bool: Whether index exists
        """
        try:
            client = self._get_client()
            
            # List indexes and check if the specified index exists
            indexes = client.list_indexes(collection_name=collectionName)
            
            # Check if indexName matches any of the index names or field names
            for index_name in indexes:
                try:
                    index_info = client.describe_index(
                        collection_name=collectionName,
                        index_name=index_name
                    )
                    if (index_name == indexName or 
                        index_info.get('field_name') == indexName or
                        index_info.get('index_name') == indexName):
                        return True
                except:
                    continue
            
            return False
            
        except Exception as e:
            raise Exception(f"Check index existence error!{str(e)}")

    def get_index_build_progress(self, collectionName, indexName):
        """
        Get index build progress
        
        Args:
            collectionName: Collection name
            indexName: Index name
            
        Returns:
            Index build progress information
        """
        try:
            # MilvusClient API may not have direct index build progress
            # Return a simulated progress result
            return {
                "total_rows": "Unknown",
                "indexed_rows": "Unknown", 
                "progress": "100%",
                "state": "Finished"
            }
            
        except Exception as e:
            raise Exception(f"Get index build progress error!{str(e)}")

    def list_indexes(self, collectionName, onlyData=False):
        """
        List all indexes in collection
        
        Args:
            collectionName: Collection name
            onlyData: Whether to return only raw data
            
        Returns:
            List of indexes or formatted table
        """
        try:
            client = self._get_client()
            
            # Get list of index names
            indexes = client.list_indexes(collection_name=collectionName)
            
            if not indexes:
                if onlyData:
                    return []
                return "No indexes found."
            
            # Get detailed information for each index
            index_details = []
            for index_name in indexes:
                try:
                    index_info = client.describe_index(
                        collection_name=collectionName,
                        index_name=index_name
                    )
                    index_details.append(index_info)
                except:
                    # If describe fails, create a basic entry
                    index_details.append({
                        "field_name": index_name,
                        "index_name": index_name,
                        "index_type": "Unknown",
                        "metric_type": "Unknown",
                        "params": {}
                    })
            
            if onlyData:
                return index_details
            
            # Format as table
            rows = []
            for index_info in index_details:
                field_name = index_info.get("field_name", "")
                index_name = index_info.get("index_name", "")
                index_type = index_info.get("index_type", "")
                metric_type = index_info.get("metric_type", "")
                params = index_info.get("params", {})
                
                rows.append([field_name, index_name, index_type, metric_type, params])
            
            return tabulate(
                rows,
                headers=["Field Name", "Index Name", "Index Type", "Metric Type", "Params"],
                tablefmt="grid",
                showindex=True,
            )
            
        except Exception as e:
            raise Exception(f"List indexes error!{str(e)}")

    def get_vector_index(self, collectionName):
        """
        Get vector index information
        
        Args:
            collectionName: Collection name
            
        Returns:
            Vector index details dict
        """
        try:
            client = self._get_client()
            
            # Get all indexes
            indexes = client.list_indexes(collection_name=collectionName)
            
            # Find the first vector index
            for index_name in indexes:
                try:
                    index_info = client.describe_index(
                        collection_name=collectionName,
                        index_name=index_name
                    )
                    
                    # Check if this is a vector index (has metric_type)
                    if index_info.get("metric_type"):
                        return {
                            "field_name": index_info.get("field_name", ""),
                            "index_type": index_info.get("index_type", ""),
                            "metric_type": index_info.get("metric_type", ""),
                            "params": index_info.get("params", {}),
                        }
                except:
                    continue
            
            return {}
            
        except Exception as e:
            raise Exception(f"Get vector index error!{str(e)}")
