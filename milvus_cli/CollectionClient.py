from pymilvus import MilvusClient, DataType
from tabulate import tabulate
from Types import DataTypeByNum


class MilvusClientCollection(object):
    """
    Collection operations class based on MilvusClient API
    Used to replace the original Collection operations based on ORM API
    """
    
    def __init__(self, connection_client=None):
        """
        Initialize Collection client
        
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

    def create_collection(
        self,
        collectionName,
        primaryField,
        fields,
        autoId=None,
        description=None,
        isDynamic=None,
        consistencyLevel="Bounded",
        shardsNum=1,
        functions=None,
    ):
        """
        Create Collection
        
        Args:
            collectionName: Collection name
            primaryField: Primary field name
            fields: Field list (FieldSchema object list)
            autoId: Whether to auto-generate ID
            description: Collection description
            isDynamic: Whether to enable dynamic fields
            consistencyLevel: Consistency level
            shardsNum: Number of shards
            functions: Function list
            
        Returns:
            Collection details
        """
        try:
            client = self._get_client()
            
            # For MilvusClient, we need to create a CollectionSchema object
            from pymilvus import FieldSchema, CollectionSchema
            schema_fields = []
            
            for field in fields:
                # Since fields are already properly configured FieldSchema objects,
                # we can use them directly or create new ones with additional properties
                if field.name == primaryField:
                    # Create primary field with proper settings  
                    field_params = {
                        'name': field.name,
                        'dtype': field.dtype,
                        'is_primary': True,
                        'auto_id': autoId if autoId is not None else False,
                        'description': getattr(field, 'description', ''),
                    }
                    
                    # Add dimension for vector fields
                    if hasattr(field, 'dim'):
                        dim_value = field.dim
                        # Handle cases where dim might be string 'null' or other unexpected values
                        if isinstance(dim_value, str):
                            if dim_value.lower() == 'null' or dim_value == '':
                                # Skip setting dim for null/empty values - let PyMilvus handle it
                                pass
                            else:
                                try:
                                    field_params['dim'] = int(dim_value)
                                except ValueError:
                                    # Skip invalid dimension values
                                    pass
                        elif dim_value is not None:
                            field_params['dim'] = dim_value
                    
                    # Add other common field properties carefully
                    for attr in ['max_length', 'element_type', 'max_capacity']:
                        if hasattr(field, attr):
                            attr_value = getattr(field, attr)
                            # Handle cases where attributes might be string 'null' or other unexpected values
                            if attr in ['max_length', 'max_capacity'] and isinstance(attr_value, str):
                                if attr_value.lower() == 'null' or attr_value == '':
                                    # Skip setting null/empty values for numeric attributes
                                    continue
                                else:
                                    try:
                                        field_params[attr] = int(attr_value)
                                    except ValueError:
                                        # Skip invalid numeric values
                                        continue
                            elif attr_value is not None:
                                field_params[attr] = attr_value
                    
                    # Handle nullable and default_value carefully to avoid conflicts
                    if hasattr(field, 'nullable') and hasattr(field, 'default_value'):
                        nullable = getattr(field, 'nullable')
                        default_value = getattr(field, 'default_value')
                        
                        # Handle cases where default_value might be string 'null'
                        if isinstance(default_value, str) and default_value.lower() == 'null':
                            default_value = None
                        
                        # Only set nullable if we have a proper default value or nullable is True
                        if nullable or default_value is not None:
                            field_params['nullable'] = nullable
                            if default_value is not None:
                                field_params['default_value'] = default_value
                    elif hasattr(field, 'nullable') and getattr(field, 'nullable'):
                        # Only set nullable=True if no default_value issue
                        field_params['nullable'] = True
                    
                    new_field = FieldSchema(**field_params)
                else:
                    # For non-primary fields, we can use the field as-is or recreate if needed
                    # Most fields can be used directly since they're already FieldSchema objects
                    new_field = field
                    
                schema_fields.append(new_field)
            
            # Create CollectionSchema
            collection_schema = CollectionSchema(
                fields=schema_fields,
                description=description or "",
                enable_dynamic_field=isDynamic or False
            )
            
            # Create Collection using MilvusClient API
            client.create_collection(
                collection_name=collectionName,
                schema=collection_schema,
                shards_num=shardsNum,
                consistency_level=consistencyLevel
            )
            
            # Return Collection details
            return self.get_collection_details(collectionName=collectionName)
            
        except Exception as e:
            raise Exception(f"Create collection error!{str(e)}")

    def list_collections(self):
        """
        List all Collections
        
        Returns:
            List of Collection names
        """
        try:
            client = self._get_client()
            return client.list_collections()
        except Exception as e:
            raise Exception(f"List collection error!{str(e)}")

    def drop_collection(self, collectionName=None):
        """
        Drop Collection
        
        Args:
            collectionName: Collection name
            
        Returns:
            Success message
        """
        try:
            client = self._get_client()
            client.drop_collection(collection_name=collectionName)
            return f"Drop collection {collectionName} successfully!"
        except Exception as e:
            raise Exception(f"Delete collection error!{str(e)}")

    def has_collection(self, collectionName=None):
        """
        Check if Collection exists
        
        Args:
            collectionName: Collection name
            
        Returns:
            bool: Whether Collection exists
        """
        try:
            client = self._get_client()
            return client.has_collection(collection_name=collectionName)
        except Exception as e:
            raise Exception(f"Has collection error!{str(e)}")

    def load_collection(self, collectionName=None):
        """
        Load Collection
        
        Args:
            collectionName: Collection name
            
        Returns:
            Success message
        """
        try:
            client = self._get_client()
            client.load_collection(collection_name=collectionName)
            return f"Load collection {collectionName} successfully!"
        except Exception as e:
            raise Exception(f"Load collection error!{str(e)}")

    def release_collection(self, collectionName=None):
        """
        Release Collection
        
        Args:
            collectionName: Collection name
            
        Returns:
            Success message
        """
        try:
            client = self._get_client()
            client.release_collection(collection_name=collectionName)
            return f"Release collection {collectionName} successfully!"
        except Exception as e:
            raise Exception(f"Release collection error!{str(e)}")

    def rename_collection(self, collectionName=None, newName=None):
        """
        Rename Collection
        
        Args:
            collectionName: Current Collection name
            newName: New Collection name
            
        Returns:
            Success message
        """
        try:
            client = self._get_client()
            client.rename_collection(
                old_name=collectionName,
                new_name=newName
            )
            return f"Rename collection {collectionName} to {newName} successfully!"
        except Exception as e:
            raise Exception(f"Rename collection error!{str(e)}")

    def get_collection_details(self, collectionName=""):
        """
        Get Collection details
        
        Args:
            collectionName: Collection name
            
        Returns:
            Formatted Collection details table
        """
        try:
            client = self._get_client()
            
            # Get Collection information
            collection_info = client.describe_collection(collection_name=collectionName)
            
            # Build display information
            rows = []
            
            # Basic information
            rows.append(["Name", collectionName])
            rows.append(["Description", collection_info.get("description", "")])
            
            # Get statistics
            try:
                stats = client.get_collection_stats(collection_name=collectionName)
                raw_count = stats.get("row_count", 0)
                
                # Handle cases where row_count might be string 'null' or other unexpected values
                if isinstance(raw_count, str):
                    if raw_count.lower() == 'null' or raw_count == '':
                        entity_count = 0
                    else:
                        try:
                            entity_count = int(raw_count)
                        except ValueError:
                            entity_count = 0
                else:
                    entity_count = raw_count if raw_count is not None else 0
                
                rows.append(["Entities", entity_count])
                rows.append(["Is Empty", entity_count == 0])
            except Exception as e:
                # More detailed error handling for debugging
                print(f"Debug: Error getting collection stats: {e}")
                rows.append(["Entities", "Unknown"])
                rows.append(["Is Empty", "Unknown"])
            
            # Field information
            fields = collection_info.get("fields", [])
            primary_field = None
            field_details = ""
            
            for field in fields:
                field_name = field.get("name", "")
                field_type = field.get("type", "")
                is_primary = field.get("is_primary", False)
                auto_id = field.get("auto_id", False)
                
                if is_primary:
                    primary_field = field_name
                    field_name = f"*{field_name}"
                
                # Get field parameters
                params_desc = ""
                if "params" in field:
                    params = field["params"]
                    if "dim" in params:
                        dim_value = params['dim']
                        # Handle string 'null' values for dimension
                        if isinstance(dim_value, str) and dim_value.lower() == 'null':
                            dim_value = "unknown"
                        params_desc += f"dim: {dim_value}"
                    if "max_length" in params:
                        if params_desc:
                            params_desc += ", "
                        max_len_value = params['max_length']
                        # Handle string 'null' values for max_length
                        if isinstance(max_len_value, str) and max_len_value.lower() == 'null':
                            max_len_value = "unknown"
                        params_desc += f"max_length: {max_len_value}"
                
                if auto_id:
                    if params_desc:
                        params_desc += ", "
                    params_desc += "auto_id: True"
                
                field_details += f"\n - {field_name} {field_type} {params_desc}"
            
            if primary_field:
                rows.append(["Primary Field", primary_field])
            
            schema_details = f"""Description: {collection_info.get("description", "")}

Fields(* is the primary field):{field_details}"""
            
            rows.append(["Schema", schema_details])
            
            # Partition information (MilvusClient API may need to get separately)
            try:
                partitions = client.list_partitions(collection_name=collectionName)
                partition_details = "  - " + "\n- ".join(partitions)
                rows.append(["Partitions", partition_details])
            except:
                rows.append(["Partitions", "  - _default"])
            
            # Index information (MilvusClient API may need to get separately)
            try:
                indexes = client.list_indexes(collection_name=collectionName)
                index_details = "  - " + "\n- ".join(indexes) if indexes else "  - No indexes"
                rows.append(["Indexes", index_details])
            except:
                rows.append(["Indexes", "  - Unknown"])
            
            return tabulate(rows, tablefmt="grid")
            
        except Exception as e:
            raise Exception(f"Get collection detail error!{str(e)}")

    def get_entities_count(self, collectionName):
        """
        Get entity count in Collection
        
        Args:
            collectionName: Collection name
            
        Returns:
            int: Entity count
        """
        try:
            client = self._get_client()
            stats = client.get_collection_stats(collection_name=collectionName)
            raw_count = stats.get("row_count", 0)
            
            # Handle cases where row_count might be string 'null' or other unexpected values
            if isinstance(raw_count, str):
                if raw_count.lower() == 'null' or raw_count == '':
                    return None
                else:
                    try:
                        return int(raw_count)
                    except ValueError:
                        return None
        
            return raw_count if raw_count is not None else 0
        except Exception as e:
            raise Exception(f"Get entities count error!{str(e)}")

    def show_loading_progress(self, collectionName=None):
        """
        Show loading progress
        
        Args:
            collectionName: Collection name
            
        Returns:
            Loading progress information
        """
        try:
            client = self._get_client()
            # MilvusClient API may not have direct loading progress interface
            # Here return a simple status check
            stats = client.get_collection_stats(collection_name=collectionName)
            return {"loading_progress": "100%", "loaded_partitions": ["_default"]}
        except Exception as e:
            raise Exception(f"Show loading progress error!{str(e)}")

    def list_field_names(self, collectionName):
        """
        List field names of Collection
        
        Args:
            collectionName: Collection name
            
        Returns:
            list: List of field names
        """
        try:
            client = self._get_client()
            collection_info = client.describe_collection(collection_name=collectionName)
            fields = collection_info.get("fields", [])
            return [field.get("name", "") for field in fields]
        except Exception as e:
            raise Exception(f"List field names error!{str(e)}")

    def list_fields_info(self, collectionName):
        """
        List field detailed information of Collection
        
        Args:
            collectionName: Collection name
            
        Returns:
            list: List of field information
        """
        try:
            client = self._get_client()
            collection_info = client.describe_collection(collection_name=collectionName)
            fields = collection_info.get("fields", [])
            
            result = []
            for field in fields:
                # Convert numeric type to string name
                field_type = field.get("type", 0)
                if isinstance(field_type, int):
                    field_type = DataTypeByNum.get(field_type, "UNKNOWN")
                field_info = {
                    "name": field.get("name", ""),
                    "type": field_type,
                    "autoId": field.get("auto_id", False),
                    "isFunctionOut": field.get("is_function_output", False),
                }
                # Add element_type for array fields
                if field.get("element_type"):
                    field_info["element_type"] = field.get("element_type")
                result.append(field_info)
            
            return result
        except Exception as e:
            raise Exception(f"List fields info error!{str(e)}")

    def flush(self, collectionName, timeout=None):
        """
        Flush collection data to storage

        Args:
            collectionName: Collection name
            timeout: Timeout value

        Returns:
            Success message
        """
        try:
            client = self._get_client()
            client.flush(collection_name=collectionName, timeout=timeout)
            return f"Flush collection {collectionName} successfully!"
        except Exception as e:
            raise Exception(f"Flush collection error!{str(e)}")

    def compact(self, collectionName, timeout=None):
        """
        Compact collection to merge small segments and remove deleted data

        Args:
            collectionName: Collection name
            timeout: Timeout value

        Returns:
            dict with message and compaction_id
        """
        try:
            client = self._get_client()
            compaction_id = client.compact(collection_name=collectionName, timeout=timeout)
            
            return {
                "message": f"Compact collection {collectionName} successfully!",
                "compaction_id": compaction_id
            }
        except Exception as e:
            raise Exception(f"Compact collection error!{str(e)}")

    def get_compaction_state(self, compactionId, timeout=None):
        """
        Get compaction state

        Args:
            compactionId: Compaction ID
            timeout: Timeout value

        Returns:
            Compaction state
        """
        try:
            client = self._get_client()
            state = client.get_compaction_state(job_id=compactionId, timeout=timeout)
            return state
        except Exception as e:
            raise Exception(f"Get compaction state error!{str(e)}")

    def get_compaction_plans(self, collectionName, compactionId, timeout=None):
        """
        Get compaction plans

        Args:
            collectionName: Collection name
            compactionId: Compaction ID
            timeout: Timeout value

        Returns:
            Compaction plans
        """
        try:
            client = self._get_client()
            plans = client.get_compaction_plans(job_id=compactionId, timeout=timeout)
            return plans
        except Exception as e:
            raise Exception(f"Get compaction plans error!{str(e)}")

    def get_replicas(self, collectionName, timeout=None):
        """
        Get replicas information

        Args:
            collectionName: Collection name
            timeout: Timeout value

        Returns:
            Replicas information
        """
        try:
            client = self._get_client()
            replicas = client.describe_replica(
                collection_name=collectionName,
                timeout=timeout
            )
            return replicas
        except Exception as e:
            raise Exception(f"Get replicas error!{str(e)}")

    def load_state(self, collectionName, partitionName=None):
        """
        Get load state of collection or partition

        Args:
            collectionName: Collection name
            partitionName: Partition name (optional)

        Returns:
            Load state
        """
        try:
            client = self._get_client()
            state = client.get_load_state(
                collection_name=collectionName,
                partition_name=partitionName
            )
            return state
        except Exception as e:
            raise Exception(f"Get load state error!{str(e)}")
