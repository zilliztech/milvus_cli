class MilvusResourceGroup(object):
    def __init__(self, connection_client=None):
        self.connection_client = connection_client

    def _get_client(self):
        """Get MilvusClient instance from connection client."""
        if self.connection_client is None:
            raise Exception("Connection client not initialized. Please connect first.")
        client = self.connection_client.get_client()
        if client is None:
            raise Exception("Not connected to Milvus. Please connect first.")
        return client

    def create_resource_group(self, name, config=None):
        """Create a resource group."""
        try:
            client = self._get_client()
            client.create_resource_group(name)
            # If config provided, update it immediately
            if config:
                client.update_resource_groups({name: config})
            return f"Create resource group {name} successfully!"
        except Exception as e:
            raise Exception(f"Create resource group error!{str(e)}")

    def list_resource_groups(self):
        """List all resource groups."""
        try:
            client = self._get_client()
            result = client.list_resource_groups()
            return result
        except Exception as e:
            raise Exception(f"List resource groups error!{str(e)}")

    def describe_resource_group(self, name):
        """Describe a resource group."""
        try:
            client = self._get_client()
            result = client.describe_resource_group(name)
            return result
        except Exception as e:
            raise Exception(f"Describe resource group error!{str(e)}")

    def drop_resource_group(self, name):
        """Drop a resource group."""
        try:
            client = self._get_client()
            client.drop_resource_group(name)
            return f"Drop resource group {name} successfully!"
        except Exception as e:
            raise Exception(f"Drop resource group error!{str(e)}")

    def update_resource_groups(self, configs):
        """Update resource groups configuration."""
        try:
            client = self._get_client()
            client.update_resource_groups(configs)
            return "Update resource groups successfully!"
        except Exception as e:
            raise Exception(f"Update resource groups error!{str(e)}")

    def transfer_replica(self, source_group, target_group, collection_name, num_replicas):
        """Transfer replicas between resource groups."""
        try:
            client = self._get_client()
            client.transfer_replica(
                source_group=source_group,
                target_group=target_group,
                collection_name=collection_name,
                num_replicas=num_replicas,
            )
            return f"Transfer {num_replicas} replica(s) from {source_group} to {target_group} successfully!"
        except Exception as e:
            raise Exception(f"Transfer replica error!{str(e)}")
