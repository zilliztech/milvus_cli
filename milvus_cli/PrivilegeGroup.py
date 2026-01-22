class MilvusPrivilegeGroup(object):
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

    def create_privilege_group(self, group_name):
        """Create a privilege group."""
        try:
            client = self._get_client()
            client.create_privilege_group(group_name)
            return f"Create privilege group {group_name} successfully!"
        except Exception as e:
            raise Exception(f"Create privilege group error!{str(e)}")

    def list_privilege_groups(self):
        """List all privilege groups."""
        try:
            client = self._get_client()
            result = client.list_privilege_groups()
            return result
        except Exception as e:
            raise Exception(f"List privilege groups error!{str(e)}")

    def add_privileges_to_group(self, group_name, privileges):
        """Add privileges to a privilege group."""
        try:
            client = self._get_client()
            client.add_privileges_to_group(group_name, privileges)
            return f"Add privileges to group {group_name} successfully!"
        except Exception as e:
            raise Exception(f"Add privileges to group error!{str(e)}")

    def remove_privileges_from_group(self, group_name, privileges):
        """Remove privileges from a privilege group."""
        try:
            client = self._get_client()
            client.remove_privileges_from_group(group_name, privileges)
            return f"Remove privileges from group {group_name} successfully!"
        except Exception as e:
            raise Exception(f"Remove privileges from group error!{str(e)}")

    def drop_privilege_group(self, group_name):
        """Drop a privilege group."""
        try:
            client = self._get_client()
            client.drop_privilege_group(group_name)
            return f"Drop privilege group {group_name} successfully!"
        except Exception as e:
            raise Exception(f"Drop privilege group error!{str(e)}")
