from __future__ import annotations

try:
    from .BaseClient import BaseMilvusClient
except ImportError:
    from BaseClient import BaseMilvusClient


class MilvusPrivilegeGroup(BaseMilvusClient):
    """Privilege group operations based on MilvusClient API."""

    def create_privilege_group(self, group_name):
        """Create a privilege group."""
        try:
            client = self._get_client()
            client.create_privilege_group(group_name)
            return f"Create privilege group {group_name} successfully!"
        except Exception as e:
            raise RuntimeError(f"Create privilege group error: {e}") from e

    def list_privilege_groups(self):
        """List all privilege groups."""
        try:
            client = self._get_client()
            result = client.list_privilege_groups()
            return result
        except Exception as e:
            raise RuntimeError(f"List privilege groups error: {e}") from e

    def add_privileges_to_group(self, group_name, privileges):
        """Add privileges to a privilege group."""
        try:
            client = self._get_client()
            client.add_privileges_to_group(group_name, privileges)
            return f"Add privileges to group {group_name} successfully!"
        except Exception as e:
            raise RuntimeError(f"Add privileges to group error: {e}") from e

    def remove_privileges_from_group(self, group_name, privileges):
        """Remove privileges from a privilege group."""
        try:
            client = self._get_client()
            client.remove_privileges_from_group(group_name, privileges)
            return f"Remove privileges from group {group_name} successfully!"
        except Exception as e:
            raise RuntimeError(f"Remove privileges from group error: {e}") from e

    def drop_privilege_group(self, group_name):
        """Drop a privilege group."""
        try:
            client = self._get_client()
            client.drop_privilege_group(group_name)
            return f"Drop privilege group {group_name} successfully!"
        except Exception as e:
            raise RuntimeError(f"Drop privilege group error: {e}") from e
