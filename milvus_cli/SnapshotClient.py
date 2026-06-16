from __future__ import annotations

try:
    from .BaseClient import BaseMilvusClient
except ImportError:
    from BaseClient import BaseMilvusClient


class MilvusSnapshot(BaseMilvusClient):
    def create_snapshot(self, snapshot_name):
        try:
            client = self._get_client()
            client.create_snapshot(snapshot_name=snapshot_name)
            return f"Create snapshot {snapshot_name} successfully!"
        except Exception as e:
            raise RuntimeError(f"Create snapshot error: {e}") from e

    def drop_snapshot(self, snapshot_name):
        try:
            client = self._get_client()
            client.drop_snapshot(snapshot_name=snapshot_name)
            return f"Drop snapshot {snapshot_name} successfully!"
        except Exception as e:
            raise RuntimeError(f"Drop snapshot error: {e}") from e

    def describe_snapshot(self, snapshot_name):
        try:
            client = self._get_client()
            result = client.describe_snapshot(snapshot_name=snapshot_name)
            return result
        except Exception as e:
            raise RuntimeError(f"Describe snapshot error: {e}") from e

    def list_snapshots(self):
        try:
            client = self._get_client()
            result = client.list_snapshots()
            return result
        except Exception as e:
            raise RuntimeError(f"List snapshots error: {e}") from e

    def restore_snapshot(self, snapshot_name):
        try:
            client = self._get_client()
            client.restore_snapshot(snapshot_name=snapshot_name)
            return f"Restore snapshot {snapshot_name} successfully!"
        except Exception as e:
            raise RuntimeError(f"Restore snapshot error: {e}") from e

    def get_restore_snapshot_state(self, snapshot_name):
        try:
            client = self._get_client()
            result = client.get_restore_snapshot_state(snapshot_name=snapshot_name)
            return result
        except Exception as e:
            raise RuntimeError(f"Get restore snapshot state error: {e}") from e

    def list_restore_snapshot_jobs(self):
        try:
            client = self._get_client()
            result = client.list_restore_snapshot_jobs()
            return result
        except Exception as e:
            raise RuntimeError(f"List restore snapshot jobs error: {e}") from e

    def pin_snapshot_data(self, snapshot_name):
        try:
            client = self._get_client()
            client.pin_snapshot_data(snapshot_name=snapshot_name)
            return f"Pin snapshot {snapshot_name} data successfully!"
        except Exception as e:
            raise RuntimeError(f"Pin snapshot data error: {e}") from e

    def unpin_snapshot_data(self, snapshot_name):
        try:
            client = self._get_client()
            client.unpin_snapshot_data(snapshot_name=snapshot_name)
            return f"Unpin snapshot {snapshot_name} data successfully!"
        except Exception as e:
            raise RuntimeError(f"Unpin snapshot data error: {e}") from e
