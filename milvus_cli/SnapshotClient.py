from __future__ import annotations

try:
    from .BaseClient import BaseMilvusClient
except ImportError:
    from BaseClient import BaseMilvusClient


class MilvusSnapshot(BaseMilvusClient):
    def create_snapshot(self, snapshot_name, collection_name, description="", compaction_protection_seconds=0):
        try:
            client = self._get_client()
            client.create_snapshot(
                snapshot_name=snapshot_name,
                collection_name=collection_name,
                description=description,
                compaction_protection_seconds=compaction_protection_seconds,
            )
            return f"Create snapshot {snapshot_name} for collection {collection_name} successfully!"
        except Exception as e:
            raise RuntimeError(f"Create snapshot error: {e}") from e

    def drop_snapshot(self, snapshot_name, collection_name):
        try:
            client = self._get_client()
            client.drop_snapshot(snapshot_name=snapshot_name, collection_name=collection_name)
            return f"Drop snapshot {snapshot_name} successfully!"
        except Exception as e:
            raise RuntimeError(f"Drop snapshot error: {e}") from e

    def describe_snapshot(self, snapshot_name, collection_name):
        try:
            client = self._get_client()
            result = client.describe_snapshot(snapshot_name=snapshot_name, collection_name=collection_name)
            return result
        except Exception as e:
            raise RuntimeError(f"Describe snapshot error: {e}") from e

    def list_snapshots(self, collection_name=""):
        try:
            client = self._get_client()
            result = client.list_snapshots(collection_name=collection_name)
            return result
        except Exception as e:
            raise RuntimeError(f"List snapshots error: {e}") from e

    def restore_snapshot(self, snapshot_name, source_collection_name, target_collection_name):
        try:
            client = self._get_client()
            job_id = client.restore_snapshot(
                snapshot_name=snapshot_name,
                source_collection_name=source_collection_name,
                target_collection_name=target_collection_name,
            )
            return f"Restore snapshot {snapshot_name} started. Job ID: {job_id}"
        except Exception as e:
            raise RuntimeError(f"Restore snapshot error: {e}") from e

    def get_restore_snapshot_state(self, job_id):
        try:
            client = self._get_client()
            result = client.get_restore_snapshot_state(job_id=job_id)
            return result
        except Exception as e:
            raise RuntimeError(f"Get restore snapshot state error: {e}") from e

    def list_restore_snapshot_jobs(self, collection_name=""):
        try:
            client = self._get_client()
            result = client.list_restore_snapshot_jobs(collection_name=collection_name)
            return result
        except Exception as e:
            raise RuntimeError(f"List restore snapshot jobs error: {e}") from e

    def pin_snapshot_data(self, snapshot_name, collection_name, ttl_seconds=0):
        try:
            client = self._get_client()
            pin_id = client.pin_snapshot_data(
                snapshot_name=snapshot_name,
                collection_name=collection_name,
                ttl_seconds=ttl_seconds,
            )
            return f"Pin snapshot {snapshot_name} data successfully. Pin ID: {pin_id}"
        except Exception as e:
            raise RuntimeError(f"Pin snapshot data error: {e}") from e

    def unpin_snapshot_data(self, pin_id):
        try:
            client = self._get_client()
            client.unpin_snapshot_data(pin_id=pin_id)
            return f"Unpin snapshot data (pin_id={pin_id}) successfully!"
        except Exception as e:
            raise RuntimeError(f"Unpin snapshot data error: {e}") from e
