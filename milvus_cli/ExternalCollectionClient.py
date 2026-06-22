from __future__ import annotations

try:
    from .BaseClient import BaseMilvusClient
except ImportError:
    from BaseClient import BaseMilvusClient


class MilvusExternalCollection(BaseMilvusClient):
    """External collection operations based on MilvusClient API."""

    def refresh_external_collection(self, collection_name):
        try:
            client = self._get_client()
            client.refresh_external_collection(collection_name=collection_name)
            return f"Refresh external collection {collection_name} successfully!"
        except Exception as e:
            raise RuntimeError(f"Refresh external collection error: {e}") from e

    def get_refresh_external_collection_progress(self, collection_name):
        try:
            client = self._get_client()
            result = client.get_refresh_external_collection_progress(
                collection_name=collection_name
            )
            return result
        except Exception as e:
            raise RuntimeError(
                f"Get refresh external collection progress error: {e}"
            ) from e

    def list_refresh_external_collection_jobs(self):
        try:
            client = self._get_client()
            result = client.list_refresh_external_collection_jobs()
            return result
        except Exception as e:
            raise RuntimeError(
                f"List refresh external collection jobs error: {e}"
            ) from e
