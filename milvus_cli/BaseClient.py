from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pymilvus import MilvusClient
    from ConnectionClient import MilvusClientConnection


class BaseMilvusClient:
    """Base class for all Milvus client modules."""

    def __init__(self, connection_client: MilvusClientConnection | None = None) -> None:
        self.connection_client = connection_client

    def _get_client(self) -> MilvusClient:
        if not self.connection_client:
            raise ConnectionError("Connection client not set!")
        client = self.connection_client.get_client()
        if not client:
            raise ConnectionError("Not connected to Milvus! Please connect first.")
        return client
