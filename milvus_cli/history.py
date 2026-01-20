import json
import os
from datetime import datetime
from pathlib import Path


class ConnectionHistory:
    """Manages persistent connection history stored in ~/.milvus_cli_connections.json"""

    DEFAULT_PATH = Path.home() / ".milvus_cli_connections.json"

    def __init__(self, path=None):
        self.path = Path(path) if path else self.DEFAULT_PATH
        self._connections = self._load()

    def _load(self):
        """Load connections from JSON file."""
        if not self.path.exists():
            return {}
        try:
            with open(self.path, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}

    def _save(self):
        """Save connections to JSON file."""
        import click
        try:
            with open(self.path, "w") as f:
                json.dump(self._connections, f, indent=2)
        except IOError as e:
            click.echo(f"Warning: Could not save connection history: {e}", err=True)

    def save_connection(self, uri, token=None, tlsmode=0, cert=None, alias=None):
        """
        Save a connection. Uses URI as alias if not provided.
        Updates timestamp if URI already exists.
        """
        key = alias if alias else uri
        self._connections[key] = {
            "uri": uri,
            "token": token,
            "tlsmode": tlsmode,
            "cert": cert,
            "last_used": datetime.now().isoformat(),
        }
        self._save()

    def get_connection(self, alias):
        """Get connection by alias."""
        return self._connections.get(alias)

    def list_connections(self):
        """Return list of saved connections with their details."""
        result = []
        for alias, conn in self._connections.items():
            result.append({
                "alias": alias,
                "uri": conn["uri"],
                "last_used": conn.get("last_used", "unknown"),
            })
        return sorted(result, key=lambda x: x.get("last_used", ""), reverse=True)

    def delete_connection(self, uri):
        """Delete connection by URI. Returns True if found and deleted."""
        # Find and remove any connection with matching URI
        to_delete = [k for k, v in self._connections.items() if v["uri"] == uri]
        if not to_delete:
            return False
        for key in to_delete:
            del self._connections[key]
        self._save()
        return True

    def clear(self):
        """Clear all saved connections."""
        self._connections = {}
        self._save()
