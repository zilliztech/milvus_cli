"""Integration tests for index commands."""
import pytest
import json
import os
from types import SimpleNamespace

from milvus_cli.scripts import init_client_cli
from milvus_cli.scripts.milvus_client_cli import cli


class TestIndex:
    """Test index-related commands."""

    @pytest.fixture
    def test_collection_for_index(self, run_connected, unique_name):
        """Create a collection for index tests."""
        coll_name = f"coll_{unique_name}"
        schema = {
            "collection_name": coll_name,
            "auto_id": True,
            "fields": [
                {"name": "id", "type": "INT64", "is_primary": True},
                {"name": "embedding", "type": "FLOAT_VECTOR", "dim": 128}
            ]
        }
        schema_file = f"/tmp/{coll_name}_schema.json"
        with open(schema_file, "w") as f:
            json.dump(schema, f)

        output, code = run_connected(f"create collection --schema-file {schema_file}")
        if code != 0:
            try:
                os.remove(schema_file)
            except OSError:
                pass
            pytest.skip(f"Failed to create collection: {output}")

        yield coll_name

        run_connected(f"delete collection -c {coll_name} --yes")
        try:
            os.remove(schema_file)
        except OSError:
            pass

    def test_create_and_delete_index(self, test_collection_for_index, run_connected):
        """Test create and delete index (non-interactive flags)."""
        coll = test_collection_for_index

        # Create index
        output, code = run_connected(f"create index -c {coll} -f embedding -t FLAT -m L2")
        assert code == 0

        # List indexes
        output, code = run_connected(f"list indexes -c {coll}")
        assert code == 0

        # Delete index
        output, code = run_connected(f"delete index -c {coll} -in embedding --yes")
        assert code == 0

    def test_show_index(self, test_collection_for_index, run_connected):
        """Test show index command."""
        coll = test_collection_for_index

        # Create index first
        run_connected(f"create index -c {coll} -f embedding -t FLAT -m L2")

        # Show index (MilvusClient indexes by field / index name)
        output, code = run_connected(f"show index -c {coll} -in embedding")
        assert code == 0

        # Cleanup
        run_connected(f"delete index -c {coll} -in embedding --yes")

    def test_alter_index_properties_command(self, cli_runner):
        """Test alter index properties command path with prompt input."""
        old_instance = init_client_cli._global_cli_instance

        captured = {}

        init_client_cli._global_cli_instance = SimpleNamespace(
            index=SimpleNamespace(
                alter_index_properties=lambda collection_name, index_name, properties: (
                    captured.update(
                        {
                            "collection": collection_name,
                            "index": index_name,
                            "properties": properties,
                        }
                    )
                    or f"Alter index {index_name} in collection {collection_name} successfully!"
                )
            )
        )

        try:
            result = cli_runner.invoke(
                cli,
                [
                    "alter",
                    "index_properties",
                    "-c",
                    "test_collection",
                    "-in",
                    "embedding",
                ],
                input="index_thread_pool\n4\nn\n",
            )
        finally:
            init_client_cli._global_cli_instance = old_instance

        assert result.exit_code == 0
        assert captured["collection"] == "test_collection"
        assert captured["index"] == "embedding"
        assert captured["properties"] == {"index_thread_pool": 4}
        assert "Alter index embedding in collection test_collection successfully!" in result.output

    def test_drop_index_properties_command(self, cli_runner):
        """Test delete index properties command path."""
        old_instance = init_client_cli._global_cli_instance

        calls = []

        init_client_cli._global_cli_instance = SimpleNamespace(
            index=SimpleNamespace(
                drop_index_properties=lambda collection_name, index_name, property_keys: (
                    calls.append((collection_name, index_name, list(property_keys)))
                    or f"Drop index {index_name} in collection {collection_name} successfully!"
                )
            )
        )

        try:
            result = cli_runner.invoke(
                cli,
                [
                    "delete",
                    "index_properties",
                    "-c",
                    "test_collection",
                    "-in",
                    "embedding",
                    "-k",
                    "index_thread_pool",
                ],
            )
        finally:
            init_client_cli._global_cli_instance = old_instance

        assert result.exit_code == 0
        assert calls == [("test_collection", "embedding", ["index_thread_pool"])]
        assert "Drop index embedding in collection test_collection successfully!" in result.output
