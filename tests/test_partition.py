"""Integration tests for partition commands."""
from types import SimpleNamespace
import pytest
import json
import os

from milvus_cli.scripts import init_client_cli
from milvus_cli.scripts.milvus_client_cli import cli


class TestPartition:
    """Test partition-related commands."""

    @pytest.fixture
    def test_collection_for_partition(self, run_connected, unique_name):
        """Create a collection for partition tests."""
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

    def test_list_partitions(self, test_collection_for_partition, run_connected):
        """Test list partitions command."""
        output, code = run_connected(f"list partitions -c {test_collection_for_partition}")
        assert code == 0
        assert "_default" in output

    def test_create_and_delete_partition(self, test_collection_for_partition, run_connected, unique_name):
        """Test create and delete partition."""
        part_name = f"part_{unique_name}"
        coll = test_collection_for_partition

        # Create
        output, code = run_connected(f"create partition -c {coll} -p {part_name}")
        assert code == 0

        # Verify
        output, code = run_connected(f"list partitions -c {coll}")
        assert part_name in output

        # Delete
        output, code = run_connected(f"delete partition -c {coll} -p {part_name}")
        assert code == 0

    def test_show_partition(self, test_collection_for_partition, run_connected):
        """Test show partition command."""
        output, code = run_connected(f"show partition -c {test_collection_for_partition} -p _default")
        assert code == 0

    def test_show_partition_stats(self, test_collection_for_partition, run_connected):
        """Test show partition_stats command."""
        output, code = run_connected(f"show partition_stats -c {test_collection_for_partition} -p _default")
        assert code == 0

    def test_show_partition_exists(self, test_collection_for_partition, run_connected):
        """Test show partition_exists command for an existing partition."""
        output, code = run_connected(
            f"show partition_exists -c {test_collection_for_partition} -p _default"
        )
        assert code == 0
        assert "true" in output.lower()
        assert test_collection_for_partition in output

    def test_show_partition_exists_false(self, test_collection_for_partition, run_connected, unique_name):
        """Test show partition_exists command for a missing partition."""
        missing = f"part_missing_{unique_name}"
        output, code = run_connected(
            f"show partition_exists -c {test_collection_for_partition} -p {missing}"
        )
        assert code == 0
        assert "false" in output.lower()

    def test_show_partition_exists_command(self, cli_runner):
        """Test show partition_exists command path."""
        old_instance = init_client_cli._global_cli_instance

        calls = []

        init_client_cli._global_cli_instance = SimpleNamespace(
            partition=SimpleNamespace(
                has_partition=lambda collection_name, partition_name: (
                    calls.append((collection_name, partition_name))
                    or (partition_name == "existing")
                )
            )
        )

        try:
            result_true = cli_runner.invoke(
                cli,
                [
                    "show",
                    "partition_exists",
                    "-c",
                    "test_collection",
                    "-p",
                    "existing",
                ],
            )

            result_false = cli_runner.invoke(
                cli,
                [
                    "show",
                    "partition_exists",
                    "-c",
                    "test_collection",
                    "-p",
                    "missing",
                ],
            )
        finally:
            init_client_cli._global_cli_instance = old_instance

        assert result_true.exit_code == 0
        assert result_false.exit_code == 0
        assert calls == [("test_collection", "existing"), ("test_collection", "missing")]
        assert "Partition 'existing' exists in collection 'test_collection': True" in result_true.output
        assert "Partition 'missing' exists in collection 'test_collection': False" in result_false.output

    def test_load_and_release_partition(self, test_collection_for_partition, run_connected):
        """Test load and release partition (requires vector index)."""
        coll = test_collection_for_partition

        output, code = run_connected(
            f"create index -c {coll} -f embedding -t FLAT -m L2"
        )
        assert code == 0

        # Load
        output, code = run_connected(f"load partition -c {coll} -p _default")
        assert code == 0

        # Release
        output, code = run_connected(f"release partition -c {coll} -p _default")
        assert code == 0
