"""Integration tests for partition commands."""
import pytest
import json
import os


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

    @pytest.mark.skip(reason="Loading partition requires an index, and create index command is interactive only")
    def test_load_and_release_partition(self, test_collection_for_partition, run_connected):
        """Test load and release partition.

        Note: This test is skipped because loading a partition requires
        an index, and the 'create index' command only supports interactive
        input, not command-line options.
        """
        coll = test_collection_for_partition

        # Load
        output, code = run_connected(f"load partition -c {coll} -p _default")
        assert code == 0

        # Release
        output, code = run_connected(f"release partition -c {coll} -p _default")
        assert code == 0
