"""Integration tests for index commands."""
import pytest
import json
import os


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

    @pytest.mark.skip(reason="create index command is interactive only - no CLI options")
    def test_create_and_delete_index(self, test_collection_for_index, run_connected):
        """Test create and delete index.

        Note: This test is skipped because the 'create index' command
        only supports interactive input, not command-line options.
        """
        coll = test_collection_for_index

        # Create index
        output, code = run_connected(f"create index -c {coll} -f embedding -t FLAT -m L2")
        assert code == 0

        # List indexes
        output, code = run_connected(f"list indexes -c {coll}")
        assert code == 0

        # Delete index
        output, code = run_connected(f"delete index -c {coll} -f embedding")
        assert code == 0

    @pytest.mark.skip(reason="create index command is interactive only - no CLI options")
    def test_show_index(self, test_collection_for_index, run_connected):
        """Test show index command.

        Note: This test is skipped because the 'create index' command
        only supports interactive input, not command-line options.
        """
        coll = test_collection_for_index

        # Create index first
        run_connected(f"create index -c {coll} -f embedding -t FLAT -m L2")

        # Show index
        output, code = run_connected(f"show index -c {coll} -f embedding")
        assert code == 0

        # Cleanup
        run_connected(f"delete index -c {coll} -f embedding")
