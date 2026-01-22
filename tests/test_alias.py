"""Integration tests for alias commands."""
import pytest
import json
import os


class TestAlias:
    """Test alias-related commands."""

    @pytest.fixture
    def test_collection_for_alias(self, run_connected, unique_name):
        """Create a collection for alias tests."""
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
            os.remove(schema_file)
            pytest.skip(f"Failed to create collection: {output}")

        yield coll_name

        run_connected(f"delete collection -c {coll_name} --yes")
        try:
            os.remove(schema_file)
        except OSError:
            pass

    def test_create_and_delete_alias(self, test_collection_for_alias, run_connected, unique_name):
        """Test create and delete alias."""
        coll = test_collection_for_alias
        alias_name = f"alias_{unique_name}"

        # Create alias
        output, code = run_connected(f"create alias -c {coll} -a {alias_name}")
        assert code == 0

        # List aliases
        output, code = run_connected(f"list aliases -c {coll}")
        assert code == 0
        assert alias_name in output

        # Delete alias
        output, code = run_connected(f"delete alias -a {alias_name}")
        assert code == 0

    def test_show_alias(self, test_collection_for_alias, run_connected, unique_name):
        """Test show alias command."""
        coll = test_collection_for_alias
        alias_name = f"alias_{unique_name}"

        # Create alias first
        run_connected(f"create alias -c {coll} -a {alias_name}")

        # Show alias
        output, code = run_connected(f"show alias -a {alias_name}")
        assert code == 0

        # Cleanup
        run_connected(f"delete alias -a {alias_name}")

    def test_alter_alias(self, test_collection_for_alias, run_connected, unique_name):
        """Test alter alias command.

        Note: Altering an alias is done via 'create alias -A' flag, not 'alter alias'.
        """
        coll = test_collection_for_alias
        alias_name = f"alias_{unique_name}"

        # Create alias first
        output, code = run_connected(f"create alias -c {coll} -a {alias_name}")
        if code != 0:
            pytest.skip(f"Failed to create alias: {output}")

        # Alter alias to same collection using -A flag (just testing command works)
        output, code = run_connected(f"create alias -c {coll} -a {alias_name} -A")

        # Cleanup - must delete alias before the fixture deletes the collection
        run_connected(f"delete alias -a {alias_name}")

        assert code == 0, f"Failed to alter alias: {output}"
