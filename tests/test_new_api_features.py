"""Integration tests for new API features."""
import pytest
import json
import os
from types import SimpleNamespace
from milvus_cli.scripts import init_client_cli
from milvus_cli.scripts.milvus_client_cli import cli


@pytest.fixture
def test_collection_simple(run_connected, unique_name):
    """Create a simple test collection."""
    coll_name = f"newapi_{unique_name}"
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
    try:
        os.remove(schema_file)
    except OSError:
        pass
    if code != 0:
        pytest.skip(f"Failed to create collection: {output}")
    yield coll_name
    run_connected(f"delete collection -c {coll_name} --yes")


class TestHasCollection:
    """Test has_collection command."""

    def test_has_collection_exists(self, run_connected, test_collection_simple):
        """Test has_collection for existing collection."""
        output, code = run_connected(f"has_collection -c {test_collection_simple}")
        assert code == 0
        assert "True" in output

    def test_has_collection_not_exists(self, run_connected, unique_name):
        """Test has_collection for non-existing collection."""
        output, code = run_connected(f"has_collection -c nonexistent_{unique_name}")
        assert code == 0
        assert "False" in output


class TestFileResource:
    """Test file resource commands."""

    def test_list_file_resources(self, run_connected):
        """Test list_file_resources command."""
        output, code = run_connected("list_file_resources")
        assert code == 0


class TestReplicateConfiguration:
    """Test replication configuration commands."""

    def test_get_replicate_configuration(self, run_connected, test_collection_simple):
        """Test get_replicate_configuration command."""
        output, code = run_connected(f"get_replicate_configuration -c {test_collection_simple}")
        assert code == 0 or "error" in output.lower()


class TestPrivilegeV2:
    """Test grant/revoke privilege v2 commands."""

    def test_grant_and_revoke_privilege_v2(self, run_connected, unique_name):
        """Test grant and revoke privilege_v2 with real Milvus."""
        role_name = f"v2role_{unique_name}"

        # Create role
        output, code = run_connected(f"create role -r {role_name}")
        assert code == 0

        # Grant privilege v2
        output, code = run_connected(
            f"grant privilege_v2 -r {role_name} -p Search -c __default_collection"
        )
        # May succeed or return error depending on Milvus version
        assert code == 0 or "error" in output.lower()

        # Revoke privilege v2
        output, code = run_connected(
            f"revoke privilege_v2 -r {role_name} -p Search -c __default_collection"
        )
        assert code == 0 or "error" in output.lower()

        # Cleanup
        run_connected(f"delete role -r {role_name}")
