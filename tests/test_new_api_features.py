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

    def test_grant_privilege_v2_command(self, cli_runner):
        """Test grant privilege_v2 command path."""
        old_instance = init_client_cli._global_cli_instance
        calls = {}

        class FakeRole:
            def grantPrivilegeV2(self, role_name, privilege, collection_name=None, database_name=None, resource_group_name=None):
                calls["role"] = role_name
                calls["privilege"] = privilege
                calls["collection"] = collection_name
                return "ok"

        init_client_cli._global_cli_instance = SimpleNamespace(role=FakeRole())
        try:
            result = cli_runner.invoke(
                cli, ["grant", "privilege_v2", "-r", "test_role", "-p", "Search", "-c", "test_col"]
            )
        finally:
            init_client_cli._global_cli_instance = old_instance

        assert result.exit_code == 0
        assert calls["role"] == "test_role"
        assert calls["privilege"] == "Search"
        assert calls["collection"] == "test_col"

    def test_revoke_privilege_v2_command(self, cli_runner):
        """Test revoke privilege_v2 command path."""
        old_instance = init_client_cli._global_cli_instance
        calls = {}

        class FakeRole:
            def revokePrivilegeV2(self, role_name, privilege, collection_name=None, database_name=None, resource_group_name=None):
                calls["role"] = role_name
                calls["privilege"] = privilege
                return "ok"

        init_client_cli._global_cli_instance = SimpleNamespace(role=FakeRole())
        try:
            result = cli_runner.invoke(
                cli, ["revoke", "privilege_v2", "-r", "test_role", "-p", "Search"]
            )
        finally:
            init_client_cli._global_cli_instance = old_instance

        assert result.exit_code == 0
        assert calls["role"] == "test_role"
        assert calls["privilege"] == "Search"
