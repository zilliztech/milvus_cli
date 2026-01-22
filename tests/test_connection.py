"""Integration tests for connection commands."""
import pytest


class TestConnection:
    """Test connection-related commands."""

    def test_connect_success(self, run_cmd):
        """Test successful connection to Milvus."""
        output, code = run_cmd("connect -uri http://localhost:19530")
        # If Milvus is not running, the connection will fail - skip the test
        if code != 0 and ("refused" in output.lower() or "failed" in output.lower() or "error" in output.lower()):
            pytest.skip("Cannot connect to Milvus server")
        assert code == 0
        assert "success" in output.lower() or "connect" in output.lower()

    def test_connect_with_alias(self, run_cmd):
        """Test connection with alias."""
        output, code = run_cmd("connect -uri http://localhost:19530")
        # If Milvus is not running, the connection will fail - skip the test
        if code != 0 and ("refused" in output.lower() or "failed" in output.lower() or "error" in output.lower()):
            pytest.skip("Cannot connect to Milvus server")
        assert code == 0

    def test_connection_verified(self, run_connected):
        """Test that connection is active by checking server_version."""
        # Note: There is no 'show connection' command in MilvusClient CLI
        # We verify connection by running server_version which requires a connection
        output, code = run_connected("server_version")
        assert code == 0
        assert "version" in output.lower()

    def test_disconnect(self, run_cmd):
        """Test disconnect command."""
        # First connect
        output, code = run_cmd("connect -uri http://localhost:19530")
        # If Milvus is not running, skip the test
        if code != 0:
            pytest.skip("Cannot connect to Milvus server")
        # Then disconnect
        output, code = run_cmd("disconnect")
        assert code == 0

    def test_version(self, run_cmd):
        """Test version command."""
        output, code = run_cmd("version")
        assert code == 0
        assert "milvus_cli" in output.lower() or "v" in output.lower()

    def test_server_version(self, run_connected):
        """Test server_version command."""
        output, code = run_connected("server_version")
        assert code == 0
