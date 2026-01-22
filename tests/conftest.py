import pytest
import shlex
from click.testing import CliRunner

@pytest.fixture(scope="session")
def cli_runner():
    """Shared CLI runner instance."""
    return CliRunner()

@pytest.fixture(scope="session")
def cli_instance():
    """Get the CLI instance."""
    from milvus_cli.scripts.milvus_client_cli import cli
    return cli

@pytest.fixture
def run_cmd(cli_runner, cli_instance):
    """Execute CLI command, return (output, exit_code)."""
    def _run(cmd: str) -> tuple:
        result = cli_runner.invoke(cli_instance, shlex.split(cmd), catch_exceptions=False)
        return result.output, result.exit_code
    return _run


# Track connection state across tests
_milvus_available = None

@pytest.fixture(scope="session")
def milvus_available(cli_runner, cli_instance):
    """Check if Milvus is available (session-scoped check)."""
    global _milvus_available
    if _milvus_available is None:
        result = cli_runner.invoke(cli_instance, ["connect", "-uri", "http://localhost:19530"])
        _milvus_available = result.exit_code == 0
        if _milvus_available:
            # Disconnect after check
            cli_runner.invoke(cli_instance, ["disconnect"])
    return _milvus_available


@pytest.fixture
def run_connected(cli_runner, cli_instance, milvus_available):
    """Execute CLI command with active connection.

    This fixture ensures the connection is established before each test
    and properly handles reconnection if needed.
    """
    if not milvus_available:
        pytest.skip("Cannot connect to Milvus server")

    # Ensure we're connected before running the test
    result = cli_runner.invoke(cli_instance, ["connect", "-uri", "http://localhost:19530"])
    if result.exit_code != 0:
        pytest.skip("Cannot connect to Milvus server")

    def _run(cmd: str) -> tuple:
        result = cli_runner.invoke(cli_instance, shlex.split(cmd), catch_exceptions=False)
        return result.output, result.exit_code

    yield _run

    # Don't disconnect - let the connection persist for efficiency
    # Other tests will reconnect if needed


@pytest.fixture
def unique_name():
    """Generate unique name for test resources."""
    import uuid
    return f"test_{uuid.uuid4().hex[:8]}"
