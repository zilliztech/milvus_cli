# Milvus CLI Tests

Integration tests for Milvus CLI.

## Prerequisites

- Python >= 3.8.5
- Running Milvus instance
- pytest

## Quick Start

```bash
# Activate virtual environment
source venv/bin/activate

# Run all tests
MILVUS_URI=http://10.102.9.174:19530 pytest tests/ -v
```

## Configuration

Set the Milvus URI using environment variable:

```bash
export MILVUS_URI=http://10.102.9.174:19530
```

Or create a `.env` file in the tests directory:

```bash
cp .env.example .env
# Edit .env and set MILVUS_URI
```

**Default**: If `MILVUS_URI` is not set, tests use `http://localhost:19530`

## Test Files

- `conftest.py` - Pytest fixtures and configuration
- `test_connection.py` - Connection and basic commands
- `test_collection.py` - Collection operations
- `test_database.py` - Database operations
- `test_index.py` - Index operations
- `test_partition.py` - Partition operations
- `test_data.py` - Data import/export
- `test_user_role.py` - User and role management
- `test_alias.py` - Alias management
- `test_resource_group.py` - Resource group operations
- `test_privilege_group.py` - Privilege group operations
- `test_search_query.py` - Search and query operations
- `test_new_features_integration.py` - New features integration
- `run_integration_tests.py` - Standalone test runner (no pytest required)

## Running Specific Tests

```bash
# Run connection tests
pytest tests/test_connection.py -v

# Run a specific test class
pytest tests/test_connection.py::TestConnection -v

# Run a specific test method
pytest tests/test_connection.py::TestConnection::test_connect_success -v
```

## Fixtures

### `milvus_uri`
Returns the Milvus URI from environment variable or default.

### `cli_runner`
Shared CLI runner instance for executing commands.

### `run_cmd(cmd: str) -> (output, exit_code)`
Execute a CLI command and return output and exit code.

### `run_connected(cmd: str) -> (output, exit_code)`
Execute a CLI command with an active Milvus connection.

### `unique_name() -> str`
Generate a unique name for test resources (e.g., `test_a1b2c3d4`).

## Common Issues

### Connection Refused

Tests will be automatically skipped if Milvus server is not available.

**Fix:**
1. Ensure Milvus is running at the specified URI
2. Check network connectivity
3. Verify `MILVUS_URI` is correct

### Tests Skipped

Some tests are skipped when:
- Milvus server is not available
- Required resources don't exist
- Server version doesn't support certain features

This is expected behavior and not an error.
