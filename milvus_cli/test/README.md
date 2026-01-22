# Milvus CLI Unit Tests

Unit tests for Milvus CLI internal modules and classes.

## Overview

These tests directly test Python classes like:
- `ConnectionClient` - Connection management
- `CollectionClient` - Collection operations
- `DatabaseClient` - Database operations
- `IndexClient` - Index operations
- etc.

## Prerequisites

- Python >= 3.8.5
- Running Milvus instance
- pymilvus >= 2.5.0

## Configuration

Create a `test.env` file in this directory or set environment variables:

```bash
# Required
MILVUS_TEST_URI=http://10.102.9.174:19530

# Optional
MILVUS_TEST_TOKEN=your_token_here
MILVUS_TEST_TLS_MODE=0
MILVUS_TEST_CERT_PATH=/path/to/cert
MILVUS_TEST_COLLECTION_PREFIX=test_collection
```

**Default values** (if not set):
- `MILVUS_TEST_URI`: `http://127.0.0.1:19530`
- `MILVUS_TEST_TOKEN`: `None`
- `MILVUS_TEST_TLS_MODE`: `0`
- `MILVUS_TEST_CERT_PATH`: `None`
- `MILVUS_TEST_COLLECTION_PREFIX`: `test_collection`

## Running Tests

### Run all unit tests

```bash
# From project root
python -m unittest discover milvus_cli/test

# Or from this directory
python -m unittest discover .
```

### Run specific test file

```bash
python -m unittest milvus_cli.test.test_connection_client
python -m unittest milvus_cli.test.test_collection_client
```

### Run specific test class

```bash
python -m unittest milvus_cli.test.test_connection_client.TestConnectionClient
```

### Run specific test method

```bash
python -m unittest milvus_cli.test.test_connection_client.TestConnectionClient.test_show_connection
```

### Verbose output

```bash
python -m unittest discover milvus_cli/test -v
```

## Test Files

- `test_config.py` - Test configuration management
- `test_connection_client.py` - Connection tests
- `test_collection_client.py` - Collection tests
- `test_database_client.py` - Database tests
- `test_index_client.py` - Index tests
- `test_partition_client.py` - Partition tests
- `test_data_client.py` - Data import/export tests
- `test_user_client.py` - User management tests
- `test_role_client.py` - Role management tests
- `test_alias_client.py` - Alias tests
- `test_cli_client.py` - CLI client tests

## Difference from Integration Tests

| Aspect | Unit Tests (`milvus_cli/test/`) | Integration Tests (`tests/`) |
|--------|--------------------------------|------------------------------|
| Framework | unittest | pytest |
| Target | Python classes/modules | CLI commands |
| Speed | Faster | Slower |
| Scope | Internal API | User interface |
| Run command | `python -m unittest discover` | `pytest tests/` or `python run_tests.py` |

## Troubleshooting

### Connection Issues

If tests fail with connection errors:
1. Ensure Milvus is running at the configured URI
2. Check `test.env` or environment variables
3. Verify network connectivity

### Import Errors

If you get import errors:
```bash
# Install the package in development mode
pip install -e .
```

### Configuration Not Loaded

If configuration is not being picked up:
1. Check that `test.env` exists in this directory
2. Or set environment variables directly
3. Run `test_config.py` to verify configuration:
   ```bash
   python milvus_cli/test/test_config.py
   ```
