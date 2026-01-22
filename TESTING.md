# Milvus CLI Testing Guide

## Overview

This project has two types of tests:

### 1. **Unit Tests** (`milvus_cli/test/`)
- **Framework**: `unittest`
- **Purpose**: Test Python modules and classes directly
- **Target**: Internal APIs (`ConnectionClient`, `CollectionClient`, etc.)
- **Run with**: `python -m unittest discover milvus_cli/test`

### 2. **Integration Tests** (`tests/`)
- **Framework**: `pytest`
- **Purpose**: Test CLI commands and user experience
- **Target**: Command-line interface
- **Run with**: `python run_tests.py` or `pytest tests/`

This guide focuses on **Integration Tests** which test the CLI interface.

## Quick Start

Run all tests with a single command:

```bash
source venv/bin/activate
pip install -e .
python run_tests.py --uri http://localhost:19530
```

This will:
- Run 71 integration tests
- Generate a single Markdown test report: `TEST_REPORT_<timestamp>.md`
- Display results in the terminal

## Test Report

After running tests, you'll get a report file like `TEST_REPORT_20260122_103931.md` containing:

- **Test Summary**: Total, passed, failed, skipped counts
- **Test Details**: Breakdown by test category
- **Failed Test Details**: Error messages (if any)

## Configuration

### Method 1: Command Line Argument

```bash
python run_tests.py --uri http://10.102.9.174:19530
```

### Method 2: Environment Variable

```bash
export MILVUS_URI=http://10.102.9.174:19530
python run_tests.py
```

### Method 3: .env File

```bash
# Create .env file in tests/ directory
echo "MILVUS_URI=http://10.102.9.174:19530" > tests/.env

# Run tests
python run_tests.py
```

## Running Specific Tests

```bash
source venv/bin/activate

# Run connection tests only
MILVUS_URI=http://10.102.9.174:19530 pytest tests/test_connection.py -v

# Run database tests only
MILVUS_URI=http://10.102.9.174:19530 pytest tests/test_database.py -v

# Run a specific test
MILVUS_URI=http://10.102.9.174:19530 pytest tests/test_connection.py::TestConnection::test_connect_success -v
```

## Test Categories

| Category | Tests | Description |
|----------|-------|-------------|
| Connection | 6 | Connection, disconnection, version queries |
| Collection | 8 | Collection CRUD, load, stats |
| Database | 5 | Database operations |
| User/Role | 4 | User and role management |
| Resource Group | 2 | Resource group management |
| Privilege Group | 3 | Privilege group management |
| Partition | 4 | Partition operations |
| Alias | 3 | Alias management |
| New Features | 30+ | Output formatting, auto-completion |

## Troubleshooting

### Connection Refused

```
Cannot connect to Milvus server
```

**Solution:**
- Ensure Milvus is running at the specified URI
- Check network connectivity
- Verify the URI is correct

### pytest Not Found

```
ModuleNotFoundError: No module named 'pytest'
```

**Solution:**
```bash
source venv/bin/activate
pip install pytest
```

### Virtual Environment Not Found

**Solution:**
```bash
python3 -m venv venv
source venv/bin/activate
pip install -e .
```

## CI/CD Integration

The test runner generates JUnit XML temporarily (deleted after report generation). For CI/CD, you can modify `run_tests.py` to preserve the XML file:

```python
# Comment out this line to keep XML file
# if junit_xml.exists():
#     junit_xml.unlink()
```

Then configure your CI to read the JUnit XML report.

## Advanced Usage

### Stop at First Failure

```bash
MILVUS_URI=http://10.102.9.174:19530 pytest tests/ -x
```

### Verbose Output

```bash
MILVUS_URI=http://10.102.9.174:19530 pytest tests/ -vv
```

### Run Only Failed Tests

```bash
# First run
MILVUS_URI=http://10.102.9.174:19530 pytest tests/

# Re-run only failed tests
MILVUS_URI=http://10.102.9.174:19530 pytest tests/ --lf
```
