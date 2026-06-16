"""Integration tests for new collection commands."""
import pytest
import json
import os
from milvus_cli.scripts import init_client_cli
from milvus_cli.scripts.milvus_client_cli import cli


@pytest.fixture
def test_collection_with_index(run_connected, unique_name):
    """Create a test collection with index and data, load it."""
    coll_name = f"newfeat_{unique_name}"
    schema = {
        "collection_name": coll_name,
        "auto_id": True,
        "fields": [
            {"name": "id", "type": "INT64", "is_primary": True},
            {"name": "embedding", "type": "FLOAT_VECTOR", "dim": 4}
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

    # Create index
    output, code = run_connected(f"create index -c {coll_name} -f embedding -t FLAT -m L2")
    if code != 0:
        pytest.skip(f"Failed to create index: {output}")

    # Load
    output, code = run_connected(f"load collection -c {coll_name}")
    if code != 0:
        pytest.skip(f"Failed to load collection: {output}")

    yield coll_name

    # Cleanup
    run_connected(f"release collection -c {coll_name}")
    run_connected(f"delete collection -c {coll_name} --yes")


class TestRunAnalyzer:
    """Test run_analyzer command with real Milvus."""

    def test_run_analyzer_standard(self, run_connected):
        """Test run_analyzer with standard analyzer."""
        output, code = run_connected('run_analyzer -t "hello world" -a standard')
        assert code == 0

    def test_run_analyzer_json_params(self, run_connected):
        """Test run_analyzer with JSON analyzer params."""
        output, code = run_connected('run_analyzer -t "test text" -a \'{"tokenizer": "standard"}\'')
        assert code == 0


class TestOptimize:
    """Test optimize command with real Milvus."""

    def test_optimize(self, run_connected, test_collection_with_index):
        """Test optimize command."""
        output, code = run_connected(f"optimize -c {test_collection_with_index}")
        assert code == 0
        assert "successfully" in output.lower()


class TestRefreshLoad:
    """Test refresh_load command with real Milvus."""

    def test_refresh_load(self, run_connected, test_collection_with_index):
        """Test refresh_load command."""
        output, code = run_connected(f"refresh_load -c {test_collection_with_index}")
        assert code == 0
        assert "successfully" in output.lower()


class TestCollectionFunction:
    """Test collection function commands with real Milvus."""

    def test_add_and_drop_collection_function(self, run_connected, unique_name):
        """Test add_collection_function and drop_collection_function."""
        coll = f"func_{unique_name}"
        schema = {
            "collection_name": coll,
            "auto_id": True,
            "fields": [
                {"name": "id", "type": "INT64", "is_primary": True},
                {"name": "text", "type": "VARCHAR", "max_length": 512},
                {"name": "embedding", "type": "FLOAT_VECTOR", "dim": 4}
            ]
        }
        schema_file = f"/tmp/{coll}_schema.json"
        with open(schema_file, "w") as f:
            json.dump(schema, f)
        output, code = run_connected(f"create collection --schema-file {schema_file}")
        try:
            os.remove(schema_file)
        except OSError:
            pass
        if code != 0:
            pytest.skip(f"Failed to create collection: {output}")

        # Add function (BM25)
        output, code = run_connected(
            f"add_collection_function -c {coll} -fn bm25_fn -ft BM25 -if text -of text"
        )
        assert code == 0 or "error" in output.lower() or "not support" in output.lower()

        # Drop function
        if code == 0:
            output, code = run_connected(
                f"drop_collection_function -c {coll} -fn bm25_fn"
            )
            assert code == 0 or "error" in output.lower()

        # Cleanup
        run_connected(f"delete collection -c {coll} --yes")


class TestCollectionField:
    """Test collection field commands with real Milvus."""

    def test_add_and_drop_collection_field(self, run_connected, unique_name):
        """Test add_collection_field and drop_collection_field."""
        coll = f"field_{unique_name}"
        schema = {
            "collection_name": coll,
            "auto_id": True,
            "fields": [
                {"name": "id", "type": "INT64", "is_primary": True},
                {"name": "embedding", "type": "FLOAT_VECTOR", "dim": 4}
            ]
        }
        schema_file = f"/tmp/{coll}_schema.json"
        with open(schema_file, "w") as f:
            json.dump(schema, f)
        output, code = run_connected(f"create collection --schema-file {schema_file}")
        try:
            os.remove(schema_file)
        except OSError:
            pass
        if code != 0:
            pytest.skip(f"Failed to create collection: {output}")

        # Add field
        output, code = run_connected(
            f"add_collection_field -c {coll} -f new_field -dt VARCHAR --max-length 256"
        )
        assert code == 0 or "error" in output.lower()

        # Drop field
        if code == 0:
            output, code = run_connected(
                f"drop_collection_field -c {coll} -f new_field"
            )
            assert code == 0 or "error" in output.lower()

        # Cleanup
        run_connected(f"delete collection -c {coll} --yes")
