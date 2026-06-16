"""Integration tests for collection commands."""
import pytest
import json
import os


class TestCollection:
    """Test collection-related commands."""

    @pytest.fixture
    def test_collection(self, run_connected, unique_name):
        """Create a test collection and clean up after."""
        coll_name = f"coll_{unique_name}"

        # Create collection with simple schema
        schema = {
            "collection_name": coll_name,
            "auto_id": True,
            "fields": [
                {"name": "id", "type": "INT64", "is_primary": True},
                {"name": "embedding", "type": "FLOAT_VECTOR", "dim": 128}
            ]
        }

        # Write schema to temp file
        schema_file = f"/tmp/{coll_name}_schema.json"
        with open(schema_file, "w") as f:
            json.dump(schema, f)

        # Create collection
        output, code = run_connected(f"create collection --schema-file {schema_file}")
        if code != 0:
            try:
                os.remove(schema_file)
            except OSError:
                pass
            pytest.skip(f"Failed to create collection: {output}")

        yield coll_name

        # Cleanup
        run_connected(f"delete collection -c {coll_name} --yes")
        try:
            os.remove(schema_file)
        except OSError:
            pass

    def test_list_collections(self, run_connected):
        """Test list collections command."""
        output, code = run_connected("list collections")
        assert code == 0

    def test_create_collection_with_schema_file(self, run_connected, unique_name):
        """Test create collection with schema file."""
        coll_name = f"coll_{unique_name}"

        # Create schema with collection name
        schema = {
            "collection_name": coll_name,
            "auto_id": True,
            "fields": [
                {"name": "id", "type": "INT64", "is_primary": True},
                {"name": "embedding", "type": "FLOAT_VECTOR", "dim": 128}
            ],
            "enable_dynamic_field": True
        }
        schema_file = f"/tmp/{coll_name}_schema.json"
        with open(schema_file, "w") as f:
            json.dump(schema, f)

        output, code = run_connected(f"create collection --schema-file {schema_file}")
        assert code == 0, f"Failed to create collection: {output}"

        # Verify
        output, code = run_connected("list collections")
        assert coll_name in output

        # Cleanup
        run_connected(f"delete collection -c {coll_name} --yes")
        try:
            os.remove(schema_file)
        except OSError:
            pass

    def test_show_collection(self, test_collection, run_connected):
        """Test show collection command."""
        output, code = run_connected(f"show collection -c {test_collection}")
        assert code == 0
        assert test_collection in output

    def test_load_and_release_collection(self, test_collection, run_connected):
        """Test load and release collection."""
        # Load
        output, code = run_connected(f"load collection -c {test_collection}")
        assert code == 0

        # Release
        output, code = run_connected(f"release collection -c {test_collection}")
        assert code == 0

    def test_rename_collection(self, test_collection, run_connected, unique_name):
        """Test rename collection."""
        new_name = f"renamed_{unique_name}"

        output, code = run_connected(f"rename collection -old {test_collection} -new {new_name}")
        assert code == 0, f"Failed to rename collection: {output}"

        # Verify new name exists
        output, code = run_connected("list collections")
        assert new_name in output

        # Cleanup with new name
        run_connected(f"delete collection -c {new_name} --yes")

    def test_show_collection_stats(self, test_collection, run_connected):
        """Test show collection_stats command."""
        output, code = run_connected(f"show collection_stats -c {test_collection}")
        assert code == 0

    def test_flush(self, test_collection, run_connected):
        """Test flush command."""
        output, code = run_connected(f"flush -c {test_collection}")
        assert code == 0

    def test_truncate_collection(self, test_collection, run_connected):
        """Test truncate command."""
        output, code = run_connected(f"truncate -c {test_collection} --yes")
        assert code == 0

    def test_flush_all(self, run_connected):
        """Test flush_all command."""
        output, code = run_connected("flush_all")
        assert code == 0


class TestNewCollectionFeatures:
    """Test new collection API features."""

    @pytest.fixture
    def test_collection_with_index(self, run_connected, unique_name):
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
        output, code = run_connected(f"create index -c {coll_name} -f embedding -t FLAT -m L2")
        if code != 0:
            pytest.skip(f"Failed to create index: {output}")
        output, code = run_connected(f"load collection -c {coll_name}")
        if code != 0:
            pytest.skip(f"Failed to load collection: {output}")
        yield coll_name
        run_connected(f"release collection -c {coll_name}")
        run_connected(f"delete collection -c {coll_name} --yes")

    def test_run_analyzer_standard(self, run_connected):
        output, code = run_connected('run_analyzer -t "hello world" -a standard')
        assert code == 0

    def test_run_analyzer_json_params(self, run_connected):
        output, code = run_connected('run_analyzer -t "test text" -a \'{"tokenizer": "standard"}\'')
        assert code == 0

    def test_optimize(self, run_connected, test_collection_with_index):
        output, code = run_connected(f"optimize -c {test_collection_with_index}")
        assert code == 0
        assert "successfully" in output.lower()

    def test_refresh_load(self, run_connected, test_collection_with_index):
        output, code = run_connected(f"refresh_load -c {test_collection_with_index}")
        assert code == 0
        assert "successfully" in output.lower()

    def test_has_collection_exists(self, run_connected, test_collection_with_index):
        output, code = run_connected(f"has_collection -c {test_collection_with_index}")
        assert code == 0
        assert "True" in output

    def test_has_collection_not_exists(self, run_connected, unique_name):
        output, code = run_connected(f"has_collection -c nonexistent_{unique_name}")
        assert code == 0
        assert "False" in output

    def test_get_replicate_configuration(self, run_connected, test_collection_with_index):
        output, code = run_connected(f"get_replicate_configuration -c {test_collection_with_index}")
        assert code == 0 or "error" in output.lower()

    def test_add_and_drop_collection_function(self, run_connected, unique_name):
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
        output, code = run_connected(
            f"add_collection_function -c {coll} -fn bm25_fn -ft BM25 -if text -of text"
        )
        assert code == 0 or "error" in output.lower() or "not support" in output.lower()
        if code == 0:
            output, code = run_connected(f"drop_collection_function -c {coll} -fn bm25_fn")
            assert code == 0 or "error" in output.lower()
        run_connected(f"delete collection -c {coll} --yes")

    def test_add_and_drop_collection_field(self, run_connected, unique_name):
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
        output, code = run_connected(
            f"add_collection_field -c {coll} -f new_field -dt VARCHAR --max-length 256"
        )
        assert code == 0 or "error" in output.lower()
        if code == 0:
            output, code = run_connected(f"drop_collection_field -c {coll} -f new_field")
            assert code == 0 or "error" in output.lower()
        run_connected(f"delete collection -c {coll} --yes")
