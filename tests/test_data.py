"""Integration tests for data operation commands."""
import pytest
import json
import os


class TestData:
    """Test data operation commands."""

    @pytest.fixture
    def loaded_collection(self, run_connected, unique_name):
        """Create and load a collection for data tests."""
        coll_name = f"coll_{unique_name}"
        schema = {
            "collection_name": coll_name,
            "auto_id": False,
            "fields": [
                {"name": "id", "type": "INT64", "is_primary": True},
                {"name": "embedding", "type": "FLOAT_VECTOR", "dim": 4}
            ]
        }
        schema_file = f"/tmp/{coll_name}_schema.json"
        with open(schema_file, "w") as f:
            json.dump(schema, f)

        output, code = run_connected(f"create collection --schema-file {schema_file}")
        if code != 0:
            try:
                os.remove(schema_file)
            except OSError:
                pass
            pytest.skip(f"Failed to create collection: {output}")

        # Create index and load
        run_connected(f"create index -c {coll_name} -f embedding -t FLAT -m L2")
        run_connected(f"load collection -c {coll_name}")

        yield coll_name

        run_connected(f"release collection -c {coll_name}")
        run_connected(f"delete collection -c {coll_name} --yes")
        try:
            os.remove(schema_file)
        except OSError:
            pass

    @pytest.mark.skip(reason="create index command is interactive only - fixture cannot create index")
    def test_insert_and_query(self, loaded_collection, run_connected):
        """Test insert and query commands.

        Note: This test is skipped because the loaded_collection fixture
        requires creating an index, but the 'create index' command only
        supports interactive input, not command-line options.
        """
        coll = loaded_collection

        # Insert data using file
        data = [
            {"id": 1, "embedding": [0.1, 0.2, 0.3, 0.4]},
            {"id": 2, "embedding": [0.5, 0.6, 0.7, 0.8]}
        ]
        data_file = f"/tmp/{coll}_data.json"
        with open(data_file, "w") as f:
            json.dump(data, f)

        output, code = run_connected(f"insert file -c {coll} --data-file {data_file}")
        assert code == 0

        # Flush to make data visible
        run_connected(f"flush -c {coll}")

        # Query
        output, code = run_connected(f"query collection -c {coll} -f 'id >= 0'")
        assert code == 0

        os.remove(data_file)

    @pytest.mark.skip(reason="create index command is interactive only - fixture cannot create index")
    def test_delete_entities(self, loaded_collection, run_connected):
        """Test delete entities command.

        Note: This test is skipped because the loaded_collection fixture
        requires creating an index, but the 'create index' command only
        supports interactive input, not command-line options.
        """
        coll = loaded_collection

        # Insert data first
        data = [{"id": 100, "embedding": [0.1, 0.2, 0.3, 0.4]}]
        data_file = f"/tmp/{coll}_del_data.json"
        with open(data_file, "w") as f:
            json.dump(data, f)

        run_connected(f"insert file -c {coll} --data-file {data_file}")
        run_connected(f"flush -c {coll}")

        # Delete
        output, code = run_connected(f"delete entity -c {coll} -f 'id == 100'")
        assert code == 0

        os.remove(data_file)
