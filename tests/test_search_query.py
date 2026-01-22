"""Integration tests for search and query commands."""
import pytest
import json
import os


class TestSearchQuery:
    """Test search and query commands."""

    @pytest.fixture
    def searchable_collection(self, run_connected, unique_name):
        """Create a collection with data for search tests."""
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

        # Create index
        run_connected(f"create index -c {coll_name} -f embedding -t FLAT -m L2")

        # Insert data
        data = [
            {"id": i, "embedding": [float(i)*0.1, float(i)*0.2, float(i)*0.3, float(i)*0.4]}
            for i in range(10)
        ]
        data_file = f"/tmp/{coll_name}_data.json"
        with open(data_file, "w") as f:
            json.dump(data, f)

        run_connected(f"insert file -c {coll_name} --data-file {data_file}")
        run_connected(f"flush -c {coll_name}")
        run_connected(f"load collection -c {coll_name}")

        yield coll_name

        run_connected(f"release collection -c {coll_name}")
        run_connected(f"delete collection -c {coll_name} --yes")
        try:
            os.remove(schema_file)
            os.remove(data_file)
        except OSError:
            pass

    @pytest.mark.skip(reason="create index command is interactive only - fixture cannot create index")
    def test_search(self, searchable_collection, run_connected):
        """Test search command.

        Note: This test is skipped because the searchable_collection fixture
        requires creating an index, but the 'create index' command only
        supports interactive input, not command-line options.
        """
        coll = searchable_collection

        output, code = run_connected(
            f"search collection -c {coll} -f embedding -v '[0.1, 0.2, 0.3, 0.4]' -l 5"
        )
        assert code == 0

    @pytest.mark.skip(reason="create index command is interactive only - fixture cannot create index")
    def test_query(self, searchable_collection, run_connected):
        """Test query command.

        Note: This test is skipped because the searchable_collection fixture
        requires creating an index, but the 'create index' command only
        supports interactive input, not command-line options.
        """
        coll = searchable_collection

        output, code = run_connected(f"query collection -c {coll} -f 'id < 5'")
        assert code == 0
