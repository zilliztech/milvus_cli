"""Integration tests for data operation commands."""
import csv
import json
import os

import pytest


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
                {"name": "embedding", "type": "FLOAT_VECTOR", "dim": 4},
            ],
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
        output, code = run_connected(
            f"create index -c {coll_name} -f embedding -t FLAT -m L2"
        )
        assert code == 0, output
        run_connected(f"load collection -c {coll_name}")

        yield coll_name

        run_connected(f"release collection -c {coll_name}")
        run_connected(f"delete collection -c {coll_name} --yes")
        try:
            os.remove(schema_file)
        except OSError:
            pass

    def test_insert_and_query(self, loaded_collection, run_connected):
        """Test insert and query commands."""
        coll = loaded_collection

        # insert file expects a positional path to .csv (cells JSON-parsed per Fs.formatRowForData)
        data_file = f"/tmp/{coll}_data.csv"
        with open(data_file, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["id", "embedding"])
            w.writerow([1, json.dumps([0.1, 0.2, 0.3, 0.4])])
            w.writerow([2, json.dumps([0.5, 0.6, 0.7, 0.8])])

        output, code = run_connected(f"insert file -c {coll} {data_file}")
        assert code == 0, output

        # Flush to make data visible
        run_connected(f"flush -c {coll}")

        # Query (top-level `query`, not `query collection`)
        output, code = run_connected(f"query -c {coll} -e 'id >= 0'")
        assert code == 0, output

        os.remove(data_file)

    def test_delete_entities(self, loaded_collection, run_connected):
        """Test delete entities command."""
        coll = loaded_collection

        data_file = f"/tmp/{coll}_del_data.csv"
        with open(data_file, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["id", "embedding"])
            w.writerow([100, json.dumps([0.1, 0.2, 0.3, 0.4])])

        run_connected(f"insert file -c {coll} {data_file}")
        run_connected(f"flush -c {coll}")

        output, code = run_connected(
            f"delete entities -c {coll} -e 'id == 100' --yes"
        )
        assert code == 0, output

        os.remove(data_file)
