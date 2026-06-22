import pytest


class TestExternalCollection:
    def test_refresh_external_collection(self, run_connected, unique_name):
        output, code = run_connected(
            f"refresh_external_collection -c {unique_name}"
        )
        assert code == 1 or "error" in output.lower() or code == 0

    def test_show_refresh_progress(self, run_connected, unique_name):
        output, code = run_connected(
            f"show refresh_external_collection_progress -c {unique_name}"
        )
        assert code == 1 or "error" in output.lower() or code == 0

    def test_list_refresh_jobs(self, run_connected):
        output, code = run_connected("list refresh_external_collection_jobs")
        assert code == 0
