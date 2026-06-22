import pytest


class TestSnapshot:
    def test_list_snapshots(self, run_connected):
        output, code = run_connected("list snapshots")
        assert code == 0

    def test_list_snapshots_with_collection(self, run_connected, unique_name):
        output, code = run_connected(f"list snapshots -c {unique_name}")
        assert code == 0

    def test_create_snapshot_missing_collection(self, run_connected, unique_name):
        snapshot_name = f"snap_{unique_name}"
        output, code = run_connected(f"create snapshot -n {snapshot_name}")
        assert code != 0 or "error" in output.lower() or "required" in output.lower()

    def test_restore_snapshot_needs_args(self, run_connected):
        output, code = run_connected("restore_snapshot")
        assert code != 0

    def test_list_restore_jobs(self, run_connected):
        output, code = run_connected("list_restore_jobs")
        assert code == 0

    def test_show_restore_state_needs_id(self, run_connected):
        output, code = run_connected("show_restore_state")
        assert code != 0

    def test_unpin_snapshot_needs_id(self, run_connected):
        output, code = run_connected("unpin_snapshot")
        assert code != 0
