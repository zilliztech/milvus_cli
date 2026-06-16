import pytest


class TestSnapshot:
    def test_list_snapshots(self, run_connected):
        output, code = run_connected("list snapshots")
        assert code == 0

    def test_create_and_delete_snapshot(self, run_connected, unique_name):
        snapshot_name = f"snap_{unique_name}"

        output, code = run_connected(f"create snapshot -n {snapshot_name}")
        assert code == 0
        assert "successfully" in output.lower()

        output, code = run_connected("list snapshots")
        assert code == 0
        assert snapshot_name in output

        output, code = run_connected(f"delete snapshot -n {snapshot_name} --yes")
        assert code == 0
        assert "successfully" in output.lower()

    def test_show_snapshot(self, run_connected, unique_name):
        snapshot_name = f"snap_{unique_name}"

        run_connected(f"create snapshot -n {snapshot_name}")

        output, code = run_connected(f"show snapshot -n {snapshot_name}")
        assert code == 0

        run_connected(f"delete snapshot -n {snapshot_name} --yes")

    def test_pin_and_unpin_snapshot(self, run_connected, unique_name):
        snapshot_name = f"snap_{unique_name}"

        run_connected(f"create snapshot -n {snapshot_name}")

        output, code = run_connected(f"pin snapshot -n {snapshot_name}")
        assert code == 0
        assert "successfully" in output.lower()

        output, code = run_connected(f"unpin snapshot -n {snapshot_name}")
        assert code == 0
        assert "successfully" in output.lower()

        run_connected(f"delete snapshot -n {snapshot_name} --yes")

    def test_list_restore_jobs(self, run_connected):
        output, code = run_connected("list restore_jobs")
        assert code == 0
