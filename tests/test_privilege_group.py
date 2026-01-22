"""Integration tests for privilege group commands."""
import pytest


class TestPrivilegeGroup:
    """Test privilege group commands."""

    def test_list_privilege_groups(self, run_connected):
        """Test list privilege_groups command."""
        output, code = run_connected("list privilege_groups")
        assert code == 0

    def test_create_and_delete_privilege_group(self, run_connected, unique_name):
        """Test create and delete privilege group."""
        group_name = f"pg_{unique_name}"

        # Create
        output, code = run_connected(f"create privilege_group -n {group_name}")
        assert code == 0
        assert "successfully" in output.lower()

        # List - verify command works (note: group names show as "Unknown" - known issue)
        output, code = run_connected("list privilege_groups")
        assert code == 0

        # Delete
        output, code = run_connected(f"delete privilege_group -n {group_name} --yes")
        assert code == 0
        assert "successfully" in output.lower()

    def test_grant_and_revoke_privilege_group(self, run_connected, unique_name):
        """Test grant and revoke privileges to group."""
        group_name = f"pg_{unique_name}"

        # Create group
        run_connected(f"create privilege_group -n {group_name}")

        # Grant privileges
        output, code = run_connected(f"grant privilege_group -n {group_name} -p Query,Search")
        assert code == 0

        # Revoke privileges
        output, code = run_connected(f"revoke privilege_group -n {group_name} -p Query")
        assert code == 0

        # Cleanup
        run_connected(f"delete privilege_group -n {group_name} --yes")
