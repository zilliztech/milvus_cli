"""Integration tests for user and role commands."""
import pytest


class TestUserRole:
    """Test user and role commands."""

    def test_list_users(self, run_connected):
        """Test list users command."""
        output, code = run_connected("list users")
        assert code == 0
        assert "root" in output

    def test_create_and_delete_user(self, run_connected, unique_name):
        """Test create and delete user."""
        username = f"user_{unique_name}"

        # Create user
        output, code = run_connected(f"create user -u {username} -p Test123456")
        assert code == 0

        # List users
        output, code = run_connected("list users")
        assert username in output

        # Delete user
        output, code = run_connected(f"delete user -u {username}")
        assert code == 0

    def test_list_roles(self, run_connected):
        """Test list roles command."""
        output, code = run_connected("list roles")
        assert code == 0

    def test_create_and_delete_role(self, run_connected, unique_name):
        """Test create and delete role."""
        role_name = f"role_{unique_name}"

        # Create role
        output, code = run_connected(f"create role -r {role_name}")
        assert code == 0

        # List roles
        output, code = run_connected("list roles")
        assert role_name in output

        # Delete role
        output, code = run_connected(f"delete role -r {role_name}")
        assert code == 0
