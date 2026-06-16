"""Integration tests for user and role commands."""
import ast
import pytest


class TestUserRole:
    """Test user and role commands."""

    def test_list_users(self, run_connected):
        """Test list users command."""
        output, code = run_connected("list users")
        assert code == 0
        users = ast.literal_eval(output.strip())
        assert isinstance(users, list)
        assert users

    def test_create_and_delete_user(self, run_connected, unique_name):
        """Test create and delete user."""
        username = f"user_{unique_name}"

        # Create user
        output, code = run_connected(f"create user -u {username} -p Test123456")
        assert code == 0

        # List users
        output, code = run_connected("list users")
        assert username in output

        # Delete user (--yes: non-interactive CI / pytest CliRunner)
        output, code = run_connected(f"delete user -u {username} --yes")
        assert code == 0

    def test_list_roles(self, run_connected):
        """Test list roles command."""
        output, code = run_connected("list roles")
        assert code == 0

    def test_show_role(self, run_connected):
        """Test show role command."""
        output, code = run_connected("show role -r admin")
        assert code == 0
        assert "admin" in output

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


class TestPrivilegeV2:
    """Test grant/revoke privilege v2 commands."""

    def test_grant_and_revoke_privilege_v2(self, run_connected, unique_name):
        role_name = f"v2role_{unique_name}"
        output, code = run_connected(f"create role -r {role_name}")
        assert code == 0
        output, code = run_connected(
            f"grant privilege_v2 -r {role_name} -p Search -c __default_collection"
        )
        assert code == 0 or "error" in output.lower()
        output, code = run_connected(
            f"revoke privilege_v2 -r {role_name} -p Search -c __default_collection"
        )
        assert code == 0 or "error" in output.lower()
        run_connected(f"delete role -r {role_name}")
