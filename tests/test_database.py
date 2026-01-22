"""Integration tests for database commands."""
import pytest


class TestDatabase:
    """Test database-related commands."""

    def test_list_databases(self, run_connected):
        """Test list databases command."""
        output, code = run_connected("list databases")
        assert code == 0
        assert "default" in output

    def test_create_and_delete_database(self, run_connected, unique_name):
        """Test create and delete database."""
        db_name = f"db_{unique_name}"

        # Create database
        output, code = run_connected(f"create database -db {db_name}")
        assert code == 0
        assert "success" in output.lower()

        # Verify it exists
        output, code = run_connected("list databases")
        assert db_name in output

        # Delete database
        output, code = run_connected(f"delete database -db {db_name} --yes")
        assert code == 0

    def test_use_database(self, run_connected):
        """Test use database command."""
        output, code = run_connected("use database -db default")
        assert code == 0

    def test_show_database(self, run_connected):
        """Test show database command."""
        output, code = run_connected("show database -db default")
        assert code == 0
        assert "default" in output

    def test_show_current_database(self, run_connected):
        """Test show database without -db shows current."""
        output, code = run_connected("show database")
        assert code == 0
