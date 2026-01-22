"""Integration tests for resource group commands."""
import pytest


class TestResourceGroup:
    """Test resource group commands."""

    def test_list_resource_groups(self, run_connected):
        """Test list resource_groups command."""
        output, code = run_connected("list resource_groups")
        assert code == 0
        # Default resource group should exist
        assert "__default_resource_group" in output or "default" in output.lower()

    def test_show_resource_group(self, run_connected):
        """Test show resource_group command."""
        output, code = run_connected("show resource_group -n __default_resource_group")
        assert code == 0
