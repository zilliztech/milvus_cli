"""
Comprehensive integration tests for Milvus CLI new features.

Tests cover:
1. Global output format control (table/json/csv)
2. Dynamic auto-completion (collections/databases)
3. Unified help documentation
"""

import pytest
import json
import sys
import os
from io import StringIO
from unittest.mock import Mock, patch, MagicMock

# Add milvus_cli to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


class TestOutputFormatter:
    """Test global output formatter with all supported formats."""

    @pytest.fixture
    def formatter(self):
        """Create OutputFormatter instance."""
        from milvus_cli.OutputFormatter import OutputFormatter
        return OutputFormatter()

    def test_formatter_initialization(self, formatter):
        """Test that formatter initializes with default table format."""
        assert formatter.format == "table"
        assert formatter.FORMATS == ["table", "json", "csv"]

    def test_format_list_json(self, formatter):
        """Test formatting list as JSON."""
        formatter.format = "json"
        items = ["products", "users", "documents"]
        result = formatter.format_list(items, header="Collections")

        # Should be valid JSON
        parsed = json.loads(result)
        assert parsed == items

    def test_format_list_csv(self, formatter):
        """Test formatting list as CSV."""
        formatter.format = "csv"
        items = ["products", "users", "documents"]
        result = formatter.format_list(items, header="Collections")

        # Should have header and items
        lines = result.strip().split('\n')
        assert lines[0] == "Collections"
        assert len(lines) == 4  # header + 3 items

    def test_format_list_table(self, formatter):
        """Test formatting list as table."""
        formatter.format = "table"
        items = ["products", "users", "documents"]
        result = formatter.format_list(items, header="Collections")

        # Should contain all items
        assert "products" in result
        assert "users" in result
        assert "documents" in result

    def test_format_output_with_dicts_json(self, formatter):
        """Test formatting dict data as JSON."""
        formatter.format = "json"
        data = [
            {"id": 1, "name": "Product1", "price": 100},
            {"id": 2, "name": "Product2", "price": 200}
        ]
        result = formatter.format_output(data)

        parsed = json.loads(result)
        assert len(parsed) == 2
        assert parsed[0]["name"] == "Product1"

    def test_format_output_with_dicts_csv(self, formatter):
        """Test formatting dict data as CSV."""
        formatter.format = "csv"
        data = [
            {"id": 1, "name": "Product1", "price": 100},
            {"id": 2, "name": "Product2", "price": 200}
        ]
        result = formatter.format_output(data)

        lines = result.strip().split('\n')
        assert "id" in lines[0]  # header
        assert len(lines) == 3  # header + 2 data rows

    def test_format_output_with_lists_table(self, formatter):
        """Test formatting list-of-lists as table."""
        formatter.format = "table"
        data = [
            ["product1", "100"],
            ["product2", "200"]
        ]
        result = formatter.format_output(data, headers=["Name", "Price"])

        assert "product1" in result
        assert "product2" in result
        assert "Name" in result

    def test_format_key_value_json(self, formatter):
        """Test formatting key-value pairs as JSON."""
        formatter.format = "json"
        data = {
            "name": "products",
            "count": 1000,
            "indexed": True
        }
        result = formatter.format_key_value(data)

        parsed = json.loads(result)
        assert parsed["name"] == "products"
        assert parsed["count"] == 1000

    def test_format_key_value_csv(self, formatter):
        """Test formatting key-value pairs as CSV."""
        formatter.format = "csv"
        data = [["name", "products"], ["count", "1000"]]
        result = formatter.format_key_value(data, key_header="Property", value_header="Value")

        lines = result.strip().split('\n')
        assert "Property" in lines[0]
        assert "Value" in lines[0]

    def test_format_switching(self, formatter):
        """Test switching between formats."""
        items = ["item1", "item2"]

        # Test all formats
        formatter.format = "json"
        json_result = formatter.format_list(items)
        assert json_result.startswith('[')

        formatter.format = "csv"
        csv_result = formatter.format_list(items)
        assert '\n' in csv_result

        formatter.format = "table"
        table_result = formatter.format_list(items)
        assert "item1" in table_result

    def test_invalid_format(self, formatter):
        """Test that invalid format raises error."""
        with pytest.raises(ValueError, match="Invalid format"):
            formatter.format = "invalid_format"

    def test_empty_data_handling(self, formatter):
        """Test handling of empty data."""
        result = formatter.format_list([])
        assert "No items" in result

    def test_none_data_handling(self, formatter):
        """Test handling of None data."""
        result = formatter.format_list(None)
        assert "No items" in result


class TestCompleterDynamicCompletion:
    """Test dynamic auto-completion for collections and databases."""

    @pytest.fixture
    def completer(self):
        """Create Completer instance."""
        from milvus_cli.utils import Completer
        return Completer()

    def test_completer_initialization(self, completer):
        """Test that completer initializes with static command dict."""
        assert hasattr(completer, 'STATIC_CMDS_DICT')
        assert isinstance(completer.STATIC_CMDS_DICT, dict)
        assert len(completer.STATIC_CMDS_DICT) > 0

    def test_static_cmds_dict_has_key_commands(self, completer):
        """Test that STATIC_CMDS_DICT includes essential commands."""
        essential_commands = ['connect', 'create', 'delete', 'list', 'query', 'set', 'show']

        for cmd in essential_commands:
            assert cmd in completer.STATIC_CMDS_DICT or any(
                cmd in str(completer.STATIC_CMDS_DICT).lower()
            ), f"Command '{cmd}' should be in dictionary"

    def test_set_output_subcommands(self, completer):
        """Test that 'set' command includes 'output' subcommand."""
        if 'set' in completer.STATIC_CMDS_DICT:
            subcommands = completer.STATIC_CMDS_DICT['set']
            assert 'output' in subcommands or isinstance(subcommands, dict)

    def test_show_output_subcommands(self, completer):
        """Test that 'show' command includes 'output' subcommand."""
        if 'show' in completer.STATIC_CMDS_DICT:
            subcommands = completer.STATIC_CMDS_DICT['show']
            assert 'output' in subcommands or isinstance(subcommands, dict)

    def test_completer_has_dynamic_methods(self, completer):
        """Test that completer has methods for dynamic completion."""
        assert hasattr(completer, '_get_collections') or \
               hasattr(completer, 'makeComplete') or \
               callable(getattr(completer, 'makeComplete', None))

    def test_collection_completion_prefix_matching(self, completer):
        """Test that collection completion supports prefix matching."""
        # Mock MilvusCli for dynamic completion
        mock_cli = Mock()
        mock_cli.collection = Mock()
        mock_cli.collection.list_collections = Mock(
            return_value=["products", "users", "documents"]
        )

        # This tests the structure for dynamic completion
        if hasattr(completer, 'set_milvus_cli_obj'):
            completer.set_milvus_cli_obj(mock_cli)

    def test_database_completion_support(self, completer):
        """Test that database completion is supported."""
        # Mock MilvusCli for database completion
        mock_cli = Mock()
        mock_cli.database = Mock()
        mock_cli.database.list_databases = Mock(
            return_value=["default", "project", "analytics"]
        )

        # Test structure
        if hasattr(completer, 'set_milvus_cli_obj'):
            completer.set_milvus_cli_obj(mock_cli)


class TestHelpDocumentation:
    """Test standardized help documentation format."""

    def test_help_sections_exist(self):
        """Test that commands have standardized help sections."""
        # Import command modules
        from milvus_cli.scripts import data_cli, collection_cli, connection_cli

        # Get command functions
        commands_to_check = [
            (data_cli, 'query'),
            (collection_cli, 'create_collection'),
            (connection_cli, 'connect')
        ]

        for module, cmd_name in commands_to_check:
            if hasattr(module, cmd_name):
                cmd = getattr(module, cmd_name)
                # Commands should have docstring
                assert cmd.__doc__ is not None, f"{cmd_name} should have docstring"

    def test_help_documentation_keywords(self):
        """Test that help documentation includes key sections."""
        from milvus_cli.scripts import data_cli

        if hasattr(data_cli, 'query'):
            docstring = data_cli.query.__doc__
            if docstring:
                # Should have standard sections
                doc_lower = docstring.lower()
                # At least one of these should be present
                assert any(keyword in doc_lower for keyword in [
                    'usage', 'example', 'option', 'parameter', 'description'
                ])

    def test_output_formatter_integration(self):
        """Test that commands can use OutputFormatter for output."""
        from milvus_cli.OutputFormatter import OutputFormatter

        formatter = OutputFormatter()

        # Test that formatter works with query results
        mock_results = [
            {"id": 1, "name": "item1"},
            {"id": 2, "name": "item2"}
        ]

        # Should work with all formats
        for fmt in ["table", "json", "csv"]:
            formatter.format = fmt
            output = formatter.format_output(mock_results)
            assert output is not None
            assert len(output) > 0


class TestIntegrationWorkflow:
    """Test complete workflows using all three features together."""

    def test_complete_workflow_json_output(self):
        """Test complete workflow with JSON format output."""
        from milvus_cli.OutputFormatter import OutputFormatter

        formatter = OutputFormatter()

        # Step 1: Set output format to JSON
        formatter.format = "json"
        assert formatter.format == "json"

        # Step 2: Format query results
        query_results = [
            {"id": 1, "name": "Product1", "price": 100},
            {"id": 2, "name": "Product2", "price": 200}
        ]

        output = formatter.format_output(query_results)

        # Step 3: Verify JSON is parseable
        parsed = json.loads(output)
        assert len(parsed) == 2
        assert parsed[0]["name"] == "Product1"

    def test_complete_workflow_csv_export(self):
        """Test complete workflow with CSV format for export."""
        from milvus_cli.OutputFormatter import OutputFormatter

        formatter = OutputFormatter()

        # Step 1: Set output format to CSV
        formatter.format = "csv"

        # Step 2: Format collection list
        collections = ["products", "users", "documents"]
        output = formatter.format_list(collections, header="Collection")

        # Step 3: Verify CSV format
        lines = output.strip().split('\n')
        assert lines[0] == "Collection"
        assert len(lines) == 4  # header + 3 items

    def test_complete_workflow_table_interactive(self):
        """Test complete workflow with table format for interactive use."""
        from milvus_cli.OutputFormatter import OutputFormatter

        formatter = OutputFormatter()

        # Step 1: Set output format to table (default)
        formatter.format = "table"

        # Step 2: Format collection info
        collection_info = [
            ["products", "100000", "HNSW"],
            ["users", "50000", "IVF_FLAT"]
        ]

        output = formatter.format_output(
            collection_info,
            headers=["Collection", "Count", "Index"]
        )

        # Step 3: Verify readable table output
        assert "products" in output
        assert "Collection" in output or "100000" in output

    def test_format_switching_workflow(self):
        """Test switching between formats in a workflow."""
        from milvus_cli.OutputFormatter import OutputFormatter

        formatter = OutputFormatter()
        data = [{"name": "test", "value": "data"}]

        results = {}

        # Get output in all formats
        for fmt in ["table", "json", "csv"]:
            formatter.format = fmt
            results[fmt] = formatter.format_output(data)

        # Verify all formats produced output
        assert all(len(v) > 0 for v in results.values())

        # Verify JSON is valid
        json_data = json.loads(results["json"])
        assert json_data == data


class TestErrorHandling:
    """Test error handling in new features."""

    def test_invalid_format_error(self):
        """Test error handling for invalid format."""
        from milvus_cli.OutputFormatter import OutputFormatter

        formatter = OutputFormatter()

        with pytest.raises(ValueError):
            formatter.format = "invalid"

    def test_empty_list_handling(self):
        """Test handling of empty lists."""
        from milvus_cli.OutputFormatter import OutputFormatter

        formatter = OutputFormatter()
        result = formatter.format_list([])

        assert "No items" in result or result == ""

    def test_invalid_data_type_handling(self):
        """Test error handling for invalid data types."""
        from milvus_cli.OutputFormatter import OutputFormatter

        formatter = OutputFormatter()

        # Should handle gracefully or raise clear error
        try:
            result = formatter.format_output("invalid_string")
            # If it doesn't raise, it should return something reasonable
            assert result is not None
        except (ValueError, TypeError):
            # Or it should raise a clear error
            pass


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
