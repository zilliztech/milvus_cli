"""Integration tests for new collection commands."""
import pytest
from types import SimpleNamespace
from milvus_cli.scripts import init_client_cli
from milvus_cli.scripts.milvus_client_cli import cli


class TestRunAnalyzer:
    """Test run_analyzer command."""

    def test_run_analyzer_standard(self, cli_runner):
        """Test run_analyzer with standard analyzer."""
        old_instance = init_client_cli._global_cli_instance
        calls = {}

        class FakeCollection:
            def run_analyzer(self, texts, params):
                calls["texts"] = texts
                calls["params"] = params
                return ["hello", "world"]

        init_client_cli._global_cli_instance = SimpleNamespace(
            collection=FakeCollection()
        )
        try:
            result = cli_runner.invoke(
                cli, ["run_analyzer", "-t", "hello world", "-a", "standard"]
            )
        finally:
            init_client_cli._global_cli_instance = old_instance

        assert result.exit_code == 0
        assert calls["texts"] == ["hello world"]
        assert calls["params"] == {"tokenizer": "standard"}

    def test_run_analyzer_json_params(self, cli_runner):
        """Test run_analyzer with JSON analyzer params."""
        old_instance = init_client_cli._global_cli_instance
        calls = {}

        class FakeCollection:
            def run_analyzer(self, texts, params):
                calls["texts"] = texts
                calls["params"] = params
                return ["test"]

        init_client_cli._global_cli_instance = SimpleNamespace(
            collection=FakeCollection()
        )
        try:
            result = cli_runner.invoke(
                cli,
                ["run_analyzer", "-t", "test text", "-a", '{"tokenizer": "jieba"}'],
            )
        finally:
            init_client_cli._global_cli_instance = old_instance

        assert result.exit_code == 0
        assert calls["params"] == {"tokenizer": "jieba"}


class TestOptimize:
    """Test optimize command."""

    def test_optimize_command(self, cli_runner):
        """Test optimize command path."""
        old_instance = init_client_cli._global_cli_instance
        calls = {}

        class FakeCollection:
            def optimize(self, collectionName):
                calls["collection"] = collectionName
                return "ok"

        init_client_cli._global_cli_instance = SimpleNamespace(
            collection=FakeCollection()
        )
        try:
            result = cli_runner.invoke(
                cli, ["optimize", "-c", "test_collection"]
            )
        finally:
            init_client_cli._global_cli_instance = old_instance

        assert result.exit_code == 0
        assert calls["collection"] == "test_collection"


class TestRefreshLoad:
    """Test refresh_load command."""

    def test_refresh_load_command(self, cli_runner):
        """Test refresh_load command path."""
        old_instance = init_client_cli._global_cli_instance
        calls = {}

        class FakeCollection:
            def refresh_load(self, collectionName):
                calls["collection"] = collectionName
                return "ok"

        init_client_cli._global_cli_instance = SimpleNamespace(
            collection=FakeCollection()
        )
        try:
            result = cli_runner.invoke(
                cli, ["refresh_load", "-c", "test_collection"]
            )
        finally:
            init_client_cli._global_cli_instance = old_instance

        assert result.exit_code == 0
        assert calls["collection"] == "test_collection"


class TestCollectionFunction:
    """Test add/drop collection function commands."""

    def test_add_collection_function_command(self, cli_runner):
        """Test add_collection_function command."""
        old_instance = init_client_cli._global_cli_instance
        calls = {}

        class FakeCollection:
            def add_collection_function(self, collectionName, function):
                calls["collection"] = collectionName
                calls["function"] = function
                return "ok"

        init_client_cli._global_cli_instance = SimpleNamespace(
            collection=FakeCollection()
        )
        try:
            result = cli_runner.invoke(
                cli,
                [
                    "add_collection_function",
                    "-c", "test_collection",
                    "-fn", "bm25_fn",
                    "-ft", "BM25",
                    "-if", "text",
                    "-of", "embedding",
                ],
            )
        finally:
            init_client_cli._global_cli_instance = old_instance

        assert result.exit_code == 0
        assert calls["collection"] == "test_collection"

    def test_drop_collection_function_command(self, cli_runner):
        """Test drop_collection_function command."""
        old_instance = init_client_cli._global_cli_instance
        calls = {}

        class FakeCollection:
            def drop_collection_function(self, collectionName, functionName):
                calls["collection"] = collectionName
                calls["function_name"] = functionName
                return "ok"

        init_client_cli._global_cli_instance = SimpleNamespace(
            collection=FakeCollection()
        )
        try:
            result = cli_runner.invoke(
                cli,
                [
                    "drop_collection_function",
                    "-c", "test_collection",
                    "-fn", "bm25_fn",
                ],
            )
        finally:
            init_client_cli._global_cli_instance = old_instance

        assert result.exit_code == 0
        assert calls["collection"] == "test_collection"
        assert calls["function_name"] == "bm25_fn"


class TestCollectionField:
    """Test add/drop collection field commands."""

    def test_add_collection_field_command(self, cli_runner):
        """Test add_collection_field command."""
        old_instance = init_client_cli._global_cli_instance
        calls = {}

        class FakeCollection:
            def add_collection_field(self, collectionName, fieldSchema):
                calls["collection"] = collectionName
                calls["field_schema"] = fieldSchema
                return "ok"

        init_client_cli._global_cli_instance = SimpleNamespace(
            collection=FakeCollection()
        )
        try:
            result = cli_runner.invoke(
                cli,
                [
                    "add_collection_field",
                    "-c", "test_collection",
                    "-f", "new_field",
                    "-dt", "INT64",
                ],
            )
        finally:
            init_client_cli._global_cli_instance = old_instance

        assert result.exit_code == 0
        assert calls["collection"] == "test_collection"

    def test_drop_collection_field_command(self, cli_runner):
        """Test drop_collection_field command."""
        old_instance = init_client_cli._global_cli_instance
        calls = {}

        class FakeCollection:
            def drop_collection_field(self, collectionName, fieldName):
                calls["collection"] = collectionName
                calls["field_name"] = fieldName
                return "ok"

        init_client_cli._global_cli_instance = SimpleNamespace(
            collection=FakeCollection()
        )
        try:
            result = cli_runner.invoke(
                cli,
                [
                    "drop_collection_field",
                    "-c", "test_collection",
                    "-f", "old_field",
                ],
            )
        finally:
            init_client_cli._global_cli_instance = old_instance

        assert result.exit_code == 0
        assert calls["collection"] == "test_collection"
        assert calls["field_name"] == "old_field"
