from types import SimpleNamespace

from click.testing import CliRunner

import milvus_cli.DataClient as data_client_module
from milvus_cli.DataClient import MilvusClientData
from milvus_cli.scripts import init_client_cli
from milvus_cli.scripts.milvus_client_cli import cli


class DummyConnection:
    def __init__(self):
        self.ensured = False

    def get_client(self):
        return object()

    def ensure_orm_connection(self):
        self.ensured = True
        return "bulk_alias"


def test_bulk_insert_uses_utility_connection(monkeypatch):
    calls = {}

    def fake_do_bulk_insert(**kwargs):
        calls.update(kwargs)
        return 42

    connection = DummyConnection()
    data = MilvusClientData(connection)
    monkeypatch.setattr(data_client_module.utility, "do_bulk_insert", fake_do_bulk_insert)

    task_id = data.bulk_insert("test_collection", ["data.json"], "_default")

    assert task_id == 42
    assert connection.ensured is True
    assert calls == {
        "collection_name": "test_collection",
        "partition_name": "_default",
        "files": ["data.json"],
        "using": "bulk_alias",
    }


def test_bulk_insert_cli_returns_nonzero_on_error():
    def failing_bulk_insert(*_args, **_kwargs):
        raise RuntimeError("bulk insert failed")

    old_instance = init_client_cli._global_cli_instance
    init_client_cli._global_cli_instance = SimpleNamespace(
        data=SimpleNamespace(bulk_insert=failing_bulk_insert)
    )
    try:
        result = CliRunner().invoke(
            cli,
            ["bulk_insert", "-c", "test_collection", "-f", "data.json"],
        )
    finally:
        init_client_cli._global_cli_instance = old_instance

    assert result.exit_code != 0
    assert "bulk insert failed" in result.output
