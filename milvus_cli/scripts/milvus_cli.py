"""
Milvus CLI entry point.

Importing each module triggers Click command registration via decorators.
"""
import sys

# Import modules to register CLI commands (side-effect imports)
from . import connection_cli as _connection_cli  # noqa: F401
from . import helper_cli as _helper_cli  # noqa: F401
from . import database_client_cli as _database_client_cli  # noqa: F401
from . import collection_client_cli as _collection_client_cli  # noqa: F401
from . import index_client_cli as _index_client_cli  # noqa: F401
from . import data_client_cli as _data_client_cli  # noqa: F401
from . import user_client_cli as _user_client_cli  # noqa: F401
from . import alias_client_cli as _alias_client_cli  # noqa: F401
from . import partition_client_cli as _partition_client_cli  # noqa: F401
from . import role_client_cli as _role_client_cli  # noqa: F401
from . import resource_group_cli as _resource_group_cli  # noqa: F401
from . import privilege_group_cli as _privilege_group_cli  # noqa: F401

from .helper_client_cli import cli, runCliPrompt  # noqa: F401

if __name__ == "__main__":
    if len(sys.argv) > 1:
        cli()
    else:
        runCliPrompt()
