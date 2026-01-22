import sys
from .connection_cli import *
from .helper_cli import *
from .database_client_cli import *
from .collection_client_cli import *
from .index_client_cli import *
from .data_client_cli import *
from .user_client_cli import *
from .alias_client_cli import *
from .partition_client_cli import *
from .role_client_cli import *
from .resource_group_cli import *
from .privilege_group_cli import *

if __name__ == "__main__":
    # If command line arguments are provided, run CLI directly
    # Otherwise, start interactive REPL
    if len(sys.argv) > 1:
        cli()
    else:
        runCliPrompt()
