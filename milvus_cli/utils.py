import readline
import re
import os
from string import Template
from pymilvus import __version__
from Types import ParameterException


def getPackageVersion():
    try:
        from importlib.metadata import PackageNotFoundError, version
        return version("milvus_cli")
    except Exception:
        # Fallback: Read version from setup.py when package is not installed
        try:
            setup_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "setup.py")
            with open(setup_path, "r") as f:
                content = f.read()
                match = re.search(r'version\s*=\s*["\']([^"\']+)["\']', content)
                if match:
                    return match.group(1)
        except Exception:
            pass
        return "dev"


class Completer(object):
    """
    Auto-completion handler with dynamic completion support.

    Features:
    - Auto-generates command dict from Click CLI
    - Supports dynamic collection/database name completion
    - Falls back to filesystem path completion
    """

    RE_SPACE = re.compile(r".*\s+$", re.M)

    # Static fallback dict (used if CLI not available)
    STATIC_CMDS_DICT = {
        "grant": ["privilege", "role"],
        "revoke": ["privilege", "role"],
        "clear": [],
        "connect": [],
        "create": [
            "alias",
            "database",
            "collection",
            "partition",
            "index",
            "user",
            "role",
        ],
        "delete": [
            "alias",
            "connection_history",
            "database",
            "collection",
            "entities",
            "ids",
            "partition",
            "index",
            "user",
            "role",
        ],
        "exit": [],
        "help": [],
        "history": ["clear"],
        "insert": ["file", "row"],
        "upsert": ["file", "row"],
        "list": [
            "connections",
            "connection_history",
            "collections",
            "databases",
            "partitions",
            "indexes",
            "users",
            "aliases",
            "roles",
            "grants",
            "bulk_insert_tasks",
        ],
        "load": ["collection", "partition"],
        "query": [],
        "get": [],
        "release": ["collection", "partition"],
        "search": [],
        "show": [
            "index_progress",
            "loading_progress",
            "query_segment_info",
            "compaction_state",
            "compaction_plans",
            "collection",
            "partition",
            "index",
            "output",
            "bulk_insert_state",
            "replicas",
            "load_state",
        ],
        "rename": ["collection"],
        "use": ["database"],
        "set": ["output"],
        "version": [],
        "server_version": [],
        "flush": [],
        "compact": [],
        "truncate": [],
        "wait_for_loading": [],
        "bulk_insert": [],
    }

    # Argument completions for specific commands
    ARGUMENT_COMPLETIONS = {
        "set": {
            "output": ["table", "json", "csv"],
        },
    }

    def __init__(self, cli_instance=None, milvus_cli_obj=None) -> None:
        super().__init__()
        self.milvus_cli_obj = milvus_cli_obj
        self.CMDS_DICT = self._generate_cmds_dict(cli_instance)
        self.COMMANDS = list(self.CMDS_DICT.keys())
        self.createCompleteFuncs(self.CMDS_DICT)

    def _generate_cmds_dict(self, cli_instance):
        """
        Generate command dictionary from Click CLI instance.
        Falls back to static dict if CLI not available.
        """
        if cli_instance is None:
            return self.STATIC_CMDS_DICT.copy()

        try:
            cmds = {}
            for name, cmd in cli_instance.commands.items():
                if hasattr(cmd, 'commands'):
                    # It's a group - get subcommands
                    cmds[name] = list(cmd.commands.keys())
                else:
                    # It's a standalone command
                    cmds[name] = []
            return cmds
        except Exception:
            return self.STATIC_CMDS_DICT.copy()

    def set_milvus_cli_obj(self, obj):
        """Set the MilvusCli object for dynamic completions."""
        self.milvus_cli_obj = obj

    def _get_collections(self):
        """Get list of collections for dynamic completion."""
        if self.milvus_cli_obj is None:
            return []
        try:
            return self.milvus_cli_obj.collection.list_collections()
        except Exception:
            return []

    def _get_databases(self):
        """Get list of databases for dynamic completion."""
        if self.milvus_cli_obj is None:
            return []
        try:
            return self.milvus_cli_obj.database.list_databases()
        except Exception:
            return []

    def _get_partitions(self, collection_name):
        """Get list of partitions for a collection."""
        if self.milvus_cli_obj is None:
            return []
        try:
            return self.milvus_cli_obj.partition.list_partitions(collection_name)
        except Exception:
            return []

    def createCompleteFuncs(self, cmdDict):
        for cmd in cmdDict:
            sub_cmds = cmdDict[cmd]
            complete_example = self.makeComplete(cmd, sub_cmds)
            setattr(self, "complete_%s" % cmd, complete_example)

    def makeComplete(self, cmd, sub_cmds):
        def f_complete(args):
            f"Completions for the {cmd} command."
            if not args:
                return self._complete_path(".")

            # Dynamic completion based on command context
            if len(args) == 1:
                current_arg = args[0]

                # Check for option flags that need collection name
                if current_arg in ["-c", "--collection", "--collection-name"]:
                    return self._get_collections()

                # Check for option flags that need database name
                if current_arg in ["-db", "--db_name"]:
                    return self._get_databases()

                # Complete subcommands
                if cmd not in ["import", "query", "search", "get"]:
                    return self._complete_2nd_level(sub_cmds, current_arg)

            # After subcommand, check for argument completions or option values
            if len(args) >= 2:
                subcommand = args[0]
                current_arg = args[-1]
                prev_arg = args[-2] if len(args) >= 2 else ""

                # Check for argument completions (e.g., "set output table/json/csv")
                if cmd in self.ARGUMENT_COMPLETIONS:
                    if subcommand in self.ARGUMENT_COMPLETIONS[cmd]:
                        arg_values = self.ARGUMENT_COMPLETIONS[cmd][subcommand]
                        if current_arg:
                            return [v + " " for v in arg_values if v.startswith(current_arg)]
                        return [v + " " for v in arg_values]

                # Collection name completion
                if prev_arg in ["-c", "--collection", "--collection-name"]:
                    collections = self._get_collections()
                    if current_arg:
                        return [c for c in collections if c.startswith(current_arg)]
                    return collections

                # Database name completion
                if prev_arg in ["-db", "--db_name"]:
                    databases = self._get_databases()
                    if current_arg:
                        return [d for d in databases if d.startswith(current_arg)]
                    return databases

            return self._complete_path(args[-1])

        return f_complete

    def _listdir(self, root):
        "List directory 'root' appending the path separator to subdirs."
        res = []
        for name in os.listdir(root):
            path = os.path.join(root, name)
            if os.path.isdir(path):
                name += os.sep
            res.append(name)
        return res

    def _complete_path(self, path=None):
        "Perform completion of filesystem path."
        if not path:
            return self._listdir(".")
        dirname, rest = os.path.split(path)
        tmp = dirname if dirname else "."
        res = [
            os.path.join(dirname, p) for p in self._listdir(tmp) if p.startswith(rest)
        ]
        # more than one match, or single match which does not exist (typo)
        if len(res) > 1 or not os.path.exists(path):
            return res
        # resolved to a single directory, so return list of files below it
        if os.path.isdir(path):
            return [os.path.join(path, p) for p in self._listdir(path)]
        # exact file match terminates this completion
        return [path + " "]

    def _complete_2nd_level(self, SUB_COMMANDS=[], cmd=None):
        if not cmd:
            return [c + " " for c in SUB_COMMANDS]
        res = [c for c in SUB_COMMANDS if c.startswith(cmd)]
        if len(res) > 1 or not (cmd in SUB_COMMANDS):
            return res
        return [cmd + " "]

    # def complete_create(self, args):
    #     "Completions for the 'create' command."
    #     if not args:
    #         return self._complete_path('.')
    #     sub_cmds = ['collection', 'partition', 'index']
    #     if len(args) <= 1:
    #         return self._complete_2nd_level(sub_cmds, args[-1])
    #     return self._complete_path(args[-1])

    def complete(self, text, state):
        "Generic readline completion entry point."
        buffer = readline.get_line_buffer()
        line = readline.get_line_buffer().split()
        # show all commands
        if not line:
            return [c + " " for c in self.COMMANDS][state]
        # account for last argument ending in a space
        if self.RE_SPACE.match(buffer):
            line.append("")
        # resolve command to the implementation function
        cmd = line[0].strip()
        if cmd in self.COMMANDS:
            impl = getattr(self, "complete_%s" % cmd)
            args = line[1:]
            if args:
                return (impl(args) + [None])[state]
            return [cmd + " "][state]
        results = [c + " " for c in self.COMMANDS if c.startswith(cmd)] + [None]
        return results[state]


msgTemp = Template(
    r"""


  __  __ _ _                    ____ _     ___
 |  \/  (_) |_   ___   _ ___   / ___| |   |_ _|
 | |\/| | | \ \ / / | | / __| | |   | |    | |
 | |  | | | |\ V /| |_| \__ \ | |___| |___ | |
 |_|  |_|_|_| \_/  \__,_|___/  \____|_____|___|

Milvus cli version: ${cli}
Pymilvus version: ${py}

Learn more: https://github.com/zilliztech/milvus_cli.

"""
)

WELCOME_MSG = msgTemp.safe_substitute(cli=getPackageVersion(), py=__version__)

EXIT_MSG = "\n\nThanks for using.\nWe hope your feedback: https://github.com/zilliztech/milvus_cli/issues/new.\n\n"
