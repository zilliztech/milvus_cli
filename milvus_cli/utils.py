import readline
import re
import os
from string import Template
from pymilvus import __version__
from Types import ParameterException


def getPackageVersion():
    try:
        from importlib.metadata import PackageNotFoundError, version
    except Exception as e:
        raise ParameterException(e)

    try:
        return version("milvus_cli")
    except PackageNotFoundError:
        return "dev"


class Completer(object):
    # COMMANDS = ['clear', 'connect', 'create', 'delete', 'describe', 'exit',
    #         'list', 'load', 'query', 'release', 'search', 'show', 'version' ]
    RE_SPACE = re.compile(r".*\s+$", re.M)
    CMDS_DICT = {
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
            "database",
            "collection",
            "entities",
            "partition",
            "index",
            "user",
            "role",
        ],
        "describe": ["collection", "partition", "index"],
        "exit": [],
        "help": [],
        "insert": ["file", "row"],
        "list": [
            "connections",
            "collections",
            "databases",
            "partitions",
            "indexes",
            "users",
            "aliases",
            "roles",
            "grants",
        ],
        "load": ["collection", "partition"],
        "query": [],
        "release": ["collection", "partition"],
        "search": [],
        "show": [
            "index_progress",
            "loading_progress",
            "query_segment",
            "compaction_state",
            "compaction_plans",
            "collection",
            "partition",
            "index",
        ],
        "rename": ["collection"],
        "use": ["database"],
        "version": [],
    }

    def __init__(self) -> None:
        super().__init__()
        self.COMMANDS = list(self.CMDS_DICT.keys())
        self.createCompleteFuncs(self.CMDS_DICT)

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
            if len(args) <= 1 and not cmd == "import":
                return self._complete_2nd_level(sub_cmds, args[-1])
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
