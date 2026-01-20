from prompt_toolkit.lexers import Lexer
from prompt_toolkit.completion import Completer as PTCompleter, Completion
from prompt_toolkit.styles import Style


# Milvus CLI commands for syntax highlighting
COMMANDS = {
    "connect", "exit", "help", "clear", "version", "server_version",
    "show", "set", "list", "create", "delete", "rename", "load", "release",
    "use", "search", "query", "insert", "upsert", "grant", "revoke",
    "flush", "compact", "truncate", "bulk_insert", "history", "get",
    "describe", "import", "wait_for_loading",
}

SUBCOMMANDS = {
    "collection", "database", "partition", "index", "user", "role", "alias",
    "connections", "collections", "databases", "partitions", "indexes",
    "users", "roles", "grants", "aliases", "output", "file", "row",
    "connection_history", "bulk_insert_tasks", "bulk_insert_state",
    "loading_progress", "index_progress", "query_segment_info",
    "compaction_state", "compaction_plans", "replicas", "load_state",
}

OPTIONS = {
    "-uri", "--uri", "-t", "--token", "-tls", "--tlsmode", "-cert", "--cert",
    "-c", "--collection", "-p", "--partition", "-db", "--db_name",
    "-f", "--fields", "-q", "--query", "-o", "--output", "--save-as",
}


class MilvusLexer(Lexer):
    """Syntax highlighter for Milvus CLI commands."""

    def lex_document(self, document):
        def get_tokens(line_number):
            line = document.lines[line_number]
            tokens = []
            pos = 0

            for word in line.split():
                # Find word position
                start = line.find(word, pos)

                # Add whitespace before word
                if start > pos:
                    tokens.append(("", line[pos:start]))

                # Determine token style
                if word in COMMANDS:
                    style = "class:command"
                elif word in SUBCOMMANDS:
                    style = "class:subcommand"
                elif word in OPTIONS or word.startswith("-"):
                    style = "class:option"
                elif word.startswith(("http://", "https://")):
                    style = "class:uri"
                else:
                    style = ""

                tokens.append((style, word))
                pos = start + len(word)

            # Add trailing whitespace
            if pos < len(line):
                tokens.append(("", line[pos:]))

            return tokens

        return get_tokens


class MilvusCompleter(PTCompleter):
    """
    prompt_toolkit completer that wraps the existing Completer.
    """

    def __init__(self, old_completer):
        self.old_completer = old_completer

    def get_completions(self, document, complete_event):
        text = document.text_before_cursor
        words = text.split()

        # Get the word being typed
        word_before_cursor = document.get_word_before_cursor()

        if not words:
            # Show all commands
            for cmd in sorted(self.old_completer.COMMANDS):
                yield Completion(cmd, start_position=0, display_meta="command")
            return

        # If text ends with space, we're completing a new word
        if text.endswith(" "):
            words.append("")

        cmd = words[0]

        if len(words) == 1:
            # Complete command name
            for c in sorted(self.old_completer.COMMANDS):
                if c.startswith(cmd):
                    yield Completion(
                        c,
                        start_position=-len(cmd),
                        display_meta="command"
                    )
        else:
            # Complete subcommand or argument
            if cmd in self.old_completer.COMMANDS:
                impl = getattr(self.old_completer, f"complete_{cmd}", None)
                if impl:
                    args = words[1:]
                    completions = impl(args)
                    if completions:
                        current = args[-1] if args else ""
                        for comp in completions:
                            comp = comp.rstrip()
                            if comp and (not current or comp.startswith(current)):
                                yield Completion(
                                    comp,
                                    start_position=-len(current),
                                    display_meta="subcommand"
                                )


# Style for syntax highlighting
milvus_style = Style.from_dict({
    "command": "#00aa00 bold",      # Green bold for commands
    "subcommand": "#0088ff",         # Blue for subcommands
    "option": "#ffaa00",             # Orange for options
    "uri": "#00aaaa",                # Cyan for URIs
    "prompt": "#00aa00 bold",        # Green prompt
})
