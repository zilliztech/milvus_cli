# Import cli and command groups from helper_client_cli to avoid duplication
from .helper_client_cli import (
    cli,
    show,
    getList,
    rename,
    create,
    grant,
    revoke,
    load,
    release,
    delete,
    use,
    search,
    query,
    insert,
)
import sys
import os
import click
import shlex

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from utils import WELCOME_MSG, EXIT_MSG, Completer, getPackageVersion
from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory
from prompt_style import MilvusLexer, MilvusCompleter, milvus_style
from pathlib import Path
from Types import ConnectException, ParameterException


def print_help_msg(command):
    with click.Context(command) as ctx:
        click.echo(command.get_help(ctx))


# Override help command to use our version
@cli.command("help")
def help_cmd():
    """Show help messages."""
    click.echo(print_help_msg(cli))


# Override version command
@cli.command("version")
def version():
    """Get Milvus_CLI version."""
    click.echo(f"Milvus_CLI v{getPackageVersion()}")


@cli.command("server_version")
@click.pass_obj
def server_version(obj):
    """
    Get Milvus server version.

    Example:

        milvus_cli > server_version
    """
    try:
        client = obj.connection.get_client()
        if client is None:
            click.echo("No connection. Use 'connect' first.", err=True)
            return
        version = client.get_server_version()
        click.echo(f"Milvus server version: {version}")
    except Exception as e:
        click.echo(message=e, err=True)


@cli.command("clear")
def clear():
    """Clear screen."""
    click.clear()


@cli.command("history")
@click.argument("action", required=False, default=None)
def history_cmd(action):
    """
    Show or manage command history.

    USAGE:
        milvus_cli > history          Show last 100 commands
        milvus_cli > history clear    Clear command history

    EXAMPLES:
        milvus_cli > history
        milvus_cli > history clear
    """
    history_path = Path.home() / ".milvus_cli_history"

    if action == "clear":
        try:
            if history_path.exists():
                history_path.unlink()
            click.echo("Command history cleared.")
        except IOError as e:
            click.echo(f"Error clearing history: {e}", err=True)
        return

    if action is not None:
        click.echo(f"Unknown action: {action}. Use 'history' or 'history clear'.", err=True)
        return

    # Show history
    if not history_path.exists():
        click.echo("No command history found.")
        return

    try:
        with open(history_path, "r") as f:
            lines = f.readlines()

        # Filter out libedit header and empty lines
        filtered_lines = []
        for line in lines:
            stripped = line.rstrip()
            # Skip libedit history header and empty lines
            if stripped and not stripped.startswith("_HiStOrY_V2_"):
                # Decode libedit escape sequences (e.g., \040 = space)
                import codecs
                try:
                    stripped = codecs.decode(stripped, 'unicode_escape')
                except Exception:
                    pass
                filtered_lines.append(stripped)

        if not filtered_lines:
            click.echo("No command history found.")
            return

        # Show last 100 commands
        recent = filtered_lines[-100:] if len(filtered_lines) > 100 else filtered_lines
        for i, line in enumerate(recent, 1):
            click.echo(f"{i:4d}  {line}")
    except IOError as e:
        click.echo(f"Error reading history: {e}", err=True)


@show.command("output")
@click.pass_obj
def show_output_format(obj):
    """
    Show current output format setting.

    USAGE:
        milvus_cli > show output

    EXAMPLES:
        milvus_cli > show output
        Current output format: table
    """
    click.echo(f"Current output format: {obj.formatter.format}")


@cli.group("set", no_args_is_help=False)
@click.pass_obj
def set_config(obj):
    """Set CLI configuration options."""
    pass


@set_config.command("output")
@click.argument("format", type=click.Choice(["table", "json", "csv"]))
@click.pass_obj
def set_output_format(obj, format):
    """
    Set the global output format for CLI results.

    USAGE:
        milvus_cli > set output <format>

    ARGUMENTS:
        format    Output format: table, json, or csv

    FORMATS:
        table    Display results in formatted ASCII tables (default)
        json     Display results as JSON for scripting/parsing
        csv      Display results as CSV for spreadsheet import

    EXAMPLES:
        milvus_cli > set output json
        milvus_cli > set output table
        milvus_cli > set output csv

    NOTES:
        - Setting persists for the current session only
        - Default format is 'table'
    """
    obj.formatter.format = format
    click.echo(f"Output format set to: {format}")


@cli.group("alter", no_args_is_help=False)
@click.pass_obj
def alter(obj):
    """Alter collection, database, or field properties."""
    pass


@cli.group("update", no_args_is_help=False)
@click.pass_obj
def update(obj):
    """Update password or resource group."""
    pass


@cli.group("transfer", no_args_is_help=False)
@click.pass_obj
def transfer(obj):
    """Transfer replica between resource groups."""
    pass


@cli.command("exit")
def quit_app():
    """Exit the CLI."""
    global _quit_app
    _quit_app = True


_quit_app = False  # global flag
comp = None  # Initialize later with CLI instance


def runCliPrompt():
    """Run CLI prompt loop with syntax highlighting and tab completion."""
    global comp, _quit_app

    args = sys.argv
    if args and (args[-1] == "--version"):
        print(f"Milvus_CLI v{getPackageVersion()}")
        return

    try:
        print(WELCOME_MSG)

        # Get the shared MilvusCli instance
        from .init_client_cli import get_milvus_cli_obj
        milvus_obj = get_milvus_cli_obj()

        # Initialize completer with CLI instance and MilvusCli object
        comp = Completer(cli_instance=cli, milvus_cli_obj=milvus_obj)

        # Setup prompt_toolkit session with history, syntax highlighting and completion
        history_path = Path.home() / ".milvus_cli_history"
        session = PromptSession(
            history=FileHistory(str(history_path)),
            lexer=MilvusLexer(),
            completer=MilvusCompleter(comp),
            style=milvus_style,
            complete_while_typing=False,
        )

        while not _quit_app:
            try:
                astr = session.prompt("milvus_cli > ")
            except (KeyboardInterrupt, EOFError):
                print()  # Print newline after ^C or ^D
                break

            if not astr.strip():
                continue

            try:
                cli(shlex.split(astr))
            except SystemExit:
                # trap argparse error message
                continue
            except ParameterException as pe:
                click.echo(message=f"{str(pe)}", err=True)
            except ConnectException as ce:
                click.echo(
                    message="Connect to milvus Error!\nPlease check your connection.",
                    err=True,
                )
            except Exception as e:
                click.echo(message=f"Error occurred!\n{str(e)}", err=True)

        print(EXIT_MSG)
    except (KeyboardInterrupt, EOFError):
        print(EXIT_MSG)
        sys.exit(0)
