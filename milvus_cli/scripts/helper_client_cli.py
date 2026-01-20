import sys
import os
import click

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
    """Print help message for a command"""
    with click.Context(command) as ctx:
        click.echo(command.get_help(ctx))


from .init_client_cli import cli


@cli.command()
def help():
    """Show help messages."""
    click.echo(print_help_msg(cli))


@cli.command()
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
        from pymilvus import utility
        version = utility.get_server_version()
        click.echo(f"Milvus server version: {version}")
    except Exception as e:
        click.echo(message=e, err=True)


@cli.command()
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
        # Show last 100 commands
        recent = lines[-100:] if len(lines) > 100 else lines
        for i, line in enumerate(recent, 1):
            click.echo(f"{i:4d}  {line.rstrip()}")
    except IOError as e:
        click.echo(f"Error reading history: {e}", err=True)


@cli.group("show", no_args_is_help=False)
@click.pass_obj
def show(obj):
    """Show connection, database, collection, loading_progress or index_progress."""
    pass


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


@cli.group("list", no_args_is_help=False)
@click.pass_obj
def getList(obj):
    """List collections, databases, partitions, users, grants or indexes."""
    pass


@cli.group("rename", no_args_is_help=False)
@click.pass_obj
def rename(obj):
    """Rename collection"""
    pass


@cli.group("create", no_args_is_help=False)
@click.pass_obj
def create(obj):
    """Create collection, database, partition, user, role or index."""
    pass


@cli.group("grant", no_args_is_help=False)
@click.pass_obj
def grant(obj):
    """Grant role, privilege."""
    pass


@cli.group("revoke", no_args_is_help=False)
@click.pass_obj
def revoke(obj):
    """Revoke role, privilege."""
    pass


@cli.group("load", no_args_is_help=False)
@click.pass_obj
def load(obj):
    """Load collection, partition"""
    pass


@cli.group("release", no_args_is_help=False)
@click.pass_obj
def release(obj):
    """Release collection, partition"""
    pass


@cli.group("delete", no_args_is_help=False)
@click.pass_obj
def delete(obj):
    """Delete collection, database, partition, alias, user, role or index."""
    pass


@cli.group("use", no_args_is_help=False)
@click.pass_obj
def use(obj):
    """Use database"""
    pass


@cli.group("search", no_args_is_help=False)
@click.pass_obj
def search(obj):
    """Similarity search"""
    pass


@cli.group("query", no_args_is_help=False)
@click.pass_obj
def query(obj):
    """Query entities in collection."""
    pass


@cli.group("insert", no_args_is_help=False)
@click.pass_obj
def insert(obj):
    """Insert entities"""
    pass


@cli.command("exit")
def quit_app():
    """Exit the CLI."""
    global quit_app
    quit_app = True


quit_app = False  # global flag
comp = None  # Initialize later with CLI instance


def runCliPrompt():
    """Run CLI prompt loop"""
    global comp, quit_app
    args = sys.argv
    if args and (args[-1] == "--version"):
        print(f"Milvus_CLI v{getPackageVersion()}")
        return
    try:
        print(WELCOME_MSG)

        # Get the shared MilvusClientCli instance
        from .init_client_cli import get_milvus_cli_obj
        milvus_obj = get_milvus_cli_obj()

        # Initialize completer with CLI instance and MilvusCli object
        comp = Completer(cli_instance=cli, milvus_cli_obj=milvus_obj)

        # Setup prompt_toolkit session with history and completion
        history_path = Path.home() / ".milvus_cli_history"
        session = PromptSession(
            history=FileHistory(str(history_path)),
            lexer=MilvusLexer(),
            completer=MilvusCompleter(comp),
            style=milvus_style,
            complete_while_typing=False,
        )

        while not quit_app:
            try:
                astr = session.prompt("milvus_cli > ")
            except KeyboardInterrupt:
                continue
            except EOFError:
                break

            if not astr.strip():
                continue

            try:
                cli(astr.split())
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
