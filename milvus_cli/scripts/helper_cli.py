from Cli import getPackageVersion
from init_cli import cli
from Types import ConnectException, ParameterException
from utils import WELCOME_MSG, EXIT_MSG, Completer
import sys
import os
import click

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)


def print_help_msg(command):
    with click.Context(command) as ctx:
        click.echo(command.get_help(ctx))


@cli.command()
def help():
    """Show help messages."""
    click.echo(print_help_msg(cli))


@cli.command()
def version():
    """Get Milvus_CLI version."""
    click.echo(f"Milvus_CLI v{getPackageVersion()}")


@cli.command()
def clear():
    """Clear screen."""
    click.clear()


@cli.group(no_args_is_help=False)
@click.pass_obj
def show(obj):
    """Show connection, database,collection, loading_progress and index_progress."""
    pass


@cli.group("list", no_args_is_help=False)
@click.pass_obj
def getList(obj):
    """List collections,databases, partitions and indexes."""
    pass


@cli.group("rename", no_args_is_help=False)
@click.pass_obj
def rename(obj):
    """Rename collection"""
    pass


@cli.group("create", no_args_is_help=False)
@click.pass_obj
def create(obj):
    """Create collection, database, partition and index."""
    pass


@cli.group("delete", no_args_is_help=False)
@click.pass_obj
def delete(obj):
    """Delete collection, database, partition,alias and index."""
    pass


@cli.command("exit")
def quit_app():
    """Exit the CLI."""
    global quit_app
    quit_app = True


quit_app = False  # global flag
comp = Completer()


def runCliPrompt():
    args = sys.argv
    if args and (args[-1] == "--version"):
        print(f"Milvus_CLI v{getPackageVersion()}")
        return
    try:
        print(WELCOME_MSG)
        while not quit_app:
            import readline

            readline.set_completer_delims(" \t\n;")
            readline.parse_and_bind("tab: complete")
            readline.set_completer(comp.complete)
            astr = input("milvus_cli > ")
            try:
                cli(astr.split())
            except SystemExit:
                # trap argparse error message
                # print('error', SystemExit)
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