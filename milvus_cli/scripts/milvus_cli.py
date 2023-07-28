from tabulate import tabulate
import sys
import os
import click

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

from utils import WELCOME_MSG, EXIT_MSG, Completer
from Cli import MilvusCli
from Types import ParameterException, ConnectException


pass_context = click.make_pass_decorator(MilvusCli, ensure=True)

@click.group(no_args_is_help=False,
             add_help_option=False,
             invoke_without_command=True)
@click.pass_context
def cli(ctx):
    """Milvus_CLI"""
    ctx.obj = MilvusCli()

def print_help_msg(command):
    with click.Context(command) as ctx:
        click.echo(command.get_help(ctx))

@cli.command()
def help():
    """Show help messages."""
    click.echo(print_help_msg(cli))


@cli.command(no_args_is_help=False)
@click.option(
    "-a",
    "--alias",
    "alias",
    help="[Optional] - Milvus link alias name, default is `default`.",
    default="default",
    type=str,
)
@click.option(
    "-uri",
    "--uri",
    "uri",
    help="[Optional] - uri, default is `http://127.0.0.1:19530`.",
    default="http://127.0.0.1:19530",
    type=str,
)
@click.option(
    "-u",
    "--username",
    "username",
    help="[Optional] - Username , default is `None`.",
    default=None,
    type=str,
)
@click.option(
    "-pwd",
    "--password",
    "password",
    help="[Optional] - Password , default is `None`.",
    default=None,
    type=str,
)
@click.pass_obj
def connect(obj, alias,uri, username, password):
    """
    Connect to Milvus.

    Example:

        milvus_cli > connect -h 127.0.0.1 -p 19530 -a default
    """
    try:
        obj.connection.connect(alias, uri,  username, password)
    except Exception as e:
        click.echo(message=e, err=True)
    else:
        click.echo("Connect Milvus successfully.")
        address,username = obj.connection.showConnection(alias)
        click.echo(tabulate(
            [["Address", address], ["User", username], ["Alias", alias]],
            tablefmt="pretty",
        ))
         


@cli.command()
def version():
    """Get Milvus_CLI version."""
    click.echo(f"Milvus_CLI v{getPackageVersion()}")




@cli.command("exit")
def quitapp():
    """Exit the CLI."""
    global quitapp
    quitapp = True


quitapp = False  # global flag
comp = Completer()


def runCliPrompt():
    args = sys.argv
    if args and (args[-1] == "--version"):
        print(f"Milvus_CLI v{getPackageVersion()}")
        return
    try:
        print(WELCOME_MSG)
        while not quitapp:
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
                    message=
                    "Connect to milvus Error!\nPlease check your connection.",
                    err=True,
                )
            except Exception as e:
                click.echo(message=f"Error occurred!\n{str(e)}", err=True)
        print(EXIT_MSG)
    except (KeyboardInterrupt, EOFError):
        print(EXIT_MSG)
        sys.exit(0)


if __name__ == "__main__":
    runCliPrompt()
