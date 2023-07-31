from tabulate import tabulate
from helper_cli import show
from init_cli import cli
import click


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
def connect(obj, alias, uri, username, password):
    """
    Connect to Milvus.

    Example:

        milvus_cli > connect -h 127.0.0.1 -p 19530 -a default
    """
    try:
        obj.connection.connect(alias, uri, username, password)
    except Exception as e:
        click.echo(message=e, err=True)
    else:
        click.echo("Connect Milvus successfully.")
        address, username = obj.connection.showConnection(alias)
        click.echo(
            tabulate(
                [["Address", address], ["User", username], ["Alias", alias]],
                tablefmt="pretty",
            )
        )


@show.command("connection")
@click.option(
    "-a",
    "--all",
    "showAll",
    help="[Optional, Flag] - Show all connections.",
    default=False,
    is_flag=True,
    type=bool,
)
@click.option(
    "-n",
    "--name",
    "name",
    help="[Optional, Flag] - Show one connection by name.",
    default=None,
    type=str,
)
@click.pass_obj
def connection(obj, showAll=True, name=None):
    """Show current/all connection details"""
    try:
        click.echo("Current connection:")
        if showAll:
            allConnections = obj.connection.showConnection(showAll=True)
            click.echo(
                tabulate(
                    allConnections,
                    headers=["Alias", "Instance"],
                    tablefmt="pretty",
                )
            )
        else:
            result = obj.connection.showConnection(showAll=False, alias=name)
            if isinstance(result, str):
                click.echo(result)
                return
            address, user = result
            click.echo(
                tabulate(
                    [["Address", address], ["User", user], ["Alias", name]],
                    tablefmt="pretty",
                )
            )
    except Exception as e:
        click.echo("Current connection2:")
        click.echo(message=e, err=True)
