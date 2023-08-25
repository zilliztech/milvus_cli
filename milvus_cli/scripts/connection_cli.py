from tabulate import tabulate
from .helper_cli import show, getList
from .init_cli import cli
import click


@cli.command(no_args_is_help=False)
@click.option(
    "-uri",
    "--uri",
    "uri",
    help="[Optional] - uri, default is `http://127.0.0.1:19530`.",
    default="http://127.0.0.1:19530",
    type=str,
)
@click.option(
    "-t",
    "--token",
    "token",
    help="[Optional] - token: username:password or zilliz cloud api key`.",
    default=None,
    type=str,
)
@click.pass_obj
def connect(obj, uri, token):
    """
    Connect to Milvus.

    Example:

        milvus_cli > connect -uri localhost:19530
    """
    try:
        obj.connection.connect(uri, token)
    except Exception as e:
        click.echo(message=e, err=True)
    else:
        click.echo("Connect Milvus successfully.")
        address, username = obj.connection.showConnection()
        click.echo(
            tabulate(
                [["Address", address], ["User", username], ["Alias", "default"]],
                tablefmt="pretty",
            )
        )


@getList.command("connections")
@click.pass_obj
def connection(obj):
    """Show current/all connection details"""
    try:
        allConnections = obj.connection.showConnection(showAll=True)
        click.echo(
            tabulate(
                allConnections,
                headers=["Alias", "Instance"],
                tablefmt="pretty",
            )
        )
    except Exception as e:
        click.echo(message=e, err=True)
