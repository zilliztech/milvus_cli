from tabulate import tabulate
from .helper_client_cli import show, getList, cli
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
@click.option(
    "-tls",
    "--tlsmode",
    "tlsmode",
    help="[Optional] - Set TLS mode: 0 (No encryption), 1 (One-way encryption), 2 (Two-way encryption).",
    default=0,
    type=int,
)
@click.option(
    "-cert",
    "--cert",
    "cert",
    help="[Optional] - Path to the client certificate file.",
    default=None,
    type=str,
)
@click.pass_obj
def connect(obj, uri, token, tlsmode, cert):
    """
    Connect to Milvus.

    Example:

        milvus_cli > connect -uri localhost:19530
    """
    try:
        obj.connection.connect(uri, token, tlsmode, cert)
    except Exception as e:
        click.echo(message=e, err=True)
    else:
        click.echo("Connect Milvus successfully.")
        connectionInfo = obj.connection.showConnection()
        click.echo(
            tabulate(
                [["Address", list(connectionInfo)[2]], ["Alias", "default"]],
                tablefmt="pretty",
            )
        )


@getList.command("connections")
@click.pass_obj
def connection(obj):
    """List connections"""
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
