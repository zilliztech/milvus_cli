from tabulate import tabulate
from .helper_client_cli import show, getList, cli
import click
import os
from pymilvus import connections


@cli.command(no_args_is_help=False)
@click.option(
    "-uri",
    "--uri",
    "uri",
    help="[Optional] - uri, default is `http://127.0.0.1:19530` or env `ZILLIZ_URI`.",
    default=None,
    type=str,
)
@click.option(
    "-t",
    "--token",
    "token",
    help="[Optional] - token: username:password or zilliz cloud api key, or env `ZILLIZ_TOKEN`.",
    default=None,
    type=str,
)
@click.option(
    "-tls",
    "--tlsmode",
    "tlsmode",
    help="[Optional] - Set TLS mode: 0 (No encryption), 1 (One-way encryption), 2 (Two-way encryption).",
    default=None,
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
    env_uri = os.getenv("ZILLIZ_URI")
    env_token = os.getenv("ZILLIZ_TOKEN")
    if uri is None:
        uri = env_uri or "http://127.0.0.1:19530"
    if token is None:
        token = env_token
    if tlsmode is None:
        tlsmode = 1 if uri.startswith("https://") else 0
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


@cli.command(no_args_is_help=False)
@click.pass_obj
def disconnect(obj):
    """
    Disconnect from Milvus.

    Example:

        milvus_cli > disconnect
    """
    try:
        click.echo(obj.connection.disconnect())
    except Exception as e:
        click.echo(message=e, err=True)


@getList.command("connections")
@click.pass_obj
def connection(obj):
    """List connections"""
    try:
        allConnections = obj.connection.showConnection(showAll=True)
        table_data = []
        for alias, _handler in allConnections:
            addr = connections.get_connection_addr(alias)
            uri = addr.get("uri") if isinstance(addr, dict) else str(addr)
            table_data.append([alias, uri])
        click.echo(
            tabulate(
                table_data,
                headers=["Alias", "Instance"],
                tablefmt="pretty",
            )
        )
    except Exception as e:
        click.echo(message=e, err=True)
