from tabulate import tabulate
from .helper_client_cli import show, getList, delete, cli
import click
import os
from pymilvus import connections
from ..history import ConnectionHistory


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
    help="Path to client certificate file (for TLS mode 2)",
    default=None,
    type=str,
)
@click.option(
    "--save-as",
    "save_as",
    help="Save connection with custom alias",
    default=None,
    type=str,
)
@click.pass_obj
def connect(obj, uri, token, tlsmode, cert, save_as):
    """
    Connect to a Milvus server.

    USAGE:
        milvus_cli > connect [-uri URI] [-t TOKEN] [-tls MODE] [-cert PATH]

    OPTIONS:
        -uri, --uri       Server URI (default: http://127.0.0.1:19530)
        -t, --token       Auth token (username:password or API key)
        -tls, --tlsmode   TLS mode: 0=none, 1=one-way, 2=two-way
        -cert, --cert     Client certificate path (for two-way TLS)

    EXAMPLES:
        # Connect to local Milvus
        milvus_cli > connect

        # Connect to remote server with auth
        milvus_cli > connect -uri http://192.168.1.100:19530 -t root:milvus

        # Connect to Zilliz Cloud
        milvus_cli > connect -uri https://xxx.zillizcloud.com -t <api_key>

        # Connect with TLS
        milvus_cli > connect -uri https://secure.milvus.io:19530 -tls 1

    ERRORS:
        - Connection refused: Check if server is running and URI is correct
        - Auth failed: Verify token/credentials
        - TLS error: Check certificate paths and TLS mode

    SEE ALSO:
        list connections, show output
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
        # Auto-save connection to history
        conn_history = ConnectionHistory()
        conn_history.save_connection(
            uri=uri,
            token=token,
            tlsmode=tlsmode,
            cert=cert,
            alias=save_as,
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
    """
    List all Milvus connections.

    USAGE:
        milvus_cli > list connections

    OUTPUT:
        Shows all connection aliases and their server addresses.

    EXAMPLES:
        milvus_cli > list connections

    SEE ALSO:
        connect
    """
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


@getList.command("connection_history")
@click.pass_obj
def list_connection_history(obj):
    """
    List saved connection history.

    USAGE:
        milvus_cli > list connection_history

    OUTPUT:
        Shows all saved connections with their aliases and URIs.

    EXAMPLES:
        milvus_cli > list connection_history

    SEE ALSO:
        connect --save-as, delete connection_history
    """
    conn_history = ConnectionHistory()
    connections = conn_history.list_connections()

    if not connections:
        click.echo("No saved connections found.")
        return

    data = [
        {
            "Alias": c["alias"],
            "URI": c["uri"],
            "Last Used": c["last_used"][:19] if c["last_used"] != "unknown" else "unknown",
        }
        for c in connections
    ]
    click.echo(obj.formatter.format_output(data))


@delete.command("connection_history")
@click.option(
    "-uri",
    "--uri",
    "uri",
    help="URI of the connection to delete",
    required=True,
    type=str,
)
@click.pass_obj
def delete_connection_history(obj, uri):
    """
    Delete a saved connection from history.

    USAGE:
        milvus_cli > delete connection_history -uri <uri>

    OPTIONS:
        -uri, --uri    URI of the connection to delete (required)

    EXAMPLES:
        milvus_cli > delete connection_history -uri http://127.0.0.1:19530

    SEE ALSO:
        list connection_history, connect --save-as
    """
    conn_history = ConnectionHistory()
    if conn_history.delete_connection(uri):
        click.echo(f"Connection '{uri}' deleted from history.")
    else:
        click.echo(f"Connection with URI '{uri}' not found in history.", err=True)
