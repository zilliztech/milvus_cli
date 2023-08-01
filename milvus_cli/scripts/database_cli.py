from tabulate import tabulate
from helper_cli import create, getList, delete
from init_cli import cli
import click


@create.command("database")
@click.option(
    "-db",
    "--db_name",
    "db_name",
    help="Database name.",
    required=True,
    type=str,
)
@click.option(
    "-a",
    "--alias",
    "alias",
    help="The connection alias name.",
    type=str,
)
@click.pass_obj
def create_database(obj, db_name=None, alias=None):
    """
    Create database.

    Example:

        milvus_cli > create database -db testdb -a default
    """
    try:
        res = obj.database.create_database(db_name, alias)
    except Exception as e:
        click.echo(message=e, err=True)
    else:
        click.echo(res)


@getList.command("databases")
@click.option(
    "-a",
    "--alias",
    "alias",
    help="The connection alias name.",
    type=str,
)
@click.pass_obj
def list_databases(obj, alias=None):
    """
    List databases.

    Example:

        milvus_cli > list databases -a default
    """
    try:
        res = obj.database.list_databases(alias)
    except Exception as e:
        click.echo(message=e, err=True)
    else:
        table_data = [[item] for item in res]
        click.echo(tabulate(table_data, headers=["db_name"], tablefmt="pretty"))


@delete.command("database")
@click.option(
    "-db",
    "--db_name",
    "db_name",
    help="Database name.",
    required=True,
    type=str,
)
@click.option(
    "-a",
    "--alias",
    "alias",
    help="The connection alias name.",
    type=str,
)
@click.pass_obj
def drop_database(obj, db_name=None, alias=None):
    """
    Drop database.

    Example:

        milvus_cli > drop database -db testdb -a default
    """
    try:
        res = obj.database.drop_database(db_name, alias)
    except Exception as e:
        click.echo(message=e, err=True)
    else:
        click.echo(res)


@cli.group("use", no_args_is_help=False)
@click.pass_obj
def use(obj):
    """Use database"""
    pass


@use.command("database")
@click.option(
    "-db",
    "--db_name",
    "db_name",
    help="Database name.",
    required=True,
    type=str,
)
@click.option(
    "-a",
    "--alias",
    "alias",
    help="The connection alias name.",
    type=str,
)
@click.pass_obj
def use_database(obj, db_name=None, alias=None):
    """
    Use database.

    Example:

        milvus_cli > use database -db testdb -a default
    """
    try:
        res = obj.database.using_database(db_name, alias)
    except Exception as e:
        click.echo(message=e, err=True)
    else:
        click.echo(res)
