from tabulate import tabulate
from .helper_client_cli import create, getList, delete, use
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
@click.pass_obj
def create_database(obj, db_name=None, alias=None):
    """
    Create a new database.

    USAGE:
        milvus_cli > create database -db <name>

    OPTIONS:
        -db, --db_name    Name for the new database (required)

    EXAMPLES:
        milvus_cli > create database -db myproject

    NOTES:
        - Database names must be unique
        - Use 'use database' to switch to the new database

    SEE ALSO:
        list databases, use database, delete database
    """
    try:
        res = obj.database.create_database(db_name)
    except Exception as e:
        click.echo(message=e, err=True)
    else:
        click.echo(res)


@getList.command("databases")
@click.pass_obj
def list_databases(obj):
    """
    List all databases in the Milvus instance.

    USAGE:
        milvus_cli > list databases

    OUTPUT:
        Lists database names in current output format (table/json/csv).

    EXAMPLES:
        milvus_cli > list databases

        # With JSON output
        milvus_cli > set output json
        milvus_cli > list databases

    SEE ALSO:
        create database, use database, delete database
    """
    try:
        res = obj.database.list_databases()
        click.echo(obj.formatter.format_list(res, header="Database"))
    except Exception as e:
        click.echo(message=e, err=True)


@delete.command("database")
@click.option(
    "-db",
    "--db_name",
    "db_name",
    help="Database name.",
    required=True,
    type=str,
)
@click.pass_obj
def drop_database(obj, db_name=None):
    """
    Delete a database and all its collections.

    USAGE:
        milvus_cli > delete database -db <name>

    OPTIONS:
        -db, --db_name    Name of database to delete (required)

    WARNING:
        This action is irreversible! All collections in the database
        will be permanently deleted.

    EXAMPLES:
        milvus_cli > delete database -db old_project

    SEE ALSO:
        list databases, create database
    """
    try:
        res = obj.database.drop_database(
            db_name,
        )
    except Exception as e:
        click.echo(message=e, err=True)
    else:
        click.echo(res)


@use.command("database")
@click.option(
    "-db",
    "--db_name",
    "db_name",
    help="Database name.",
    required=True,
    type=str,
)
@click.pass_obj
def use_database(obj, db_name=None):
    """
    Use database.

    Example:

        milvus_cli > use database -db testdb
    """
    try:
        res = obj.database.using_database(
            db_name,
        )
    except Exception as e:
        click.echo(message=e, err=True)
    else:
        click.echo(res)
