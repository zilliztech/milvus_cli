from tabulate import tabulate
from .helper_cli import create, getList, delete, use, show, alter
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
@click.option(
    "--yes", "-y",
    is_flag=True,
    help="Skip confirmation prompt",
)
@click.pass_obj
def drop_database(obj, db_name=None, yes=False):
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
        milvus_cli > delete database -db old_project --yes

    SEE ALSO:
        list databases, create database
    """
    if not yes:
        click.echo(
            f"Warning!\nYou are trying to delete database '{db_name}' and all its collections. This action cannot be undone!\n"
        )
        if not click.confirm("Do you want to continue?"):
            return
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


@show.command("database")
@click.option(
    "-db",
    "--db_name",
    "db_name",
    help="Database name. If not provided, shows current database.",
    required=False,
    default=None,
    type=str,
)
@click.pass_obj
def describe_database(obj, db_name):
    """
    Show database details and properties.

    USAGE:
        milvus_cli > show database           Show current database
        milvus_cli > show database -db <name>  Show specific database

    OPTIONS:
        -db, --db_name    Name of database to describe (optional)

    OUTPUT:
        Displays database information including:
        - name: Database name
        - properties: Database properties (replica number, etc.)

    EXAMPLES:
        milvus_cli > show database
        milvus_cli > show database -db default

    SEE ALSO:
        list databases, alter database
    """
    try:
        if db_name is None:
            db_name = obj.database.get_current_database()
        result = obj.database.describe_database(db_name)
        click.echo(f"Database: {db_name}")
        click.echo(f"Properties: {result}")
    except Exception as e:
        click.echo(message=e, err=True)


@alter.command("database")
@click.option(
    "-db",
    "--db_name",
    "db_name",
    help="Database name.",
    required=True,
    type=str,
)
@click.pass_obj
def alter_database(obj, db_name):
    """
    Alter database properties.

    USAGE:
        milvus_cli > alter database -db <name>

    OPTIONS:
        -db, --db_name    Name of database to alter (required)

    INTERACTIVE PROMPTS:
        Property key      The property to set
        Property value    The value to set

    AVAILABLE PROPERTIES:
        database.replica.number    Number of replicas for the database

    EXAMPLES:
        milvus_cli > alter database -db mydb
        Property key: database.replica.number
        Property value: 2

    SEE ALSO:
        show database, create database
    """
    try:
        properties = {}
        while True:
            key = click.prompt("Property key (e.g., database.replica.number)")
            value = click.prompt(f"Property value for '{key}'")
            # Try to convert to appropriate type
            if value.lower() in ["true", "false"]:
                value = value.lower() == "true"
            elif value.isdigit():
                value = int(value)
            properties[key] = value
            if not click.confirm("Add another property?", default=False):
                break
        result = obj.database.alter_database(db_name, properties)
        click.echo(result)
    except Exception as e:
        click.echo(message=e, err=True)
