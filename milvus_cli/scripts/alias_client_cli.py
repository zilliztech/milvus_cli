from .helper_client_cli import create, getList, delete, show
import click


@create.command("alias")
@click.option(
    "-c",
    "--collection-name",
    "collectionName",
    help="Collection name to be specified alias.",
    type=str,
)
@click.option(
    "-a",
    "--alias-name",
    "aliasName",
    help="The alias of the collection.",
    type=str,
)
@click.option(
    "-A",
    "--alter",
    "alter",
    help="[Optional, Flag] - Change an existing alias to current collection.",
    default=False,
    is_flag=True,
)
@click.pass_obj
def create_alias(
    obj,
    collectionName,
    aliasName,
    alter,
):
    """
    Specify alias for a collection.
    Alias cannot be duplicated, you can't assign same alias to different collections.
    But you can specify multiple aliases for a collection, for example:

    create alias -c car -a carAlias1

    You can also change alias of a collection to another collection.
    If the alias doesn't exist, it will return error.
    Use "-A" option to change alias owner collection, for example:

    create alias -c car2 -A -a carAlias1
    """
    try:
        if alter:
            result = obj.alias.alter_alias(aliasName, collectionName)
        else:
            result = obj.alias.create_alias(collectionName, aliasName)
    except Exception as ce:
        click.echo("Error!\n{}".format(str(ce)))
    else:
        click.echo(result)


@getList.command("aliases")
@click.option(
    "-c",
    "--collection-name",
    "collectionName",
    default=None,
    help="[Optional] Collection name to list aliases for. If not specified, lists all aliases.",
    type=str,
)
@click.pass_obj
def list_aliases(obj, collectionName):
    """
    List aliases in the database.

    Without collection name, lists all aliases in the current database.
    With collection name, lists aliases for that specific collection.

    Examples:
      list aliases              - List all aliases in the database
      list aliases -c car       - List aliases for collection 'car'
    """
    try:
        result = obj.alias.list_aliases(collectionName)

        # Format and display results
        if isinstance(result, dict):
            # Result is a dictionary with metadata
            aliases = result.get("aliases", [])
            collection_name = result.get("collection_name", "")
        elif isinstance(result, list):
            # Result is a simple list of aliases
            aliases = result
            collection_name = collectionName or ""
        else:
            # Fallback for unexpected types
            aliases = result if result else []
            collection_name = collectionName or ""

        if not aliases:
            if collectionName:
                click.echo(f"No aliases found for collection '{collectionName}'")
            else:
                click.echo("No aliases found in the database")
        else:
            if collectionName:
                click.echo(f"Aliases for collection '{collectionName}':")
                for alias in aliases:
                    click.echo(f"  {alias}")
            else:
                click.echo("All aliases in the database:")
                for alias in aliases:
                    # Handle list_all_aliases format: [{'alias': name, 'collection': name}, ...]
                    if isinstance(alias, dict):
                        alias_name = alias.get('alias', '')
                        coll_name = alias.get('collection', '')
                        click.echo(f"  {alias_name} -> {coll_name}")
                    else:
                        click.echo(f"  {alias}")
    except Exception as e:
        click.echo(message=e, err=True)


@delete.command("alias")
@click.option(
    "-a",
    "--alias-name",
    "aliasName",
    help="The alias of the collection.",
    type=str,
)
@click.pass_obj
def delete_alias(obj, aliasName):
    """
    Delete alias of a collection.
    Example:

      delete alias -a carAlias1
    """
    try:
        click.echo(obj.alias.drop_alias(aliasName))
    except Exception as e:
        click.echo(message=e, err=True)


@show.command("alias")
@click.option(
    "-a",
    "--alias-name",
    "aliasName",
    required=True,
    help="The alias name to describe.",
    type=str,
)
@click.pass_obj
def show_alias(obj, aliasName):
    """
    Show details of an alias.

    Example:

      show alias -a carAlias1
    """
    try:
        alias_info = obj.alias.describe_alias(aliasName)
        if isinstance(alias_info, dict):
            click.echo(f"Alias: {aliasName}")
            click.echo(f"  Collection: {alias_info.get('collection_name', 'N/A')}")
            click.echo(f"  Database: {alias_info.get('db_name', 'default')}")
        else:
            click.echo(alias_info)
    except Exception as e:
        click.echo(message=e, err=True)
