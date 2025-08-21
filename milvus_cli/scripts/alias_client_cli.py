from .helper_client_cli import create, getList, delete
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
            result = obj.alias.alter_alias(collectionName, aliasName)
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
    help="Collection name to be specified alias.",
    type=str,
)
@click.pass_obj
def list_aliases(obj, collectionName):
    """
    List all aliases of a collection.
    Example:

      list aliases -c car
    """
    try:
        click.echo(obj.alias.list_aliases(collectionName))
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
