from tabulate import tabulate
from helper_cli import create, getList, delete, rename, show, load, release
import click
import os
import sys

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from Validation import validateCollectionParameter
from Types import ParameterException


@create.command("collection")
@click.option(
    "-c",
    "--collection-name",
    "collectionName",
    help="Collection name to specify alias.",
    type=str,
)
@click.option(
    "-p", "--schema-primary-field", "primaryField", help="Primary field name."
)
@click.option(
    "-A",
    "--schema-auto-id",
    "autoId",
    help="[Optional, Flag] - Enable auto id.",
    default=False,
    is_flag=True,
)
@click.option("-a", "--alias", "alias", help="The connection alias name.", type=str)
@click.option(
    "-desc",
    "--schema-description",
    "description",
    help="[Optional] - Description details.",
    default="",
)
@click.option(
    "-d",
    "--is-dynamic",
    "isDynamic",
    help="[Optional] - Collection schema supports dynamic fields or not.",
    default=None,
)
@click.option(
    "-level",
    "--consistency-level",
    "consistencyLevel",
    help="[Optional] - Consistency level: Bounded,Session,Strong, Eventual .",
    default="Bounded",
)
@click.option(
    "-f",
    "--schema-field",
    "fields",
    help='[Multiple] - FieldSchema. Usage is "<Name>:<DataType>:<Dim(if vector) or Description>"',
    default=None,
    multiple=True,
)
@click.option(
    "-s",
    "--shards-num",
    "shardsNum",
    help="[Optional] - Shards number",
    default=1,
    type=int,
)
@click.pass_obj
def create_collection(
    obj,
    collectionName,
    primaryField,
    autoId,
    description,
    fields,
    isDynamic,
    consistencyLevel,
    shardsNum,
    alias,
):
    """
    Create collection.

    Example:

      create collection -c car -f id:INT64:primary_field -f vector:FLOAT_VECTOR:128 -f color:INT64:color -f brand:INT64:brand -p id -A -d 'car_collection'
    """
    try:
        validateCollectionParameter(
            collectionName,
            primaryField,
            fields,
        )
    except ParameterException as pe:
        click.echo("Error!\n{}".format(str(pe)))
    else:
        click.echo(
            obj.collection.create_collection(
                collectionName,
                primaryField,
                fields,
                autoId,
                description,
                isDynamic,
                consistencyLevel,
                shardsNum,
                alias,
            )
        )
        click.echo("Create collection successfully!")


@getList.command("collections")
@click.option("-a", "--alias", "alias", help="The connection alias name.", type=str)
@click.pass_obj
def list_collections(obj, alias):
    """
    List all collections.
    Example:

        milvus_cli > list collections

    """
    try:
        click.echo(obj.collection.list_collections(alias))
    except Exception as e:
        click.echo(message=e, err=True)


@delete.command("collection")
@click.option(
    "-a", "--alias", "alias", help="[Optional]: The connection alias name.", type=str
)
@click.option(
    "-c",
    "--collection-name",
    "collectionName",
    help="The name of collection to be deleted.",
    required=True,
)
@click.pass_obj
def delete_collection(obj, collectionName, alias):
    """
    Drops the collection together with its index files.

    Example:

        milvus_cli > delete collection -c car
    """
    click.echo(
        "Warning!\nYou are trying to delete the collection with data. This action cannot be undone!\n"
    )
    if not click.confirm("Do you want to continue?"):
        return
    try:
        obj.collection.has_collection(alias, collectionName)
    except Exception as e:
        click.echo(f"Error occurred when get collection by name!\n{str(e)}")
    else:
        result = obj.collection.drop_collection(alias, collectionName)
        click.echo(result)


@rename.command("collection")
@click.option("-a", "--alias", "alias", help="The connection alias name.", type=str)
@click.option(
    "-old",
    "--old-collection-name",
    "collectionName",
    help="The old collection name",
    type=str,
    required=True,
)
@click.option(
    "-new",
    "--new-collection-name",
    "newName",
    help="The new collection name",
    type=str,
    required=True,
)
@click.pass_obj
def rename_collection(obj, alias, collectionName, newName):
    """
    Rename collection.

    Example:

        milvus_cli > rename collection -old car -new new_car
    """
    try:
        obj.collection.has_collection(alias, collectionName)
    except Exception as e:
        click.echo(f"Error occurred when get collection by name!\n{str(e)}")
    else:
        result = obj.collection.rename_collection(alias, collectionName, newName)
        click.echo(result)


@show.command("collection")
@click.option(
    "-c", "--collection-name", "collectionName", help="The name of collection."
)
@click.pass_obj
def describe_collection(obj, collectionName):
    """
    Describe collection.

    Example:

        milvus_cli > describe collection -c test_collection_insert
    """
    try:
        click.echo(obj.collection.get_collection_details(collectionName))
    except Exception as e:
        click.echo(message=e, err=True)


@load.command("collection")
@click.option(
    "-c", "--collection-name", "collectionName", help="The name of collection."
)
@click.option("-a", "--alias", "alias", help="The connection alias name.", type=str)
@click.pass_obj
def load_collection(obj, collectionName, alias):
    """
    Load collection.

    Example:

        milvus_cli > load collection -c test_collection
    """
    try:
        click.echo(obj.collection.load_collection(alias, collectionName))
    except Exception as e:
        click.echo(message=e, err=True)


@release.command("collection")
@click.option(
    "-c", "--collection-name", "collectionName", help="The name of collection."
)
@click.option("-a", "--alias", "alias", help="The connection alias name.", type=str)
@click.pass_obj
def release_collection(obj, alias, collectionName):
    """
    Release collection.

    Example:

        milvus_cli > release collection -c test_collection
    """
    try:
        click.echo(obj.collection.release_collection(alias, collectionName))
    except Exception as e:
        click.echo(message=e, err=True)


@show.command("loading_progress")
@click.option(
    "-c", "--collection-name", "collectionName", help="The name of collection."
)
@click.option("-a", "--alias", "alias", help="The connection alias name.", type=str)
@click.pass_obj
def show_loading_progress(obj, alias, collectionName):
    """
    Show loading progress.

    Example:

        milvus_cli > show loading_progress -c test_collection
    """
    try:
        click.echo(obj.collection.show_loading_progress(collectionName, alias))
    except Exception as e:
        click.echo(message=e, err=True)
