from tabulate import tabulate
import click
import os
import sys

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from .helper_cli import create, getList, delete, rename, show, load, release
from Types import FieldDataTypes, BUILT_IN_ANALYZERS
from pymilvus import FieldSchema, DataType


@create.command("collection")
@click.pass_obj
def create_collection(obj):
    """
    Create collection.
    Example:

        milvus_cli > create collection
    """
    collectionName = click.prompt("Please input collection name", type=str)
    autoId = click.prompt("Please input auto id", default=False, type=bool)
    description = click.prompt("Please input description", default="")
    isDynamic = click.prompt("Is support dynamic field", default=False, type=bool)
    consistencyLevel = click.prompt(
        "Please input consistency level(Strong(0),Bounded(1), Session(2), and Eventually(3))",
        default="1",
        type=int,
    )
    shardsNum = click.prompt("Please input shards number", default=1)
    fields = []
    primaryField = None

    def handle_primary_field(fieldName, primaryField):
        if primaryField is None:
            isPrimary = click.confirm(f"Is {fieldName} the primary key?", default=False)
            return isPrimary
        return False

    while True:
        fieldName = click.prompt(
            "Field name",
            type=str,
        )
        fieldType = click.prompt("Field type", type=click.Choice(FieldDataTypes))
        fieldDesc = click.prompt("Field description", default="", type=str)
        upperFieldType = fieldType.upper()
        if upperFieldType in [
            "BINARY_VECTOR",
            "FLOAT_VECTOR",
            "BFLOAT16_VECTOR",
            "FLOAT16_VECTOR",
            "SPARSE_FLOAT_VECTOR",
        ]:
            dim = click.prompt("Dimension", type=int)
            fields.append(
                FieldSchema(
                    name=fieldName,
                    dtype=DataType[upperFieldType],
                    dim=int(dim),
                )
            )
        elif upperFieldType == "VARCHAR":
            maxLength = click.prompt("Max length", default=65535, type=int)
            isPrimary = handle_primary_field(fieldName, primaryField)
            if isPrimary:
                primaryField = fieldName

            enableAnalyzer = click.prompt("Enable analyzer?", default=False, type=bool)
            enableMatch = click.prompt("Enable match?", default=False, type=bool)
            analyzerBuiltInType = click.prompt(
                "Analyzer built-in type",
                default="",
                type=click.Choice(BUILT_IN_ANALYZERS),
            )

            analyzer_params = (
                None if analyzerBuiltInType == "" else {"type": analyzerBuiltInType}
            )
            fields.append(
                FieldSchema(
                    name=fieldName,
                    dtype=DataType[upperFieldType],
                    max_length=int(maxLength),
                    enable_analyzer=enableAnalyzer,
                    enable_match=enableMatch,
                    analyzer_params=analyzer_params,
                )
            )
        elif upperFieldType == "ARRAY":
            maxCapacity = click.prompt("Max capacity", type=int)
            elementType = click.prompt(
                "Element type", type=click.Choice(FieldDataTypes)
            )
            maxLength = None
            if elementType.upper() == "VARCHAR":
                maxLength = click.prompt("Max length", type=int)
            fields.append(
                FieldSchema(
                    name=fieldName,
                    dtype=DataType[upperFieldType],
                    element_type=DataType[elementType.upper()],
                    max_capacity=int(maxCapacity),
                    max_length=int(maxLength),
                )
            )
        else:
            if upperFieldType == "INT64":
                isPrimary = handle_primary_field(fieldName, primaryField)
                if isPrimary:
                    primaryField = fieldName

            fields.append(
                FieldSchema(
                    name=fieldName,
                    dtype=DataType[upperFieldType],
                    description=fieldDesc,
                )
            )
        if not click.confirm(
            "Ensure you have already created vector and primary fields. Do you want to add more fields?",
            default=True,
        ):
            break

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
        )
    )
    click.echo("Create collection successfully!")


@getList.command("collections")
@click.pass_obj
def list_collections(obj):
    """
    List all collections.
    Example:

        milvus_cli > list collections

    """
    try:
        click.echo(obj.collection.list_collections())
    except Exception as e:
        click.echo(message=e, err=True)


@delete.command("collection")
@click.option(
    "-c",
    "--collection-name",
    "collectionName",
    help="The name of collection to be deleted.",
    required=True,
)
@click.pass_obj
def delete_collection(obj, collectionName):
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
        obj.collection.has_collection(collectionName)
    except Exception as e:
        click.echo(f"Error occurred when get collection by name!\n{str(e)}")
    else:
        result = obj.collection.drop_collection(collectionName)
        click.echo(result)


@rename.command("collection")
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
def rename_collection(obj, collectionName, newName):
    """
    Rename collection.

    Example:

        milvus_cli > rename collection -old car -new new_car
    """
    try:
        obj.collection.has_collection(collectionName)
    except Exception as e:
        click.echo(f"Error occurred when get collection by name!\n{str(e)}")
    else:
        result = obj.collection.rename_collection(collectionName, newName)
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

        milvus_cli > show collection -c test_collection_insert
    """
    try:
        click.echo(obj.collection.get_collection_details(collectionName))
    except Exception as e:
        click.echo(message=e, err=True)


@load.command("collection")
@click.option(
    "-c", "--collection-name", "collectionName", help="The name of collection."
)
@click.pass_obj
def load_collection(
    obj,
    collectionName,
):
    """
    Load collection.

    Example:

        milvus_cli > load collection -c test_collection
    """
    try:
        click.echo(obj.collection.load_collection(collectionName))
    except Exception as e:
        click.echo(message=e, err=True)


@release.command("collection")
@click.option(
    "-c", "--collection-name", "collectionName", help="The name of collection."
)
@click.pass_obj
def release_collection(obj, collectionName):
    """
    Release collection.

    Example:

        milvus_cli > release collection -c test_collection
    """
    try:
        click.echo(obj.collection.release_collection(collectionName))
    except Exception as e:
        click.echo(message=e, err=True)


@show.command("loading_progress")
@click.option(
    "-c", "--collection-name", "collectionName", help="The name of collection."
)
@click.pass_obj
def show_loading_progress(obj, collectionName):
    """
    Show loading progress.

    Example:

        milvus_cli > show loading_progress -c test_collection
    """
    try:
        click.echo(obj.collection.show_loading_progress(collectionName))
    except Exception as e:
        click.echo(message=e, err=True)
