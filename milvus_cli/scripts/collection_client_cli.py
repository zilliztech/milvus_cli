from tabulate import tabulate
import click
import os
import sys

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from .helper_client_cli import create, getList, delete, rename, show, load, release, cli
from Types import FieldDataTypes, BUILT_IN_ANALYZERS
from pymilvus import FieldSchema, DataType, FunctionType, Function

NOT_SET = "Not set"


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

    def handleNullableAndDefaultValue(isPrimary, fieldType):
        if isPrimary:
            return None, NOT_SET
        else:
            nullable = click.prompt("Nullable", default=False, type=bool)
            defaultValue = click.prompt(
                f"Default value (type: {fieldType}):", default=NOT_SET
            )

            return nullable, defaultValue

    needFunction = click.confirm(
        "Do you want to add embedding function?", default=False
    )
    functions = None
    if needFunction:
        funcName = click.prompt("Function name", type=str)
        funcType = click.prompt(
            "Please input function type(BM25(1))",
            default=1,
            type=int,
        )
        inputFieldName = click.prompt(
            "Name of the VARCHAR field containing raw text data", type=str
        )
        outputFieldName = click.prompt(
            "Name of the SPARSE_FLOAT_VECTOR field reserved to store generated embeddings",
            type=str,
        )
        needCreateFields = click.confirm(
            "Do you want to create two fields for the function? Otherwise, you should create them yourself later. Default is False",
            default=False,
        )
        functions = [
            Function(
                name=funcName,
                function_type=funcType,
                input_field_names=[inputFieldName],
                output_field_names=[outputFieldName],
            )
        ]
        if needCreateFields:
            fields.append(
                FieldSchema(
                    name=inputFieldName,
                    dtype=DataType.VARCHAR,
                    max_length=65535,
                    enable_analyzer=True,
                    description="Raw text data",
                )
            )
            fields.append(
                FieldSchema(
                    name=outputFieldName,
                    dtype=DataType.SPARSE_FLOAT_VECTOR,
                    description="Generated embeddings",
                )
            )

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
            field_schema_params = {
                "name": fieldName,
                "dtype": DataType[upperFieldType],
                "description": fieldDesc,
            }
            if upperFieldType != "SPARSE_FLOAT_VECTOR":
                dim = click.prompt("Dimension", type=int)
                field_schema_params["dim"] = dim  # Pass dim parameter directly, not in params dict
            fields.append(FieldSchema(**field_schema_params))

        elif upperFieldType == "VARCHAR":
            maxLength = click.prompt("Max length", default=65535, type=int)
            isPrimary = handle_primary_field(fieldName, primaryField)
            if isPrimary:
                primaryField = fieldName

            nullable, defaultValue = handleNullableAndDefaultValue(
                isPrimary, fieldType=upperFieldType
            )
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

            field_schema_params = {
                "name": fieldName,
                "dtype": DataType[upperFieldType],
                "max_length": int(maxLength),
                "enable_analyzer": enableAnalyzer,
                "enable_match": enableMatch,
                "analyzer_params": analyzer_params,
                "nullable": nullable,
                "description": fieldDesc,
            }
            if defaultValue is not NOT_SET:
                field_schema_params["default_value"] = defaultValue

            fields.append(FieldSchema(**field_schema_params))
        elif upperFieldType == "ARRAY":
            maxCapacity = click.prompt("Max capacity", type=int)
            elementType = click.prompt(
                "Element type", type=click.Choice(FieldDataTypes)
            )
            maxLength = None
            if elementType.upper() == "VARCHAR":
                maxLength = click.prompt("Max length", type=int)
            nullable, defaultValue = handleNullableAndDefaultValue(
                False, fieldType=upperFieldType
            )

            field_schema_params = {
                "name": fieldName,
                "dtype": DataType[upperFieldType],
                "element_type": DataType[elementType.upper()],
                "max_capacity": int(maxCapacity),
                "max_length": int(maxLength) if maxLength else None,
                "nullable": nullable,
                "description": fieldDesc,
            }
            if defaultValue is not NOT_SET:
                field_schema_params["default_value"] = defaultValue

            fields.append(FieldSchema(**field_schema_params))
        else:
            isPrimary = False
            if upperFieldType == "INT64":
                isPrimary = handle_primary_field(fieldName, primaryField)
                if isPrimary:
                    primaryField = fieldName
            nullable, defaultValue = handleNullableAndDefaultValue(
                isPrimary, fieldType=upperFieldType
            )

            field_schema_params = {
                "name": fieldName,
                "dtype": DataType[upperFieldType],
                "description": fieldDesc,
                "nullable": nullable,
            }
            if defaultValue is not NOT_SET:
                field_schema_params["default_value"] = defaultValue

            fields.append(FieldSchema(**field_schema_params))
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
            functions,
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


@cli.command("flush")
@click.option(
    "-c", "--collection-name", "collectionName", help="The name of collection.", required=True
)
@click.option(
    "-t",
    "--timeout",
    "timeout",
    help="[Optional] - Timeout in seconds.",
    default=None,
    type=float,
)
@click.pass_obj
def flush_collection(obj, collectionName, timeout):
    """
    Flush collection data to storage.

    Example:

        milvus_cli > flush -c test_collection
    """
    try:
        result = obj.collection.flush(collectionName, timeout)
        click.echo(result)
    except Exception as e:
        click.echo(message=e, err=True)


@cli.command("compact")
@click.option(
    "-c", "--collection-name", "collectionName", help="The name of collection.", required=True
)
@click.option(
    "-t",
    "--timeout",
    "timeout",
    help="[Optional] - Timeout in seconds.",
    default=None,
    type=float,
)
@click.pass_obj
def compact_collection(obj, collectionName, timeout):
    """
    Compact collection to merge small segments and remove deleted data.

    Example:

        milvus_cli > compact -c test_collection
    """
    try:
        result = obj.collection.compact(collectionName, timeout)
        click.echo(result["message"])
        click.echo(f"Compaction ID: {result['compaction_id']}")
    except Exception as e:
        click.echo(message=e, err=True)


@show.command("compaction_state")
@click.option(
    "-id", "--compaction-id", "compactionId", help="The compaction ID.", required=True, type=int
)
@click.pass_obj
def show_compaction_state(obj,  compactionId):
    """
    Show compaction state.

    Example:

        milvus_cli > show compaction_state -id 123
    """
    try:
        state = obj.collection.get_compaction_state(compactionId)
        click.echo(f"Compaction state: {state}")
    except Exception as e:
        click.echo(message=e, err=True)


@show.command("compaction_plans")
@click.option(
    "-c", "--collection-name", "collectionName", help="The name of collection.", required=True
)
@click.option(
    "-id", "--compaction-id", "compactionId", help="The compaction ID.", required=True, type=int
)
@click.pass_obj
def show_compaction_plans(obj, collectionName, compactionId):
    """
    Show compaction plans.

    Example:

        milvus_cli > show compaction_plans -c test_collection -id 123
    """
    try:
        plans = obj.collection.get_compaction_plans(collectionName, compactionId)
        click.echo(plans)
    except Exception as e:
        click.echo(message=e, err=True)


@show.command("replicas")
@click.option(
    "-c", "--collection-name", "collectionName", help="The name of collection.", required=True
)
@click.pass_obj
def show_replicas(obj, collectionName):
    """
    Show replicas information.

    Example:

        milvus_cli > show replicas -c test_collection
    """
    try:
        replicas = obj.collection.get_replicas(collectionName)
        click.echo(replicas)
    except Exception as e:
        click.echo(message=e, err=True)


@show.command("load_state")
@click.option(
    "-c", "--collection-name", "collectionName", help="The name of collection.", required=True
)
@click.option(
    "-p",
    "--partition",
    "partitionName",
    help="[Optional] - The name of partition.",
    default=None,
)
@click.pass_obj
def show_load_state(obj, collectionName, partitionName):
    """
    Show load state of collection or partition.

    Example:

        milvus_cli > show load_state -c test_collection
    """
    try:
        state = obj.collection.load_state(collectionName, partitionName)
        click.echo(f"Load state: {state}")
    except Exception as e:
        click.echo(message=e, err=True)


@show.command("query_segment_info")
@click.option(
    "-c", "--collection-name", "collectionName", help="The name of collection.", required=True
)
@click.pass_obj
def show_query_segment_info(obj, collectionName):
    """
    Show query segment information.

    Example:

        milvus_cli > show query_segment_info -c test_collection
    """
    try:
        from pymilvus import utility
        info = utility.get_query_segment_info(collectionName)
        if info:
            click.echo(info)
        else:
            click.echo("No segment info available.")
    except Exception as e:
        click.echo(message=e, err=True)
