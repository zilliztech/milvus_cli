from tabulate import tabulate
import click
import os
import sys

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from .init_client_cli import cli
from .helper_cli import create, getList, delete, rename, show, load, release, alter
from Types import FieldDataTypes, BUILT_IN_ANALYZERS
from pymilvus import FieldSchema, DataType, FunctionType, Function

NOT_SET = "Not set"


@create.command("collection")
@click.option(
    "--schema-file",
    "schema_file",
    help="JSON file containing collection schema",
    type=click.Path(exists=True),
    default=None,
)
@click.pass_obj
def create_collection(obj, schema_file):
    """
    Create a new collection with schema definition.

    USAGE:
        milvus_cli > create collection [--schema-file <path>]

    OPTIONS:
        --schema-file    JSON file containing collection schema (optional)
                         If provided, creates collection from file instead of interactive mode

    INTERACTIVE PROMPTS (when --schema-file is not provided):
        Collection name      Unique identifier for the collection
        Auto ID              Auto-generate primary key values (true/false)
        Description          Optional collection description
        Dynamic field        Allow undefined fields (true/false)
        Consistency level    0=Strong, 1=Bounded, 2=Session, 3=Eventually
        Shards number        Number of data shards (default: 1)
        Embedding function   Optional BM25 function for text search
        Fields               Define schema fields interactively

    FIELD TYPES:
        Scalar:    INT8, INT16, INT32, INT64, FLOAT, DOUBLE, BOOL, VARCHAR
        Vector:    BINARY_VECTOR, FLOAT_VECTOR, BFLOAT16_VECTOR, FLOAT16_VECTOR
        Sparse:    SPARSE_FLOAT_VECTOR
        Complex:   JSON, ARRAY

    SCHEMA FILE FORMAT (JSON):
        {
            "collection_name": "my_collection",
            "description": "Optional description",
            "auto_id": true,
            "enable_dynamic_field": false,
            "consistency_level": 1,
            "shards_num": 1,
            "fields": [
                {"name": "id", "type": "INT64", "is_primary": true},
                {"name": "embedding", "type": "FLOAT_VECTOR", "dim": 128},
                {"name": "text", "type": "VARCHAR", "max_length": 1024}
            ]
        }

    EXAMPLES:
        # Interactive mode
        milvus_cli > create collection

        # From schema file
        milvus_cli > create collection --schema-file /path/to/schema.json

        # Interactive example
        Collection name: products
        Auto ID: true
        Description: Product catalog
        Dynamic field: false
        Consistency level: 1
        Shards: 2

        Field name: embedding
        Field type: FLOAT_VECTOR
        Dimension: 768

    NOTES:
        - Collection names must be unique within a database
        - Primary key field is required (INT64 or VARCHAR)
        - Vector fields require dimension specification

    SEE ALSO:
        list collections, show collection, delete collection, create index
    """
    # Handle schema file if provided
    if schema_file:
        import json
        with open(schema_file, 'r') as f:
            schema_data = json.load(f)

        # Extract collection configuration from schema
        collection_name = schema_data.get('collection_name', schema_data.get('name'))
        if not collection_name:
            click.echo("Error: Schema file must contain 'collection_name' or 'name' field", err=True)
            return

        fields = schema_data.get('fields', [])
        enable_dynamic = schema_data.get('enable_dynamic_field', False)
        auto_id = schema_data.get('auto_id', False)
        description = schema_data.get('description', '')

        # Build schema fields using FieldSchema
        schema_fields = []
        primary_field = None
        for field in fields:
            field_name = field.get('name', field.get('field_name'))
            field_type = field.get('type', field.get('datatype', '')).upper()

            field_params = {
                'name': field_name,
                'dtype': DataType[field_type],
                'description': field.get('description', ''),
            }

            if field.get('is_primary', False):
                primary_field = field_name

            if field.get('max_length'):
                field_params['max_length'] = field['max_length']
            if field.get('dim'):
                field_params['dim'] = field['dim']
            if field.get('nullable') is not None:
                field_params['nullable'] = field['nullable']
            if field.get('element_type'):
                field_params['element_type'] = DataType[field['element_type'].upper()]
            if field.get('max_capacity'):
                field_params['max_capacity'] = field['max_capacity']

            schema_fields.append(FieldSchema(**field_params))

        if not primary_field:
            click.echo("Error: Schema must have a primary key field (set 'is_primary': true)", err=True)
            return

        # Create collection using existing method
        click.echo(
            obj.collection.create_collection(
                collection_name,
                primary_field,
                schema_fields,
                auto_id,
                description,
                enable_dynamic,
                schema_data.get('consistency_level', 1),
                schema_data.get('shards_num', 1),
                None,  # functions
            )
        )
        click.echo("Create collection successfully!")
        return

    # Existing interactive mode
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
    List all collections in the current database.

    USAGE:
        milvus_cli > list collections

    OUTPUT:
        Lists collection names in current output format (table/json/csv).

    EXAMPLES:
        milvus_cli > list collections

        # With JSON output
        milvus_cli > set output json
        milvus_cli > list collections

    SEE ALSO:
        show collection, create collection, use database
    """
    try:
        res = obj.collection.list_collections()
        click.echo(obj.formatter.format_list(res, header="Collection"))
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
@click.option(
    "--yes", "-y",
    is_flag=True,
    help="Skip confirmation prompt",
)
@click.pass_obj
def delete_collection(obj, collectionName, yes):
    """
    Drops the collection together with its index files.

    Example:

        milvus_cli > delete collection -c car
        milvus_cli > delete collection -c car --yes
    """
    if not yes:
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
        client = obj.connection.get_client()
        # Get loaded and persistent segments info
        loaded_segments = client.list_loaded_segments(collection_name=collectionName)
        persistent_segments = client.list_persistent_segments(collection_name=collectionName)

        if loaded_segments or persistent_segments:
            click.echo(f"Loaded segments: {len(loaded_segments) if loaded_segments else 0}")
            if loaded_segments:
                for seg in loaded_segments[:10]:  # Show first 10
                    click.echo(f"  - Segment ID: {seg}")
                if len(loaded_segments) > 10:
                    click.echo(f"  ... and {len(loaded_segments) - 10} more")

            click.echo(f"\nPersistent segments: {len(persistent_segments) if persistent_segments else 0}")
            if persistent_segments:
                for seg in persistent_segments[:10]:  # Show first 10
                    click.echo(f"  - Segment ID: {seg}")
                if len(persistent_segments) > 10:
                    click.echo(f"  ... and {len(persistent_segments) - 10} more")
        else:
            click.echo("No segment info available.")
    except Exception as e:
        click.echo(message=e, err=True)


@alter.command("collection_properties")
@click.option(
    "-c", "--collection-name", "collectionName", help="The name of collection.", required=True
)
@click.pass_obj
def alter_collection_properties(obj, collectionName):
    """
    Alter collection properties like TTL, mmap, etc.

    USAGE:
        milvus_cli > alter collection_properties -c <collection_name>

    INTERACTIVE PROMPTS:
        Property key      The property to set (e.g., collection.ttl.seconds)
        Property value    The value to set

    AVAILABLE PROPERTIES:
        collection.ttl.seconds    Time-to-live in seconds (0 = disabled)
        mmap.enabled              Enable memory-mapped storage (true/false)

    EXAMPLES:
        milvus_cli > alter collection_properties -c products
        Property key: collection.ttl.seconds
        Property value: 86400

    SEE ALSO:
        show collection, drop collection_properties
    """
    try:
        properties = {}
        while True:
            key = click.prompt("Property key (e.g., collection.ttl.seconds, mmap.enabled)")
            value = click.prompt(f"Property value for '{key}'")
            # Try to convert to appropriate type
            if value.lower() in ["true", "false"]:
                value = value.lower() == "true"
            elif value.isdigit():
                value = int(value)
            properties[key] = value
            if not click.confirm("Add another property?", default=False):
                break
        result = obj.collection.alter_collection_properties(collectionName, properties)
        click.echo(result)
    except Exception as e:
        click.echo(message=e, err=True)


@delete.command("collection_properties")
@click.option(
    "-c", "--collection-name", "collectionName", help="The name of collection.", required=True
)
@click.option(
    "-k", "--property-key", "propertyKey", help="The property key to delete.", required=True
)
@click.pass_obj
def drop_collection_properties(obj, collectionName, propertyKey):
    """
    Drop collection properties by key.

    USAGE:
        milvus_cli > delete collection_properties -c <collection> -k <key>

    OPTIONS:
        -c, --collection-name    Target collection (required)
        -k, --property-key       Property key to delete (required)

    EXAMPLES:
        milvus_cli > delete collection_properties -c products -k collection.ttl.seconds

    SEE ALSO:
        alter collection_properties, show collection
    """
    try:
        result = obj.collection.drop_collection_properties(collectionName, [propertyKey])
        click.echo(result)
    except Exception as e:
        click.echo(message=e, err=True)


@alter.command("collection_field")
@click.option(
    "-c", "--collection-name", "collectionName", help="The name of collection.", required=True
)
@click.option(
    "-f", "--field-name", "fieldName", help="The name of field to alter.", required=True
)
@click.pass_obj
def alter_collection_field(obj, collectionName, fieldName):
    """
    Alter collection field properties.

    USAGE:
        milvus_cli > alter collection_field -c <collection> -f <field>

    INTERACTIVE PROMPTS:
        Property key      The field property to set
        Property value    The value to set

    AVAILABLE PROPERTIES:
        max_length       Maximum byte length for VARCHAR fields
        max_capacity     Maximum elements for ARRAY fields
        mmap.enabled     Enable memory-mapped storage (true/false)

    EXAMPLES:
        milvus_cli > alter collection_field -c products -f description
        Property key: max_length
        Property value: 1024

    SEE ALSO:
        show collection, create collection
    """
    try:
        field_params = {}
        while True:
            key = click.prompt("Property key (e.g., max_length, max_capacity, mmap.enabled)")
            value = click.prompt(f"Property value for '{key}'")
            # Try to convert to appropriate type
            if value.lower() in ["true", "false"]:
                value = value.lower() == "true"
            elif value.isdigit():
                value = int(value)
            field_params[key] = value
            if not click.confirm("Add another property?", default=False):
                break
        result = obj.collection.alter_collection_field(collectionName, fieldName, field_params)
        click.echo(result)
    except Exception as e:
        click.echo(message=e, err=True)


@show.command("collection_stats")
@click.option(
    "-c", "--collection-name", "collectionName", help="The name of collection.", required=True
)
@click.pass_obj
def show_collection_stats(obj, collectionName):
    """
    Show collection statistics.

    USAGE:
        milvus_cli > show collection_stats -c <collection_name>

    OUTPUT:
        Displays statistics including:
        - row_count: Total number of entities
        - data_size: Size of data in bytes

    EXAMPLES:
        milvus_cli > show collection_stats -c products

    SEE ALSO:
        show collection, list collections
    """
    try:
        stats = obj.collection.get_collection_stats(collectionName)
        click.echo(f"Collection Statistics for '{collectionName}':")
        click.echo(stats)
    except Exception as e:
        click.echo(message=e, err=True)


@cli.command("flush_all")
@click.option(
    "-t",
    "--timeout",
    "timeout",
    help="[Optional] - Timeout in seconds.",
    default=None,
    type=float,
)
@click.pass_obj
def flush_all_collections(obj, timeout):
    """
    Flush all collections to storage.

    USAGE:
        milvus_cli > flush_all [-t <timeout>]

    OPTIONS:
        -t, --timeout    Timeout in seconds (optional)

    EXAMPLES:
        milvus_cli > flush_all
        milvus_cli > flush_all -t 60

    SEE ALSO:
        flush, show flush_state
    """
    try:
        result = obj.collection.flush_all(timeout)
        click.echo(result)
    except Exception as e:
        click.echo(message=e, err=True)


@show.command("flush_state")
@click.option(
    "-c", "--collection-name", "collectionName", help="The name of collection.", required=True
)
@click.pass_obj
def show_flush_state(obj, collectionName):
    """
    Show flush state for a collection.

    USAGE:
        milvus_cli > show flush_state -c <collection_name>

    EXAMPLES:
        milvus_cli > show flush_state -c products

    SEE ALSO:
        flush, flush_all
    """
    try:
        state = obj.collection.get_flush_state(collectionName)
        click.echo(f"Flush state for '{collectionName}': {state}")
    except Exception as e:
        click.echo(message=e, err=True)


@cli.command("query_iterator")
@click.pass_obj
def query_iterator(obj):
    """
    Query entities with iterator for large result sets.

    USAGE:
        milvus_cli > query_iterator

    INTERACTIVE PROMPTS:
        Collection name      Target collection
        Filter expression    Query condition
        Output fields        Fields to return
        Batch size           Number of results per batch (default: 1000)
        Limit                Maximum total results (optional)

    EXAMPLES:
        milvus_cli > query_iterator

        Collection name: products
        Filter expression: price > 100
        Output fields: id, name, price
        Batch size: 500

    SEE ALSO:
        query, search_iterator
    """
    try:
        collectionName = click.prompt(
            "Collection name", type=click.Choice(obj.collection.list_collections())
        )
        expr = click.prompt("Filter expression")
        outputFields = click.prompt(
            f'Output fields (comma separated) {obj.collection.list_field_names(collectionName)}',
            default="",
        )
        batchSize = click.prompt("Batch size", default=1000, type=int)
        limit = click.prompt("Limit (0 for no limit)", default=0, type=int)

        output_fields_list = None
        if outputFields:
            # Strip whitespace and quotes, filter out empty strings
            fields = [f.strip().strip("'\"") for f in outputFields.split(",")]
            output_fields_list = [f for f in fields if f] or None
        limit_val = limit if limit > 0 else None

        # Use MilvusClient query_iterator for better compatibility
        client = obj.connection.get_client()
        iterator = client.query_iterator(
            collection_name=collectionName,
            filter=expr,
            output_fields=output_fields_list,
            batch_size=batchSize,
            limit=limit_val,
        )

        total_count = 0
        batch_num = 0
        while True:
            result = iterator.next()
            if not result:
                break
            batch_num += 1
            total_count += len(result)
            click.echo(f"\n--- Batch {batch_num} ({len(result)} results) ---")
            click.echo(obj.formatter.format_output(result))

            if not click.confirm("Continue to next batch?", default=True):
                break

        iterator.close()
        click.echo(f"\nTotal results retrieved: {total_count}")

    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)


@cli.command("search_iterator")
@click.pass_obj
def search_iterator(obj):
    """
    Search with iterator for large result sets.

    USAGE:
        milvus_cli > search_iterator

    INTERACTIVE PROMPTS:
        Collection name      Target collection
        Vector field         Field containing vectors
        Search vector        Query vector
        Batch size           Results per batch
        Limit                Maximum total results
        Filter expression    Optional pre-filter
        Output fields        Fields to return

    EXAMPLES:
        milvus_cli > search_iterator

        Collection name: products
        Vector field: embedding
        Search vector: [0.1, 0.2, 0.3, ...]
        Batch size: 100
        Limit: 1000

    SEE ALSO:
        search, query_iterator
    """
    try:
        import json

        collectionName = click.prompt(
            "Collection name", type=click.Choice(obj.collection.list_collections())
        )
        annsField = click.prompt(
            "Vector field",
            type=click.Choice(obj.collection.list_field_names(collectionName)),
        )

        vector_str = click.prompt("Search vector (e.g., [0.1, 0.2, ...])")
        vector = json.loads(vector_str)
        data = [vector]

        batchSize = click.prompt("Batch size", default=100, type=int)
        limit = click.prompt("Limit", default=1000, type=int)

        # Get index info for metric type
        indexes = obj.index.list_indexes(collectionName, onlyData=True)
        metric_type = "L2"
        for index in indexes:
            if index.get("field_name") == annsField:
                metric_type = index.get("metric_type", "L2")
                break

        expr = click.prompt("Filter expression (optional)", default="")

        client = obj.connection.get_client()
        outputFields = click.prompt(
            f'Output fields (comma separated) {obj.collection.list_field_names(collectionName)}',
            default="",
        )
        output_fields_list = None
        if outputFields:
            # Strip whitespace and quotes, filter out empty strings
            fields = [f.strip().strip("'\"") for f in outputFields.split(",")]
            output_fields_list = [f for f in fields if f] or None

        # Use MilvusClient search_iterator (more robust than ORM Collection)
        iterator = client.search_iterator(
            collection_name=collectionName,
            data=data,
            anns_field=annsField,
            search_params={"metric_type": metric_type},
            batch_size=batchSize,
            limit=limit,
            filter=expr if expr else None,
            output_fields=output_fields_list,
        )

        total_count = 0
        batch_num = 0
        while True:
            result = iterator.next()
            if not result:
                break
            batch_num += 1
            total_count += len(result)
            click.echo(f"\n--- Batch {batch_num} ({len(result)} results) ---")
            for hit in result:
                # Handle both dict and object result formats
                if isinstance(hit, dict):
                    hit_id = hit.get("id")
                    distance = hit.get("distance")
                    fields = {k: v for k, v in hit.items() if k not in ("id", "distance")}
                else:
                    hit_id = hit.id
                    distance = hit.distance
                    fields = getattr(hit, "fields", getattr(hit, "entity", {}))
                click.echo(f"ID: {hit_id}, Distance: {distance}, Fields: {fields}")

            if not click.confirm("Continue to next batch?", default=True):
                break

        iterator.close()
        click.echo(f"\nTotal results retrieved: {total_count}")

    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
