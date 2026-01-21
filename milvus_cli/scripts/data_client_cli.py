from .helper_client_cli import delete, insert, cli, show, getList

import click
import os
import sys

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from Validation import validateQueryParams, validateSearchParams
from Types import ParameterException, IndexTypesMap, DataTypeByNum
from Fs import readCsvFile
import json
from tabulate import tabulate
from pymilvus import DataType


@delete.command("entities")
@click.option("-c", "--collection-name", "collectionName", help="Collection name.")
@click.option(
    "-p",
    "--partition",
    "partitionName",
    help="[Optional] - Name of partitions that contain entities.",
    default=None,
)
@click.pass_obj
def delete_entities(
    obj,
    collectionName,
    partitionName,
):
    """
    Delete entities using filter expression.

    USAGE:
        milvus_cli > delete entities -c <collection> [-p <partition>]

    OPTIONS:
        -c, --collection-name    Target collection (required)
        -p, --partition          Limit deletion to partition (optional)

    INTERACTIVE PROMPTS:
        Expression    Filter to select entities for deletion
                     Example: "id in [1, 2, 3]"
                     Example: "category == 'obsolete'"

    WARNING:
        This action is irreversible! Deleted entities cannot be recovered.

    EXAMPLES:
        # Delete by ID
        milvus_cli > delete entities -c products
        Expression: id in [100, 101, 102]

        # Delete by condition
        milvus_cli > delete entities -c products
        Expression: status == "deleted" and updated_at < 1704067200

    SEE ALSO:
        delete ids, query
    """
    expr = click.prompt(
        '''The expression to specify entities to be deleted, such as "film_id in [ 0, 1 ]"'''
    )
    click.echo(
        "You are trying to delete the entities of collection. This action cannot be undone!\n"
    )
    if not click.confirm("Do you want to continue?"):
        return
    result = obj.data.delete_entities(expr, collectionName, partitionName)
    click.echo(result)


@cli.command("query")
@click.pass_obj
def query(obj):
    """
    Query entities with filter expressions.

    USAGE:
        milvus_cli > query

    INTERACTIVE PROMPTS:
        Collection name      Select from available collections
        Query expression     Filter condition (e.g., "id > 100")
        Partition names      Optional, comma-separated
        Output fields        Fields to return, comma-separated
        Timeout              Request timeout in seconds
        Guarantee timestamp  Consistency snapshot point
        Graceful time        Bounded consistency window (default: 5s)

    EXPRESSION SYNTAX:
        Comparison:    id > 100, price <= 50.0
        In/Not In:     color in ["red", "blue"]
        Logical:       id > 10 and price < 100
        String match:  name like "test%"

    EXAMPLES:
        milvus_cli > query

        Collection name: products
        The query expression: category == "electronics"
        Fields to return: id, name, price

    OUTPUT:
        Results displayed in current format (table/json/csv).
        Use 'set output json' for JSON output.

    SEE ALSO:
        search, get, set output
    """
    collectionName = click.prompt(
        "Collection name", type=click.Choice(obj.collection.list_collections())
    )
    expr = click.prompt("The query expression")
    partitionNames = click.prompt(
        'The names of partitions to query (split by "," if multiple, press Enter to skip)',
        default="",
    )
    outputFields = click.prompt(
        f'Fields to return(split by "," if multiple) {obj.collection.list_field_names(collectionName)}',
        default="",
    )
    timeout = click.prompt("timeout", default="")
    guarantee_timestamp = click.prompt(
        "Guarantee timestamp. This instructs Milvus to see all operations performed before a provided timestamp. If no such timestamp is provided, then Milvus will search all operations performed to date.",
        default=0,
        type=int,
    )
    graceful_time = click.prompt(
        "Graceful time. Only used in bounded consistency level. If graceful_time is set, PyMilvus will use current timestamp minus the graceful_time as the guarantee_timestamp. This option is 5s by default if not set.",
        default=5,
        type=int,
    )

    try:
        queryParameters = validateQueryParams(
            expr,
            outputFields,
            timeout,
            guarantee_timestamp,
            graceful_time,
            partitionNames,
        )
    except ParameterException as pe:
        click.echo("Error!\n{}".format(str(pe)))
    else:
        results = obj.data.query(collectionName, queryParameters)
        if results:
            click.echo(obj.formatter.format_output(results))
        else:
            click.echo("No results found.")


@insert.command("file")
@click.option(
    "-c",
    "--collection-name",
    "collectionName",
    help="The name of collection to be imported.",
)
@click.option(
    "-p",
    "--partition",
    "partitionName",
    help="[Optional] - The partition name which the data will be inserted to, if partition name is not passed, then the data will be inserted to '_default' partition.",
    default=None,
)
@click.option(
    "-t",
    "--timeout",
    "timeout",
    help="[Optional] - An optional duration of time in seconds to allow for the RPC. If timeout is not set, the client keeps waiting until the server responds or an error occurs.",
    default=None,
    type=float,
)
@click.argument("path")
@click.pass_obj
def insert_data(obj, collectionName, partitionName, timeout, path):
    """
    Import data from CSV file into a collection.

    USAGE:
        milvus_cli > insert file -c <collection> [options] <path>

    ARGUMENTS:
        path    Path to CSV file (local path or URL)

    OPTIONS:
        -c, --collection-name    Target collection (required)
        -p, --partition          Target partition (default: _default)
        -t, --timeout            Request timeout in seconds

    CSV FORMAT:
        - First row must be headers matching field names
        - Vector fields: comma-separated values in brackets
          Example: "[1.0, 2.0, 3.0]"
        - JSON fields: valid JSON strings

    EXAMPLES:
        # Insert from local file
        milvus_cli > insert file -c products ./data/products.csv

        # Insert from URL
        milvus_cli > insert file -c products https://example.com/data.csv

        # Insert to specific partition
        milvus_cli > insert file -c products -p 2024_data ./products.csv

    ERRORS:
        - Schema mismatch: CSV headers must match collection field names
        - Dimension error: Vector dimensions must match schema
        - Type error: Values must be convertible to field types

    SEE ALSO:
        insert row, upsert file, create collection
    """
    try:
        result = readCsvFile(path.replace('"', "").replace("'", ""))
        data = result["data"]
        result = obj.data.insert(collectionName, data, partitionName, timeout)
    except Exception as e:
        click.echo("Error!\n{}".format(str(e)))
    else:
        click.echo(f"\nInserted successfully.\n")
        click.echo(result)


@insert.command("row")
@click.pass_obj
def insert_row(obj):
    """
    Insert a row of data into a collection.

    Example:
        milvus_cli > insert row
    """
    try:
        collectionName = click.prompt(
            "Collection name", type=click.Choice(obj.collection.list_collections())
        )
        partitionName = click.prompt(
            "Partition name",
            default="_default",
        )
        fields = obj.collection.list_fields_info(collectionName)
        data = {}
        for field in fields:
            fieldType = field["type"]
            autoId = field["autoId"]
            if autoId:
                continue
            value = click.prompt(
                f"Enter value for {field['name']} ({fieldType})", default=""
            )
            if value == "":
                value = None
            elif fieldType in [DataType.INT8, DataType.INT16, DataType.INT32, DataType.INT64]:
                value = int(value)
            elif fieldType in [DataType.FLOAT, DataType.DOUBLE]:
                value = float(value)
            elif fieldType in [DataType.BOOL]:
                value = bool(value)
            elif fieldType == DataType.BINARY_VECTOR:
                # Binary vector needs bytes
                value = bytes([int(x.strip()) for x in value.strip("[]").split(",")])
            elif fieldType in [
                DataType.FLOAT_VECTOR,
                DataType.FLOAT16_VECTOR,
                DataType.BFLOAT16_VECTOR,
            ]:
                value = [float(x.strip()) for x in value.strip("[]").split(",")]
            elif fieldType == DataType.SPARSE_FLOAT_VECTOR:
                # Sparse vector can be dict format {index: value, ...} or list format
                if value.strip().startswith("{"):
                    value = json.loads(value)
                else:
                    value = [float(x.strip()) for x in value.strip("[]").split(",")]
            elif fieldType == DataType.ARRAY:
                # Array element type should be checked from field schema
                element_type = field.get("element_type")
                if element_type == DataType.INT8 or element_type == DataType.INT16 or element_type == DataType.INT32 or element_type == DataType.INT64:
                    value = [int(x.strip()) for x in value.strip("[]").split(",")]
                elif element_type == DataType.FLOAT or element_type == DataType.DOUBLE:
                    value = [float(x.strip()) for x in value.strip("[]").split(",")]
                elif element_type == DataType.VARCHAR or element_type == DataType.STRING:
                    # For string arrays, split by comma and strip quotes if present
                    value = [x.strip().strip("'\"") for x in value.strip("[]").split(",")]
                elif element_type == DataType.BOOL:
                    value = [x.strip().lower() in ['true', '1', 'yes'] for x in value.strip("[]").split(",")]
                else:
                    # Fallback: try float
                    value = [float(x.strip()) for x in value.strip("[]").split(",")]
            elif fieldType in [DataType.JSON]:
                value = json.loads(value)
            if value != None:
                data[field["name"]] = value
        result = obj.data.insert(collectionName, [data], partitionName)
    except Exception as e:
        click.echo("Error!\n{}".format(str(e)))
    else:
        click.echo(f"\nInserted successfully.\n")
        click.echo(result)


@cli.group("upsert", no_args_is_help=False)
@click.pass_obj
def upsert(obj):
    """Upsert entities (insert or update)"""
    pass


@upsert.command("file")
@click.option(
    "-c",
    "--collection-name",
    "collectionName",
    help="The name of collection to be upserted.",
)
@click.option(
    "-p",
    "--partition",
    "partitionName",
    help="[Optional] - The partition name which the data will be upserted to.",
    default=None,
)
@click.option(
    "-t",
    "--timeout",
    "timeout",
    help="[Optional] - An optional duration of time in seconds to allow for the RPC.",
    default=None,
    type=float,
)
@click.argument("path")
@click.pass_obj
def upsert_data(obj, collectionName, partitionName, timeout, path):
    """
    Upsert data from csv file(local or remote) with headers.

    Example:

        milvus_cli > upsert file -c car 'data.csv'
    """
    try:
        result = readCsvFile(path.replace('"', "").replace("'", ""))
        data = result["data"]
        result = obj.data.upsert(collectionName, data, partitionName, timeout)
    except Exception as e:
        click.echo("Error!\n{}".format(str(e)))
    else:
        click.echo(f"\nUpserted successfully.\n")
        click.echo(result)


@upsert.command("row")
@click.pass_obj
def upsert_row(obj):
    """
    Upsert a row of data into a collection.

    Example:
        milvus_cli > upsert row
    """
    try:
        collectionName = click.prompt(
            "Collection name", type=click.Choice(obj.collection.list_collections())
        )
        partitionName = click.prompt(
            "Partition name",
            default="_default",
        )
        fields = obj.collection.list_fields_info(collectionName)
        data = {}
        for field in fields:
            fieldType = field["type"]
            value = click.prompt(
                f"Enter value for {field['name']} ({fieldType})", default=""
            )
            if value == "":
                value = None
            elif fieldType in ["INT8", "INT16", "INT32", "INT64"]:
                value = int(value)
            elif fieldType in ["FLOAT", "DOUBLE"]:
                value = float(value)
            elif fieldType in ["BOOL"]:
                value = value.lower() in ['true', '1', 'yes']
            elif fieldType == "BINARY_VECTOR":
                # Binary vector needs bytes
                value = bytes([int(x.strip()) for x in value.strip("[]").split(",")])
            elif fieldType in [
                "FLOAT_VECTOR",
                "FLOAT16_VECTOR",
                "BFLOAT16_VECTOR",
            ]:
                value = [float(x.strip()) for x in value.strip("[]").split(",")]
            elif fieldType == "SPARSE_FLOAT_VECTOR":
                # Sparse vector can be dict format {index: value, ...} or list format
                if value.strip().startswith("{"):
                    value = json.loads(value)
                else:
                    value = [float(x.strip()) for x in value.strip("[]").split(",")]
            elif fieldType == "ARRAY":
                # Array element type should be checked from field schema
                # element_type is int, convert to string using DataTypeByNum
                element_type = field.get("element_type")
                if isinstance(element_type, int):
                    element_type = DataTypeByNum.get(element_type, "UNKNOWN")
                if element_type in ["INT8", "INT16", "INT32", "INT64"]:
                    value = [int(x.strip()) for x in value.strip("[]").split(",")]
                elif element_type in ["FLOAT", "DOUBLE"]:
                    value = [float(x.strip()) for x in value.strip("[]").split(",")]
                elif element_type in ["VARCHAR", "STRING"]:
                    # For string arrays, split by comma and strip quotes if present
                    value = [x.strip().strip("'\"") for x in value.strip("[]").split(",")]
                elif element_type == "BOOL":
                    value = [x.strip().lower() in ['true', '1', 'yes'] for x in value.strip("[]").split(",")]
                else:
                    # Fallback: try float
                    value = [float(x.strip()) for x in value.strip("[]").split(",")]
            elif fieldType in ["JSON"]:
                value = json.loads(value)
            if value != None:
                data[field["name"]] = value
        result = obj.data.upsert(collectionName, [data], partitionName)
    except Exception as e:
        click.echo("Error!\n{}".format(str(e)))
    else:
        click.echo(f"\nUpserted successfully.\n")
        click.echo(result)


@cli.command("get")
@click.pass_obj
def get_by_ids(obj):
    """
    Get entities by IDs.

    Example:

        milvus_cli > get

        Collection name: car

        The IDs (e.g. [1,2,3]): [1,2,3]

        Fields to return(split by "," if multiple) []: id, color, brand
    """
    collectionName = click.prompt(
        "Collection name", type=click.Choice(obj.collection.list_collections())
    )
    ids_str = click.prompt('The IDs (e.g. [1,2,3])')
    ids = json.loads(ids_str)
    outputFields = click.prompt(
        f'Fields to return(split by "," if multiple) {obj.collection.list_field_names(collectionName)}',
        default="",
    )
    output_fields_list = [f.strip() for f in outputFields.split(",")] if outputFields else None

    try:
        results = obj.data.get_by_ids(collectionName, ids, output_fields_list)
        if results:
            click.echo(obj.formatter.format_output(results))
        else:
            click.echo("No results found.")
    except Exception as e:
        click.echo("Error!\n{}".format(str(e)))


@delete.command("ids")
@click.option("-c", "--collection-name", "collectionName", help="Collection name.")
@click.option(
    "-p",
    "--partition",
    "partitionName",
    help="[Optional] - Name of partition.",
    default=None,
)
@click.pass_obj
def delete_by_ids(
    obj,
    collectionName,
    partitionName,
):
    """
    Delete entities by IDs.

    Example:

        milvus_cli > delete ids -c test_collection
    """
    ids_str = click.prompt('The IDs to delete (e.g. [1,2,3])')
    ids = json.loads(ids_str)

    click.echo(
        f"You are trying to delete {len(ids)} entities. This action cannot be undone!\n"
    )
    if not click.confirm("Do you want to continue?"):
        return

    try:
        result = obj.data.delete_by_ids(collectionName, ids, partitionName)
        click.echo(result)
    except Exception as e:
        click.echo("Error!\n{}".format(str(e)))


@cli.command("bulk_insert")
@click.option(
    "-c",
    "--collection-name",
    "collectionName",
    help="The name of collection.",
    required=True,
)
@click.option(
    "-p",
    "--partition",
    "partitionName",
    help="[Optional] - The partition name.",
    default=None,
)
@click.option(
    "-f",
    "--files",
    "files",
    help="File paths (comma separated, e.g., 's3://bucket/file1.json,s3://bucket/file2.json').",
    required=True,
)
@click.pass_obj
def bulk_insert(obj, collectionName, partitionName, files):
    """
    Bulk insert data from remote storage (S3, MinIO, etc.).

    USAGE:
        milvus_cli > bulk_insert -c <collection> -f <files>

    OPTIONS:
        -c, --collection-name    Target collection (required)
        -p, --partition          Target partition (optional)
        -f, --files              Comma-separated file paths (required)

    SUPPORTED SOURCES:
        - S3: s3://bucket/path/file.json
        - MinIO: minio://bucket/path/file.json
        - Local: /path/to/file.json (if configured)

    FILE FORMATS:
        - JSON: Array of objects or JSON Lines
        - Parquet: Apache Parquet format

    EXAMPLES:
        # Single file
        milvus_cli > bulk_insert -c products -f 's3://mybucket/data.json'

        # Multiple files
        milvus_cli > bulk_insert -c products -f 's3://bucket/part1.json,s3://bucket/part2.json'

    NOTES:
        - Returns a task ID for tracking progress
        - Use 'show bulk_insert_state -id <task_id>' to check status
        - Use 'list bulk_insert_tasks' to see all tasks

    SEE ALSO:
        show bulk_insert_state, list bulk_insert_tasks, insert file
    """
    try:
        file_list = [f.strip() for f in files.split(",")]
        task_id = obj.data.bulk_insert(collectionName, file_list, partitionName)
        click.echo(f"Bulk insert task submitted successfully!")
        click.echo(f"Task ID: {task_id}")
    except Exception as e:
        click.echo("Error!\n{}".format(str(e)))


@show.command("bulk_insert_state")
@click.option(
    "-id",
    "--task-id",
    "taskId",
    help="The bulk insert task ID.",
    required=True,
    type=int,
)
@click.pass_obj
def show_bulk_insert_state(obj, taskId):
    """
    Show bulk insert task state.

    Example:

        milvus_cli > show bulk_insert_state -id 123
    """
    try:
        state = obj.data.get_bulk_insert_state(taskId)
        click.echo(f"Task state: {state}")
    except Exception as e:
        click.echo("Error!\n{}".format(str(e)))


@getList.command("bulk_insert_tasks")
@click.option(
    "-l",
    "--limit",
    "limit",
    help="[Optional] - Maximum number of tasks to return.",
    default=None,
    type=int,
)
@click.option(
    "-c",
    "--collection-name",
    "collectionName",
    help="[Optional] - Filter by collection name.",
    default=None,
)
@click.pass_obj
def list_bulk_insert_tasks(obj, limit, collectionName):
    """
    List bulk insert tasks.

    Example:

        milvus_cli > list bulk_insert_tasks
    """
    try:
        tasks = obj.data.list_bulk_insert_tasks(limit, collectionName)
        if tasks:
            click.echo(tasks)
        else:
            click.echo("No bulk insert tasks found.")
    except Exception as e:
        click.echo("Error!\n{}".format(str(e)))


@cli.command("search")
@click.pass_obj
def search(obj):
    """
    Perform vector similarity search.

    USAGE:
        milvus_cli > search

    INTERACTIVE PROMPTS:
        Collection name     Target collection
        Vector field        Field containing vectors
        Search vectors      Query vector (manual or from file)
        Metric type         Distance metric (auto-detected from index)
        Search params       Index-specific parameters
        Top K               Number of results to return
        Filter expression   Optional pre-filter
        Partition names     Optional partition filter
        Output fields       Fields to return

    METRIC TYPES:
        L2       Euclidean distance (smaller = more similar)
        IP       Inner product (larger = more similar)
        COSINE   Cosine similarity (larger = more similar)

    SEARCH PARAMS BY INDEX:
        IVF_FLAT:     nprobe (e.g., 10)
        IVF_SQ8:      nprobe (e.g., 10)
        HNSW:         ef (e.g., 64)
        DISKANN:      search_list (e.g., 20)
        AUTOINDEX:    (no params needed)

    EXAMPLES:
        milvus_cli > search

        Collection name: products
        Vector field: embedding
        Search vectors: [0.1, 0.2, 0.3, ...]
        Top K: 10

    SEE ALSO:
        query, create index, show index
    """
    collectionName = click.prompt(
        "Collection name", type=click.Choice(obj.collection.list_collections())
    )

    annsField = click.prompt(
        "The vector field used to search of collection",
        type=click.Choice(obj.collection.list_field_names(collectionName)),
    )

    fields_info = obj.collection.list_fields_info(collectionName)
    annsField_info = next(
        (field for field in fields_info if field["name"] == annsField), None
    )
    if not annsField_info:
        click.echo(f"Field {annsField} not found in collection {collectionName}.")
        return
    isFunctionOut = annsField_info["isFunctionOut"]
    data = None
    if isFunctionOut:
        text = click.prompt(
            "Enter the text to search",
        )
        data = [text]
    else:
        vector = click.prompt(
            "The vectors of search data (input as a list, e.g., [1,2,3]). The length should match your dimension.",
        )
        # format vector from string to list
        vector = [float(x) for x in vector.strip("[]").split(",")]
        data = [vector]

    indexes = obj.index.list_indexes(collectionName, onlyData=True)
    indexDetails = None
    for index in indexes:
        if index.field_name == annsField:
            indexDetails = index
            break

    hasIndex = not not indexDetails
    if indexDetails:
        index_type = indexDetails._index_params.get("index_type", "AUTOINDEX")
        metric_type = indexDetails._index_params.get("metric_type", "")
        click.echo(f"Metric type: {metric_type}")
        metricType = metric_type
        search_parameters = IndexTypesMap[index_type]["search_parameters"]
        params = []
        for parameter in search_parameters:
            paramInput = click.prompt(f"Search parameter {parameter}'s value")
            params += [f"{parameter}:{paramInput}"]
    else:
        metricType = ""
        params = []

    groupByField = click.prompt(
        "Groups search by Field",
        default="",
        type=str,
    )
    if groupByField != "":
        params += [f"group_by_field:{groupByField}"]
    roundDecimal = click.prompt(
        "The specified number of decimal places of returned distance",
        default=-1,
        type=int,
    )
    limit = click.prompt(
        "The max number of returned record, also known as topk", default=None, type=int
    )
    expr = click.prompt("The boolean expression used to filter attribute", default="")
    partitionNames = click.prompt(
        'The names of partitions to search (split by "," if multiple, press Enter to skip)',
        default="",
    )
    outputFields = click.prompt(
        f'Fields to return(split by "," if multiple) {obj.collection.list_field_names(collectionName)}',
        default="",
    )
    guarantee_timestamp = click.prompt(
        "Guarantee timestamp. This instructs Milvus to see all operations performed before a provided timestamp. If no such timestamp is provided, then Milvus will search all operations performed to date.",
        default=0,
        type=int,
    )

    try:
        searchParameters = validateSearchParams(
            data=data,
            annsField=annsField,
            metricType=metricType,
            params=params,
            limit=limit,
            expr=expr,
            outputFields=outputFields,
            roundDecimal=roundDecimal,
            hasIndex=hasIndex,
            guarantee_timestamp=guarantee_timestamp,
            partitionNames=partitionNames,
        )

    except ParameterException as pe:
        click.echo("Error!\n{}".format(str(pe)))

    else:
        results = obj.data.search(
            collectionName,
            searchParameters,
        )
        click.echo(f"Search result: \n")
        click.echo(results)
