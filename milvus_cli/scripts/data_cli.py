from .helper_cli import delete, insert
from .init_cli import cli

import click
import os
import sys

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from Validation import validateQueryParams, validateSearchParams
from Types import ParameterException, IndexTypesMap
from Fs import readCsvFile
import json
from tabulate import tabulate


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
    Delete entities from collection.

    Example:

        milvus_cli > delete entities -c test_collection
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
    Query with a set of criteria, and results in a list of records that match the query exactly.

    Example:

        milvus_cli > query

        Collection name: car

        The query expression: color in [2000,2002]

        A list of fields to return(split by "," if multiple) []: id, color, brand

        timeout []:

        Guarantee timestamp. This instructs Milvus to see all operations performed before a provided timestamp. If no such timestamp is provided, then Milvus will search all operations performed to date. [0]:

        Graceful time. Only used in bounded consistency level. If graceful_time is set, PyMilvus will use current timestamp minus the graceful_time as the guarantee_timestamp. This option is 5s by default if not set. [5]:

    """
    collectionName = click.prompt(
        "Collection name", type=click.Choice(obj.collection.list_collections())
    )
    expr = click.prompt("The query expression")
    # partitionNames = click.prompt(
    #     f'The names of partitions to search (split by "," if multiple) {obj._list_partition_names(collectionName)}',
    #     default="",
    # )
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
        )
    except ParameterException as pe:
        click.echo("Error!\n{}".format(str(pe)))
    else:
        results = obj.data.query(collectionName, queryParameters)
        if results:
            headers = results[0].keys()
            rows = [result.values() for result in results]
            table = tabulate(rows, headers, tablefmt="grid")
            click.echo(table)
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
    help="[Optional] - The partition name which the data will be inserted to, if partition name is not passed, then the data will be inserted to “_default” partition.",
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
    Import data from csv file(local or remote) with headers and insert into target collection.

    Example-1:

        milvus_cli > insert -c car 'examples/import_csv/vectors.csv'

        Reading file from local path.

        Reading csv file...  [####################################]  100%

        Column names are ['vector', 'color', 'brand']

        Processed 50001 lines.

        Inserting ...

        Insert successfully.

        \b
    --------------------------  ------------------
    Total insert entities:                   50000
    Total collection entities:              150000
    Milvus timestamp:           428849214449254403
    --------------------------  ------------------

    Example-2:

        milvus_cli > import -c car 'https://raw.githubusercontent.com/zilliztech/milvus_cli/main/examples/import_csv/vectors.csv'

        Reading file from remote URL.

        Reading csv file...  [####################################]  100%

        Column names are ['vector', 'color', 'brand']

        Processed 50001 lines.

        Inserting ...

        Insert successfully.

        \b
    --------------------------  ------------------
    Total insert entities:                   50000
    Total collection entities:              150000
    Milvus timestamp:           428849214449254403
    --------------------------  ------------------
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
        fields = obj.collection.list_field_names_and_types(collectionName)
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
            elif fieldType in ["INT8", "INT16", "INT32", "INT64"]:
                value = int(value)
            elif fieldType in ["FLOAT", "DOUBLE"]:
                value = float(value)
            elif fieldType in ["BOOL"]:
                value = bool(value)
            elif fieldType in [
                "BINARY_VECTOR",
                "FLOAT_VECTOR",
                "FLOAT16_VECTOR",
                "BFLOAT16_VECTOR",
                "SPARSE_FLOAT_VECTOR",
                "ARRAY",  #  may not working, because of array support different element types
            ]:
                value = [float(x) for x in value.strip("[]").split(",")]
            elif fieldType in ["JSON"]:
                value = json.loads(value)

            data[field["name"]] = value
        result = obj.data.insert(collectionName, data, partitionName)
    except Exception as e:
        click.echo("Error!\n{}".format(str(e)))
    else:
        click.echo(f"\nInserted successfully.\n")
        click.echo(result)


@cli.command("search")
@click.pass_obj
def search(obj):
    """
    Conducts a vector similarity search with an optional boolean expression as filter.

    Example:
        milvus_cli > search
    """
    collectionName = click.prompt(
        "Collection name", type=click.Choice(obj.collection.list_collections())
    )

    vector = click.prompt(
        "The vectors of search data (input as a list, e.g., [1,2,3]). The length should match your dimension.",
    )
    # format vector from string to list
    vector = [float(x) for x in vector.strip("[]").split(",")]
    data = [vector]

    annsField = click.prompt(
        "The vector field used to search of collection",
        type=click.Choice(obj.collection.list_field_names(collectionName)),
    )
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
