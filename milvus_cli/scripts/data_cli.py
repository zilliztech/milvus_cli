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
        click.echo(obj.data.query(collectionName, queryParameters))


@cli.command("insert")
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


@cli.command("search")
@click.pass_obj
def search(obj):
    """
    Conducts a vector similarity search with an optional boolean expression as filter.

    Example-1(import a CSV file):

        Collection name (car, test_collection): car

        The vectors of search data (the length of data is number of query (nq),
        the dim of every vector in data must be equal to vector field’s of
        collection. You can also import a CSV file without headers): examples/import_csv/search_vectors.csv

        The vector field used to search of collection (vector): vector

        Metric type: L2

        Search parameter nprobe's value: 10

        The max number of returned record, also known as topk: 2

        The boolean expression used to filter attribute []: id > 0

    Example-2(collection has index):

        Collection name (car, test_collection): car

        \b
        The vectors of search data (the length of data is number of query (nq),
        the dim of every vector in data must be equal to vector field’s of
        collection. You can also import a CSV file without headers):
            [[0.71, 0.76, 0.17, 0.13, 0.42, 0.07, 0.15, 0.67, 0.58, 0.02, 0.39,
            0.47, 0.58, 0.88, 0.73, 0.31, 0.23, 0.57, 0.33, 0.2, 0.03, 0.43,
            0.78, 0.49, 0.17, 0.56, 0.76, 0.54, 0.45, 0.46, 0.05, 0.1, 0.43,
            0.63, 0.29, 0.44, 0.65, 0.01, 0.35, 0.46, 0.66, 0.7, 0.88, 0.07,
            0.49, 0.92, 0.57, 0.5, 0.16, 0.77, 0.98, 0.1, 0.44, 0.88, 0.82,
            0.16, 0.67, 0.63, 0.57, 0.55, 0.95, 0.13, 0.64, 0.43, 0.71, 0.81,
            0.43, 0.65, 0.76, 0.7, 0.05, 0.24, 0.03, 0.9, 0.46, 0.28, 0.92,
            0.25, 0.97, 0.79, 0.73, 0.97, 0.49, 0.28, 0.64, 0.19, 0.23, 0.51,
            0.09, 0.1, 0.53, 0.03, 0.23, 0.94, 0.87, 0.14, 0.42, 0.82, 0.91,
            0.11, 0.91, 0.37, 0.26, 0.6, 0.89, 0.6, 0.32, 0.11, 0.98, 0.67,
            0.12, 0.66, 0.47, 0.02, 0.15, 0.6, 0.64, 0.57, 0.14, 0.81, 0.75,
            0.11, 0.49, 0.78, 0.16, 0.63, 0.57, 0.18]]

        The vector field used to search of collection (vector): vector

        Metric type: L2

        Search parameter nprobe's value: 10

        The specified number of decimal places of returned distance [-1]: 5

        The max number of returned record, also known as topk: 2

        The boolean expression used to filter attribute []: id > 0

        timeout []:

    """
    collectionName = click.prompt(
        "Collection name", type=click.Choice(obj.collection.list_collections())
    )
    data = click.prompt(
        "The vectors of search data (the length of data is number of query (nq), the dim of every vector in data must be equal to vector field’s of collection. You can also import a CSV file without headers)"
    )
    annsField = click.prompt(
        "The vector field used to search of collection",
        type=click.Choice(obj.collection.list_field_names(collectionName)),
    )
    indexDetails = obj.index.get_vector_index(collectionName)
    hasIndex = not not indexDetails
    if indexDetails:
        index_type = indexDetails["index_type"]
        search_parameters = IndexTypesMap[index_type]["search_parameters"]
        metric_type = indexDetails["metric_type"]
        click.echo(f"Metric type: {metric_type}")
        metricType = metric_type
        params = []
        for parameter in search_parameters:
            paramInput = click.prompt(f"Search parameter {parameter}'s value")
            params += [f"{parameter}:{paramInput}"]
    else:
        metricType = ""
        params = []
    roundDecimal = click.prompt(
        "The specified number of decimal places of returned distance",
        default=-1,
        type=int,
    )
    limit = click.prompt(
        "The max number of returned record, also known as topk", default=None, type=int
    )
    expr = click.prompt("The boolean expression used to filter attribute", default="")
    # partitionNames = click.prompt(
    #     f'The names of partitions to search (split by "," if multiple) {obj._list_partition_names(collectionName)}',
    #     default="",
    # )
    timeout = click.prompt("Timeout", default="")
    guarantee_timestamp = click.prompt(
        "Guarantee Timestamp(It instructs Milvus to see all operations performed before a provided timestamp. If no such timestamp is provided, then Milvus will search all operations performed to date)",
        default=0,
        type=int,
    )

    export, exportPath = False, ""
    # if click.confirm('Would you like to export results as a CSV file?'):
    #     export = True
    #     exportPath = click.prompt('Directory path to csv file')
    # export = click.prompt('Would you like to export results as a CSV file?', default='n', type=click.Choice(['Y', 'n']))
    # if export:
    #     exportPath = click.prompt('Directory path to csv file')
    try:
        searchParameters = validateSearchParams(
            data,
            annsField,
            metricType,
            params,
            limit,
            expr,
            timeout,
            roundDecimal,
            hasIndex=hasIndex,
            guarantee_timestamp=guarantee_timestamp,
        )
    except ParameterException as pe:
        click.echo("Error!\n{}".format(str(pe)))

    else:
        if export:
            results = obj.data.search(
                collectionName, searchParameters, prettierFormat=False
            )
        else:
            results = obj.data.search(collectionName, searchParameters)
            click.echo(f"Search results:\n")
            click.echo(results)
            # click.echo(obj.search(collectionName, searchParameters))
