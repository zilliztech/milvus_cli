from tabulate import tabulate
from .helper_cli import create, getList, delete, show
from .init_cli import cli
import click
import os
import sys

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
from Types import IndexTypes, MetricTypes, IndexTypesMap


@create.command("index")
@click.pass_obj
def createIndex(obj):
    """
    Create index.

    Example:

        milvus_cli > create index

        Collection name (car, car2): car2

        The name of the field to create an index for (vector): vector

        Index type (FLAT, IVF_FLAT, IVF_SQ8, IVF_PQ, RNSG, HNSW, ANNOY,AUTOINDEX): IVF_FLAT

        Index metric type (L2, IP, HAMMING, TANIMOTO, COSINE): L2

        Index params nlist: 2

        Timeout []:
    """
    try:
        collectionName = click.prompt(
            "Collection name", type=click.Choice(obj.collection.list_collections())
        )
        fieldName = click.prompt(
            "The name of the field to create an index for",
            type=click.Choice(obj.collection.list_field_names(collectionName)),
        )
        indexName = click.prompt("Index name")

        indexType = click.prompt(
            "Index type", type=click.Choice(IndexTypes), default=""
        )
        metricType = click.prompt(
            "Vector Index metric type", type=click.Choice(MetricTypes), default=""
        )
        params = []

        ignoreIndexType = ["", "AUTOINDEX"]
        if indexType not in ignoreIndexType:
            index_building_parameters = IndexTypesMap[indexType][
                "index_building_parameters"
            ]
            for param in index_building_parameters:
                tmpParam = click.prompt(f"Index params {param}")
                params.append(f"{param}:{tmpParam}")

    except Exception as e:
        click.echo("Error!\n{}".format(str(e)))

    else:
        click.echo(
            obj.index.create_index(
                collectionName,
                fieldName,
                indexName,
                indexType,
                metricType,
                params,
            )
        )
        click.echo("Create index successfully!")


@getList.command("indexes")
@click.option(
    "-c",
    "--collection",
    "collectionName",
    help="The collection name.",
    type=str,
)
@click.pass_obj
def list_indexes(obj, collectionName):
    """
    List all indexes.

    Example:

        milvus_cli > list indexes -c test_collection

    """
    try:
        click.echo(obj.index.list_indexes(collectionName))
    except Exception as e:
        click.echo(message=e, err=True)


@show.command("index")
@click.option(
    "-c",
    "--collection",
    "collectionName",
    help="The collection name.",
    type=str,
)
@click.option("-in", "--index-name", "indexName", help="Index name")
@click.pass_obj
def show_index_details(obj, collectionName, indexName):
    """
    Show index details.

    Example:

        milvus_cli > show index -c test_collection -in index_name

    """
    try:
        click.echo(obj.index.get_index_details(collectionName, indexName))
    except Exception as e:
        click.echo(message=e, err=True)


@delete.command("index")
@click.option(
    "-c",
    "--collection",
    "collectionName",
    help="The collection name.",
    type=str,
)
@click.option("-in", "--index-name", "indexName", help="Index name")
@click.pass_obj
def delete_index(obj, collectionName, indexName):
    """
    Delete index.

    Example:

        milvus_cli > delete index -c test_collection -in index_name

    """
    click.echo(
        "Warning!\nYou are trying to delete the index of collection. This action cannot be undone!\n"
    )
    if not click.confirm("Do you want to continue?"):
        return
    try:
        click.echo(obj.index.drop_index(collectionName, indexName))
    except Exception as e:
        click.echo(message=e, err=True)


@show.command("index_progress")
@click.option(
    "-c",
    "--collection",
    "collectionName",
    help="The collection name.",
    type=str,
)
@click.option("-in", "--index-name", "indexName", help="Index name")
@click.pass_obj
def show_index_progress(obj, collectionName, indexName):
    """
    Show index progress.

    Example:

        milvus_cli > show index_progress -c test_collection -in index_name

    """
    try:
        click.echo(obj.index.get_index_build_progress(collectionName, indexName))
    except Exception as e:
        click.echo(message=e, err=True)
