from .helper_cli import create, getList, delete, show, load, release
import click


@create.command("partition")
@click.option("-c", "--collection-name", "collectionName", help="Collection name.")
@click.option("-p", "--partition", "partitionName", help="The name of partition.")
@click.option(
    "-d",
    "--description",
    "description",
    help="[Optional] - Partition description.",
    default="",
)
@click.pass_obj
def create_partition(obj, collectionName, partitionName, description):
    """
    Create partition.

    Example:

        milvus_cli > create partition -c car -p new_partition -d test_add_partition
    """
    click.echo(
        obj.partition.create_partition(collectionName, description, partitionName)
    )
    click.echo("Create partition successfully!")


@show.command("partition")
@click.option("-c", "--collection-name", "collectionName", help="Collection name.")
@click.option("-p", "--partition", "partitionName", help="The name of partition.")
@click.pass_obj
def describe_partition(obj, collectionName, partitionName):
    """
    Describe partition.

    Example:

        milvus_cli > show partition -c car -p new_partition
    """
    click.echo(obj.partition.describe_partition(collectionName, partitionName))


@getList.command("partitions")
@click.option("-c", "--collection-name", "collectionName", help="Collection name.")
@click.pass_obj
def list_partitions(obj, collectionName):
    """
    List partitions.

    Example:

        milvus_cli > list partitions -c car
    """
    click.echo(obj.partition.list_partition_names(collectionName))


@delete.command("partition")
@click.option("-c", "--collection-name", "collectionName", help="Collection name.")
@click.option("-p", "--partition", "partitionName", help="The name of partition.")
@click.pass_obj
def delete_partition(obj, collectionName, partitionName):
    """
    Delete partition.

    Example:

        milvus_cli > delete partition -c car -p new_partition
    """
    click.echo(obj.partition.delete_partition(collectionName, partitionName))


@load.command("partition")
@click.option("-c", "--collection-name", "collectionName", help="Collection name.")
@click.option("-p", "--partition", "partitionName", help="The name of partition.")
@click.pass_obj
def load_partition(obj, collectionName, partitionName):
    """
    Load partition.

    Example:

        milvus_cli > load partition -c car -p new_partition
    """
    click.echo(obj.partition.load_partition(collectionName, partitionName))


@release.command("partition")
@click.option("-c", "--collection-name", "collectionName", help="Collection name.")
@click.option("-p", "--partition", "partitionName", help="The name of partition.")
@click.pass_obj
def release_partition(obj, collectionName, partitionName):
    """
    Release partition.

    Example:

        milvus_cli > release partition -c car -p new_partition
    """
    click.echo(obj.partition.release_partition(collectionName, partitionName))
