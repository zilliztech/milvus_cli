from tabulate import tabulate
import sys
import os
import click

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from utils import PyOrm, Completer, getPackageVersion, WELCOME_MSG, EXIT_MSG
from Fs import readCsvFile
from Validation import (
    validateParamsByCustomFunc,
    validateCollectionParameter,
    validateIndexParameter,
    validateSearchParams,
    validateQueryParams,
    validateCalcParams,
)
from Types import ParameterException, ConnectException
from Types import MetricTypes, IndexTypesMap, IndexTypes


pass_context = click.make_pass_decorator(PyOrm, ensure=True)


@click.group(no_args_is_help=False, add_help_option=False, invoke_without_command=True)
@click.pass_context
def cli(ctx):
    """Milvus_CLI"""
    ctx.obj = PyOrm()


def print_help_msg(command):
    with click.Context(command) as ctx:
        click.echo(command.get_help(ctx))


@cli.command()
def help():
    """Show help messages."""
    click.echo(print_help_msg(cli))


@cli.command(no_args_is_help=False)
@click.option(
    "-a",
    "--alias",
    "alias",
    help="[Optional] - Milvus link alias name, default is `default`.",
    default="default",
    type=str,
)
@click.option(
    "-h",
    "--host",
    "host",
    help="[Optional] - Host name, default is `127.0.0.1`.",
    default="127.0.0.1",
    type=str,
)
@click.option(
    "-p",
    "--port",
    "port",
    help="[Optional] - Port, default is `19530`.",
    default=19530,
    type=int,
)
@click.option(
    "-s",
    "--secure",
    "secure",
    help="[Optional] - Secure, default is `False`.",
    default=False,
    type=bool,
)
@click.option(
    "-u",
    "--username",
    "username",
    help="[Optional] - Username , default is `None`.",
    default=None,
    type=str,
)
@click.option(
    "-pwd",
    "--password",
    "password",
    help="[Optional] - Password , default is `None`.",
    default=None,
    type=str,
)
@click.option(
    "-D",
    "--disconnect",
    "disconnect",
    help="[Optional, Flag] - Disconnect from a Milvus server by alias, default is `default`.",
    default=False,
    is_flag=True,
)
@click.pass_obj
def connect(obj, alias, host, port,secure,username,password, disconnect):
    """
    Connect to Milvus.

    Example:

        milvus_cli > connect -h 127.0.0.1 -p 19530 -a default
    """
    try:
        obj.connect(alias, host, port, disconnect,secure,username,password)
    except Exception as e:
        click.echo(message=e, err=True)
    else:
        if disconnect:
            click.echo("Disconnected.")
        else:
            click.echo("Connect Milvus successfully.")
            click.echo(obj.showConnection(alias))


@cli.command()
def version():
    """Get Milvus_CLI version."""
    click.echo(f"Milvus_CLI v{getPackageVersion()}")


@cli.command()
def clear():
    """Clear screen."""
    click.clear()


@cli.group(no_args_is_help=False)
@click.pass_obj
def show(obj):
    """Show connection, loading_progress and index_progress."""
    pass


@show.command()
@click.option(
    "-a",
    "--all",
    "showAll",
    help="[Optional, Flag] - Show all connections.",
    default=False,
    is_flag=True,
)
@click.pass_obj
def connection(obj, showAll):
    """Show current/all connection details"""
    try:
        (not showAll) and obj.checkConnection()
    except Exception as e:
        click.echo("No connections.")
    else:
        click.echo(obj.showConnection(showAll=showAll))


@show.command("loading_progress")
@click.option(
    "-c", "--collection-name", "collection", help="The name of collection is loading"
)
@click.option(
    "-p",
    "--partition",
    "partition",
    help="[Optional, Multiple] - The names of partitions are loading",
    default=None,
    multiple=True,
)
@click.pass_obj
def loadingProgress(obj, collection, partition):
    """Show #loaded entities vs #total entities."""
    try:
        obj.checkConnection()
        validateParamsByCustomFunc(
            obj.getTargetCollection, "Collection Name Error!", collection
        )
        result = obj.showCollectionLoadingProgress(collection, partition)
    except Exception as e:
        click.echo(message=e, err=True)
    else:
        click.echo(
            tabulate(
                [[result.get("num_loaded_entities"), result.get("num_total_entities")]],
                headers=["num_loaded_entities", "num_total_entities"],
                tablefmt="pretty",
            )
        )


@show.command("index_progress")
@click.option(
    "-c", "--collection-name", "collection", help="The name of collection is loading"
)
# ! TODO: To be removed
@click.option("-i", "--index", "index", help="[Optional] - Index name.", default="")
@click.pass_obj
def indexProgress(obj, collection, index):
    """Show # indexed entities vs. # total entities."""
    try:
        obj.checkConnection()
        validateParamsByCustomFunc(
            obj.getTargetCollection, "Collection Name Error!", collection
        )
        result = obj.showIndexBuildingProgress(collection, index)
    except Exception as e:
        click.echo(message=e, err=True)
    else:
        click.echo(
            tabulate(
                [[result.get("indexed_rows"), result.get("total_rows")]],
                headers=["indexed_rows", "total_rows"],
                tablefmt="pretty",
            )
        )


@show.command("query_segment")
@click.option(
    "-c",
    "--collection-name",
    "collection",
    help="A string representing the collection to get segments info.",
)
@click.option(
    "-t",
    "--timeout",
    "timeout",
    help="[Optional] - An optional duration of time in seconds to allow for the RPC. When timeout is not set, client waits until server response or error occur.",
    default=None,
    type=float,
)
@click.pass_obj
def querySegmentInfo(obj, collection, timeout):
    """Return segments information from query nodes."""
    click.echo(obj.getQuerySegmentInfo(collection, timeout, prettierFormat=True))


@show.command("compaction_state")
@click.option(
    "-c",
    "--collection-name",
    "collection",
    help="Collection name.",
)
@click.option(
    "-t",
    "--timeout",
    "timeout",
    help="[Optional] - An optional duration of time in seconds to allow for the RPC. When timeout is not set, client waits until server response or error occur.",
    default=None,
    type=float,
)
@click.pass_obj
def compactionStateInfo(obj, collection, timeout):
    """Return the current collection's compaction state."""
    click.echo(obj.getCompactCollectionState(collection, timeout))


@show.command("compaction_plans")
@click.option(
    "-c",
    "--collection-name",
    "collection",
    help="Collection name.",
)
@click.option(
    "-t",
    "--timeout",
    "timeout",
    help="[Optional] - An optional duration of time in seconds to allow for the RPC. When timeout is not set, client waits until server response or error occur.",
    default=None,
    type=float,
)
@click.pass_obj
def compactionStateInfo(obj, collection, timeout):
    """Return the current collection's compaction state."""
    click.echo(obj.getCompactCollectionPlans(collection, timeout))


@cli.command()
@click.option(
    "-c", "--collection-name", "collection", help="The name of collection to load."
)
@click.option(
    "-p",
    "--partition",
    "partition",
    help="[Optional, Multiple] - The name of partition to load.",
    default=[],
    multiple=True,
)
@click.pass_obj
def load(obj, collection, partition):
    """Load specified collection/partitions from disk to memory."""
    try:
        validateParamsByCustomFunc(
            obj.getTargetCollection, "Collection Name Error!", collection
        )
        for partitionName in partition:
            validateParamsByCustomFunc(
                obj.getTargetPartition,
                "Partition Name Error!",
                collection,
                partitionName,
            )
        if partition:
            result = obj.loadPartitions(collection, partition)
        else:
            result = obj.loadCollection(collection)
    except Exception as e:
        click.echo(message=e, err=True)
    else:
        if partition:
            click.echo(f"""Load {collection}'s partitions {partition} successfully""")
        else:
            click.echo(f"""Load Collection {collection} successfully""")
        click.echo(result)


@cli.command()
@click.option(
    "-c",
    "--collection-name",
    "collection",
    help="The name of collection to be released.",
)
@click.option(
    "-p",
    "--partition",
    "partition",
    help="[Optional, Multiple] - The name of partition to released.",
    default=[],
    multiple=True,
)
@click.pass_obj
def release(obj, collection, partition):
    """Release specified collection/partitions from memory."""
    try:
        validateParamsByCustomFunc(
            obj.getTargetCollection, "Collection Name Error!", collection
        )
        for partitionName in partition:
            validateParamsByCustomFunc(
                obj.getTargetPartition,
                "Partition Name Error!",
                collection,
                partitionName,
            )
        if partition:
            result = obj.releasePartitions(collection, partition)
        else:
            result = obj.releaseCollection(collection)
    except Exception as e:
        click.echo(message=e, err=True)
    else:
        if partition:
            click.echo(
                f"""Release {collection}'s partitions {partition} successfully"""
            )
        else:
            click.echo(f"""Release Collection {collection} successfully""")
        click.echo(result)


@cli.group("list", no_args_is_help=False)
@click.pass_obj
def listDetails(obj):
    """List collections, partitions and indexes."""
    pass


# @listDetails.command("alias")
# @click.option("-c", "--collection-name", "collection", help="The name of collection.")
# @click.option(
#     "--timeout",
#     "-t",
#     "timeout",
#     help="[Optional] - An optional duration of time in seconds to allow for the RPC. When timeout is not set, client waits until server response or error occur.",
#     default=None,
#     type=float,
# )
# @click.pass_obj
# def listCollectionAlias(obj, collection, timeout):
#     """List all alias of the collection."""
#     try:
#         obj.checkConnection()
#         click.echo(obj.listCollectionAlias(collection, timeout))
#     except Exception as e:
#         click.echo(message=e, err=True)


@listDetails.command()
@click.option(
    "--timeout",
    "-t",
    "timeout",
    help="[Optional] - An optional duration of time in seconds to allow for the RPC. When timeout is not set, client waits until server response or error occur.",
    default=None,
    type=float,
)
@click.pass_obj
def collections(obj, timeout):
    """List all collections."""
    try:
        obj.checkConnection()
        click.echo(obj.listCollections(timeout))
    except Exception as e:
        click.echo(message=e, err=True)


@listDetails.command()
@click.option("-c", "--collection-name", "collection", help="The name of collection.")
@click.pass_obj
def partitions(obj, collection):
    """List all partitions of the specified collection."""
    try:
        obj.checkConnection()
        validateParamsByCustomFunc(
            obj.getTargetCollection, "Collection Name Error!", collection
        )
        click.echo(obj.listPartitions(collection))
    except Exception as e:
        click.echo(message=e, err=True)


@listDetails.command()
@click.option("-c", "--collection-name", "collection", help="The name of collection.")
@click.pass_obj
def indexes(obj, collection):
    """List all indexes of the specified collection."""
    try:
        obj.checkConnection()
        validateParamsByCustomFunc(
            obj.getTargetCollection, "Collection Name Error!", collection
        )
        click.echo(obj.listIndexes(collection))
    except Exception as e:
        click.echo(message=e, err=True)

@listDetails.command()
@click.pass_obj
def users(obj):
    """List all users in Milvus"""
    try:
        obj.checkConnection()
        click.echo(obj.listCredUsers())
    except Exception as e:
        click.echo(message=e, err=True)


@cli.group("describe", no_args_is_help=False)
@click.pass_obj
def describeDetails(obj):
    """Describe collection, partition and index."""
    pass


@describeDetails.command("collection")
@click.option("-c", "--collection-name", "collection", help="The name of collection.")
@click.pass_obj
def describeCollection(obj, collection):
    """
    Describe collection.

    Example:

        milvus_cli > describe collection -c test_collection_insert
    """
    try:
        obj.checkConnection()
        click.echo(obj.getCollectionDetails(collection))
    except Exception as e:
        click.echo(message=e, err=True)


@describeDetails.command("partition")
@click.option(
    "-c", "--collection-name", "collectionName", help="The name of collection."
)
@click.option(
    "-p",
    "--partition",
    "partition",
    help='[Optional] - The name of partition, default is "_default".',
    default="_default",
)
@click.pass_obj
def describePartition(obj, collectionName, partition):
    """
    Describe partition.

    Example:

        milvus_cli > describe partition -c test_collection_insert -p _default
    """
    try:
        obj.checkConnection()
        collection = obj.getTargetCollection(collectionName)
    except Exception as e:
        click.echo(f"Error when getting collection by name!\n{str(e)}")
    else:
        click.echo(obj.getPartitionDetails(collection, partition))


@describeDetails.command("index")
@click.option(
    "-c", "--collection-name", "collectionName", help="The name of collection."
)
@click.pass_obj
def describeIndex(obj, collectionName):
    """
    Describe index.

    Example:

        milvus_cli > describe index -c car
    """
    try:
        obj.checkConnection()
        collection = obj.getTargetCollection(collectionName)
    except Exception as e:
        click.echo(f"Error when getting collection by name!\n{str(e)}")
    else:
        click.echo(obj.getIndexDetails(collection))


@cli.group("create", no_args_is_help=False)
@click.pass_obj
def createDetails(obj):
    """Create collection, partition and index."""
    pass


@createDetails.command("alias")
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
    "aliasNames",
    help="[Multiple] - The alias of the collection.",
    type=str,
    multiple=True,
)
@click.option(
    "-A",
    "--alter",
    "alter",
    help="[Optional, Flag] - Change an existing alias to current collection.",
    default=False,
    is_flag=True,
)
@click.option(
    "-t",
    "--timeout",
    "timeout",
    help="[Optional] - An optional duration of time in seconds to allow for the RPC. If timeout is not set, the client keeps waiting until the server responds or an error occurs.",
    default=None,
    type=float,
)
@click.pass_obj
def createAlias(obj, collectionName, aliasNames, alter, timeout):
    """
    Specify alias for a collection.
    Alias cannot be duplicated, you can't assign same alias to different collections.
    But you can specify multiple aliases for a collection, for example:

    create alias -c car -a carAlias1 -a carAlias2

    You can also change alias of a collection to another collection.
    If the alias doesn't exist, it will return error.
    Use "-A" option to change alias owner collection, for example:

    create alias -c car2 -A -a carAlias1 -a carAlias2
    """
    try:
        obj.checkConnection()
        if alter:
            result = obj.alterCollectionAliasList(collectionName, aliasNames, timeout)
        else:
            result = obj.createCollectionAliasList(collectionName, aliasNames, timeout)
    except ConnectException as ce:
        click.echo("Error!\n{}".format(str(ce)))
    else:
        if len(result) == len(aliasNames):
            click.echo(
                f"""{len(result)} alias {"altered" if alter else "created"} successfully."""
            )


@createDetails.command("collection")
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
    "-a",
    "--schema-auto-id",
    "autoId",
    help="[Optional, Flag] - Enable auto id.",
    default=False,
    is_flag=True,
)
@click.option(
    "-d",
    "--schema-description",
    "description",
    help="[Optional] - Description details.",
    default="",
)
@click.option(
    "-f",
    "--schema-field",
    "fields",
    help='[Multiple] - FieldSchema. Usage is "<Name>:<DataType>:<Dim(if vector) or Description>"',
    default=None,
    multiple=True,
)
@click.pass_obj
def createCollection(obj, collectionName, primaryField, autoId, description, fields):
    """
    Create collection.

    Example:

      create collection -c car -f id:INT64:primary_field -f vector:FLOAT_VECTOR:128 -f color:INT64:color -f brand:INT64:brand -p id -a -d 'car_collection'
    """
    try:
        obj.checkConnection()
        validateCollectionParameter(collectionName, primaryField, fields)
    except ParameterException as pe:
        click.echo("Error!\n{}".format(str(pe)))
    except ConnectException as ce:
        click.echo("Error!\n{}".format(str(ce)))
    else:
        click.echo(
            obj.createCollection(
                collectionName, primaryField, autoId, description, fields
            )
        )
        click.echo("Create collection successfully!")


@createDetails.command("partition")
@click.option("-c", "--collection-name", "collectionName", help="Collection name.")
@click.option("-p", "--partition", "partition", help="The name of partition.")
@click.option(
    "-d",
    "--description",
    "description",
    help="[Optional] - Partition description.",
    default="",
)
@click.pass_obj
def createPartition(obj, collectionName, partition, description):
    """
    Create partition.

    Example:

        milvus_cli > create partition -c car -p new_partition -d test_add_partition
    """
    try:
        obj.checkConnection()
        obj.getTargetCollection(collectionName)
    except Exception as e:
        click.echo(f"Error occurred when get collection by name!\n{str(e)}")
    else:
        click.echo(obj.createPartition(collectionName, description, partition))
        click.echo("Create partition successfully!")


@createDetails.command("index")
@click.pass_obj
def createIndex(obj):
    """
    Create index.

    Example:

        milvus_cli > create index

        Collection name (car, car2): car2

        The name of the field to create an index for (vector): vector

        Index type (FLAT, IVF_FLAT, IVF_SQ8, IVF_PQ, RNSG, HNSW, ANNOY): IVF_FLAT

        Index metric type (L2, IP, HAMMING, TANIMOTO): L2

        Index params nlist: 2

        Timeout []:
    """
    try:
        obj.checkConnection()
        collectionName = click.prompt(
            "Collection name", type=click.Choice(obj._list_collection_names())
        )
        fieldName = click.prompt(
            "The name of the field to create an index for",
            type=click.Choice(
                obj._list_field_names(collectionName, showVectorOnly=True)
            ),
        )
        indexType = click.prompt("Index type", type=click.Choice(IndexTypes))
        metricType = click.prompt("Index metric type", type=click.Choice(MetricTypes))
        index_building_parameters = IndexTypesMap[indexType][
            "index_building_parameters"
        ]
        params = []
        for param in index_building_parameters:
            tmpParam = click.prompt(f"Index params {param}")
            params.append(f"{param}:{tmpParam}")
        inputTimeout = click.prompt("Timeout", default="")
        timeout = inputTimeout if inputTimeout else None
        validateIndexParameter(indexType, metricType, params)
    except ParameterException as pe:
        click.echo("Error!\n{}".format(str(pe)))
    except ConnectException as ce:
        click.echo("Error!\n{}".format(str(ce)))
    else:
        click.echo(
            obj.createIndex(
                collectionName, fieldName, indexType, metricType, params, timeout
            )
        )
        click.echo("Create index successfully!")


@createDetails.command("user")
@click.option("-u", "--username", "username", help="The username of milvus user.")
@click.option("-p", "--password", "password", help="The pawssord of milvus user.")
@click.pass_obj
def createUser(obj, username, password):
    """
    Create user.

    Example:

        milvus_cli > create user -u zilliz -p zilliz
    """
    try:
        obj.checkConnection()
        click.echo(obj.createCredUser(username,password))
        click.echo("Create user successfully")
    except Exception as e:
        click.echo(message=e, err=True)
    


@cli.group("delete", no_args_is_help=False)
@click.pass_obj
def deleteObject(obj):
    """Delete specified collection, partition and index."""
    pass


@deleteObject.command("alias")
@click.option(
    "-a", "--alias-name", "aliasName", help="The alias of the collection.", type=str
)
@click.option(
    "-t",
    "--timeout",
    "timeout",
    help="[Optional] - An optional duration of time in seconds to allow for the RPC. If timeout is not set, the client keeps waiting until the server responds or an error occurs.",
    default=None,
    type=float,
)
@click.pass_obj
def deleteAlias(obj, aliasName, timeout):
    """
    Delete an alias.
    """
    click.echo(
        "Warning!\nYou are trying to delete an alias. This action cannot be undone!\n"
    )
    if not click.confirm("Do you want to continue?"):
        return
    try:
        obj.checkConnection()
        obj.dropCollectionAlias(aliasName, timeout)
    except ConnectException as ce:
        click.echo("Error!\n{}".format(str(ce)))
    else:
        click.echo(f"Drop alias '{aliasName}' successfully.")


@deleteObject.command("collection")
@click.option(
    "-c",
    "--collection-name",
    "collectionName",
    help="The name of collection to be deleted.",
)
@click.option(
    "-t",
    "--timeout",
    "timeout",
    help="[Optional] - An optional duration of time in seconds to allow for the RPC. If timeout is not set, the client keeps waiting until the server responds or an error occurs.",
    default=None,
    type=float,
)
@click.pass_obj
def deleteCollection(obj, collectionName, timeout):
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
        obj.checkConnection()
        obj.getTargetCollection(collectionName)
    except Exception as e:
        click.echo(f"Error occurred when get collection by name!\n{str(e)}")
    else:
        result = obj.dropCollection(collectionName, timeout)
        click.echo("Drop collection successfully!") if not result else click.echo(
            "Drop collection failed!"
        )


@deleteObject.command("partition")
@click.option("-c", "--collection-name", "collectionName", help="Collection name")
@click.option("-p", "--partition", "partition", help="The name of partition.")
@click.option(
    "-t",
    "--timeout",
    "timeout",
    help="[Optional] - An optional duration of time in seconds to allow for the RPC. If timeout is not set, the client keeps waiting until the server responds or an error occurs.",
    default=None,
    type=float,
)
@click.pass_obj
def deletePartition(obj, collectionName, partition, timeout):
    """
    Drop the partition and its corresponding index files.

    Example:

        milvus_cli > delete partition -c car -p new_partition
    """
    click.echo(
        "Warning!\nYou are trying to delete the partition with data. This action cannot be undone!\n"
    )
    if not click.confirm("Do you want to continue?"):
        return
    try:
        obj.checkConnection()
        obj.getTargetCollection(collectionName)
    except Exception as e:
        click.echo(f"Error occurred when get collection by name!\n{str(e)}")
    else:
        result = obj.dropPartition(collectionName, partition, timeout)
        click.echo("Drop partition successfully!") if not result else click.echo(
            "Drop partition failed!"
        )


@deleteObject.command("index")
@click.option("-c", "--collection-name", "collectionName", help="Collection name")
@click.option(
    "-t",
    "--timeout",
    "timeout",
    help="[Optional] - An optional duration of time in seconds to allow for the RPC. If timeout is not set, the client keeps waiting until the server responds or an error occurs.",
    default=None,
    type=float,
)
@click.pass_obj
def deleteIndex(obj, collectionName, timeout):
    """
    Drop index and its corresponding index files.

    Example:

        milvus_cli > delete index -c car
    """
    click.echo(
        "Warning!\nYou are trying to delete the index of collection. This action cannot be undone!\n"
    )
    if not click.confirm("Do you want to continue?"):
        return
    try:
        obj.checkConnection()
        obj.getTargetCollection(collectionName)
    except Exception as e:
        click.echo(f"Error occurred when get collection by name!\n{str(e)}")
    else:
        result = obj.dropIndex(collectionName, timeout)
        click.echo("Drop index successfully!") if not result else click.echo(
            "Drop index failed!"
        )


@deleteObject.command("user")
@click.option("-u", "--username", "username", help="The username of milvus user")
@click.pass_obj
def deleteUser(obj, username):
    """
    Drop user in milvus by username

    Example:

        milvus_cli > delete user -u zilliz
    """
    click.echo(
        "Warning!\nYou are trying to delete the user in milvus. This action cannot be undone!\n"
    )
    if not click.confirm("Do you want to continue?"):
        return
    try:
        obj.checkConnection()
        result = obj.deleteCredUser(username)
        click.echo("Drop user successfully!")
        click.echo(result)
    except Exception as e:
        click.echo(message=e, err=True)
   

@deleteObject.command("entities")
@click.option("-c", "--collection-name", "collectionName", help="Collection name.")
@click.option(
    "-p",
    "--partition",
    "partitionName",
    help="[Optional] - Name of partitions that contain entities.",
    default="",
)
@click.option(
    "-t",
    "--timeout",
    "timeout",
    help="[Optional] - An optional duration of time in seconds to allow for the RPC. If timeout is not set, the client keeps waiting until the server responds or an error occurs.",
    default=None,
    type=float,
)
@click.pass_obj
def deleteEntities(obj, collectionName, partitionName, timeout):
    """
    Delete entities with an expression condition. And return results to show which primary key is deleted successfully.

    Example:

        milvus_cli > delete entities -c car

        The expression to specify entities to be deleted, such as "film_id in [ 0, 1 ]": film_id in [ 0, 1 ]

        You are trying to delete the entities of collection. This action cannot be undone!

        Do you want to continue? [y/N]: y
    """
    expr = click.prompt(
        '''The expression to specify entities to be deleted, such as "film_id in [ 0, 1 ]"'''
    )
    click.echo(
        "You are trying to delete the entities of collection. This action cannot be undone!\n"
    )
    if not click.confirm("Do you want to continue?"):
        return
    try:
        obj.checkConnection()
        obj.getTargetCollection(collectionName)
    except Exception as e:
        click.echo(f"Error occurred when get collection by name!\n{str(e)}")
    else:
        partitionValue = partitionName if partitionName else None
        timeoutValue = timeout if timeout else None
        result = obj.deleteEntities(expr, collectionName, partitionValue, timeoutValue)
        click.echo(result)


@cli.command()
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

        The names of partitions to search (split by "," if multiple) ['_default'] []: _default

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

        The names of partitions to search (split by "," if multiple) ['_default'] []: _default

        timeout []:

    Example-3(collection has no index):

        Collection name (car, car2): car

        The vectors of search data( the length of data is number of query (nq),
        the dim of every vector in data must be equal to vector field’s of
        collection. You can also import a CSV file without headers): examples/import_csv/search_vectors.csv

        The vector field used to search of collection (vector): vector

        The specified number of decimal places of returned distance [-1]: 5

        The max number of returned record, also known as topk: 2

        The boolean expression used to filter attribute []:

        The names of partitions to search (split by "," if multiple) ['_default'] []:

        Timeout []:
    """
    collectionName = click.prompt(
        "Collection name", type=click.Choice(obj._list_collection_names())
    )
    data = click.prompt(
        "The vectors of search data (the length of data is number of query (nq), the dim of every vector in data must be equal to vector field’s of collection. You can also import a CSV file without headers)"
    )
    annsField = click.prompt(
        "The vector field used to search of collection",
        type=click.Choice(obj._list_field_names(collectionName, showVectorOnly=True)),
    )
    indexDetails = obj._list_index(collectionName)
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
    partitionNames = click.prompt(
        f'The names of partitions to search (split by "," if multiple) {obj._list_partition_names(collectionName)}',
        default="",
    )
    timeout = click.prompt("Timeout", default="")
    guarantee_timestamp = click.prompt(
        "Guarantee Timestamp(It instructs Milvus to see all operations performed before a provided timestamp. If no such timestamp is provided, then Milvus will search all operations performed to date)",
        default=0,
        type=int,
    )
    travel_timestamp = click.prompt(
        "Travel Timestamp(Specify a timestamp in a search to get results based on a data view)",
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
            partitionNames,
            timeout,
            roundDecimal,
            hasIndex=hasIndex,
            guarantee_timestamp=guarantee_timestamp,
            travel_timestamp=travel_timestamp,
        )
        obj.checkConnection()
    except ParameterException as pe:
        click.echo("Error!\n{}".format(str(pe)))
    except ConnectException as ce:
        click.echo("Error!\n{}".format(str(ce)))
    else:
        if export:
            results = obj.search(collectionName, searchParameters, prettierFormat=False)
        else:
            results = obj.search(collectionName, searchParameters)
            click.echo(f"Search results:\n")
            for idx, item in enumerate(results):
                click.echo(f"No.{idx+1}:\n{item}\n")
            # click.echo(obj.search(collectionName, searchParameters))


@cli.command()
@click.pass_obj
def query(obj):
    """
    Query with a set of criteria, and results in a list of records that match the query exactly.

    Example 1:

        milvus_cli > query

        Collection name: car

        The query expression: id in [ 428960801420883491, 428960801420883492, 428960801420883493 ]

        Name of partitions that contain entities(split by "," if multiple) []: default

        A list of fields to return(split by "," if multiple) []: color, brand

        timeout []:

        Guarantee timestamp. This instructs Milvus to see all operations performed before a provided timestamp. If no such timestamp is provided, then Milvus will search all operations performed to date. [0]:

        Graceful time. Only used in bounded consistency level. If graceful_time is set, PyMilvus will use current timestamp minus the graceful_time as the guarantee_timestamp. This option is 5s by default if not set. [5]:

        Travel timestamp. Users can specify a timestamp in a search to get results based on a data view at a specified point in time. [0]: 428960801420883491

    Example 2:

        milvus_cli > query

        Collection name: car

        The query expression: id > 428960801420883491

        Name of partitions that contain entities(split by "," if multiple) []: default

        A list of fields to return(split by "," if multiple) []: id, color, brand

        timeout []:

        Guarantee timestamp. This instructs Milvus to see all operations performed before a provided timestamp. If no such timestamp is provided, then Milvus will search all operations performed to date. [0]:

        Graceful time. Only used in bounded consistency level. If graceful_time is set, PyMilvus will use current timestamp minus the graceful_time as the guarantee_timestamp. This option is 5s by default if not set. [5]:

        Travel timestamp. Users can specify a timestamp in a search to get results based on a data view at a specified point in time. [0]: 428960801420883491
    """
    collectionName = click.prompt(
        "Collection name", type=click.Choice(obj._list_collection_names())
    )
    expr = click.prompt("The query expression")
    partitionNames = click.prompt(
        f'The names of partitions to search (split by "," if multiple) {obj._list_partition_names(collectionName)}',
        default="",
    )
    outputFields = click.prompt(
        f'Fields to return(split by "," if multiple) {obj._list_field_names(collectionName)}',
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
    travel_timestamp = click.prompt(
        "Travel timestamp. Users can specify a timestamp in a search to get results based on a data view at a specified point in time.",
        default=0,
        type=int,
    )
    try:
        queryParameters = validateQueryParams(
            expr,
            partitionNames,
            outputFields,
            timeout,
            guarantee_timestamp,
            graceful_time,
            travel_timestamp,
        )
        obj.checkConnection()
    except ParameterException as pe:
        click.echo("Error!\n{}".format(str(pe)))
    except ConnectException as ce:
        click.echo("Error!\n{}".format(str(ce)))
    else:
        click.echo(obj.query(collectionName, queryParameters))


@cli.command("import")
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
def importData(obj, collectionName, partitionName, timeout, path):
    """
    Import data from csv file(local or remote) with headers and insert into target collection.

    Example-1:

        milvus_cli > import -c car 'examples/import_csv/vectors.csv'

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
        obj.checkConnection()
        validateParamsByCustomFunc(
            obj.getTargetCollection, "Collection Name Error!", collectionName
        )
        result = readCsvFile(path.replace('"', "").replace("'", ""))
        data = result["data"]
        result = obj.importData(collectionName, data, partitionName, timeout)
    except Exception as e:
        click.echo("Error!\n{}".format(str(e)))
    else:
        click.echo(f"\nInserted successfully.\n")
        click.echo(result)


@cli.group("calc", no_args_is_help=False)
@click.pass_obj
def calcUtils(obj):
    """Calculate distance, mkts_from_hybridts, mkts_from_unixtime and hybridts_to_unixtime."""
    pass


@calcUtils.command("distance")
@click.pass_obj
def calcDistance(obj):
    """
    Calculate distance between two vector arrays.

    Example:

        milvus_cli > calc distance

        Import left operator vectors from existing collection? [y/N]: n

        The vector's type (float_vectors, bin_vectors): float_vectors

        \b
        Left vectors:
            [[0.083, 0.992, 0.931, 0.433, 0.93, 0.706, 0.668, 0.481, 0.255, 0.088,
            0.121, 0.701, 0.935, 0.142, 0.012, 0.197, 0.066, 0.864, 0.263, 0.732,
            0.445, 0.672, 0.184, 0.675, 0.361, 0.115, 0.396, 0.206, 0.084, 0.274,
            0.523, 0.958, 0.071, 0.646, 0.864, 0.434, 0.212, 0.5, 0.319, 0.608,
            0.356, 0.745, 0.672, 0.488, 0.221, 0.485, 0.193, 0.557, 0.546, 0.626,
            0.593, 0.526, 0.404, 0.795, 0.076, 0.156, 0.231, 0.1, 0.18, 0.796,
            0.716, 0.752, 0.816, 0.363], [0.284, 0.135, 0.172, 0.198, 0.752, 0.174,
            0.314, 0.18, 0.672, 0.727, 0.062, 0.611, 0.921, 0.851, 0.238, 0.648,
            0.794, 0.177, 0.639, 0.339, 0.402, 0.977, 0.887, 0.528, 0.768, 0.16,
            0.698, 0.016, 0.906, 0.261, 0.902, 0.93, 0.547, 0.146, 0.65, 0.072,
            0.876, 0.645, 0.303, 0.922, 0.807, 0.093, 0.063, 0.344, 0.667, 0.81,
            0.662, 0.147, 0.242, 0.641, 0.903, 0.714, 0.637, 0.365, 0.512, 0.267,
            0.577, 0.809, 0.698, 0.62, 0.768, 0.402, 0.922, 0.592]]

        Import right operator vectors from existing collection? [y/N]: n

        The vector's type (float_vectors, bin_vectors): float_vectors

        \b
        Right vectors:
            [[0.518, 0.034, 0.786, 0.251, 0.04, 0.247, 0.55, 0.595, 0.638, 0.957,
            0.303, 0.023, 0.007, 0.712, 0.841, 0.648, 0.807, 0.429, 0.402, 0.904,
            0.002, 0.882, 0.69, 0.268, 0.732, 0.511, 0.942, 0.202, 0.749, 0.234,
            0.666, 0.517, 0.787, 0.399, 0.565, 0.457, 0.57, 0.937, 0.712, 0.981,
            0.928, 0.678, 0.154, 0.775, 0.754, 0.532, 0.074, 0.493, 0.288, 0.229,
            0.9, 0.657, 0.936, 0.184, 0.478, 0.587, 0.592, 0.84, 0.793, 0.985,
            0.826, 0.595, 0.947, 0.175], [0.704, 0.02, 0.937, 0.249, 0.431, 0.99,
            0.779, 0.855, 0.731, 0.665, 0.773, 0.647, 0.135, 0.44, 0.621, 0.329,
            0.718, 0.003, 0.927, 0.511, 0.515, 0.359, 0.744, 0.828, 0.31, 0.161,
            0.605, 0.539, 0.331, 0.077, 0.503, 0.668, 0.275, 0.72, 0.172, 0.035,
            0.88, 0.762, 0.646, 0.727, 0.83, 0.001, 0.085, 0.188, 0.583, 0.709,
            0.134, 0.683, 0.246, 0.214, 0.863, 0.109, 0.168, 0.539, 0.451, 0.303,
            0.064, 0.575, 0.547, 0.85, 0.75, 0.789, 0.681, 0.735], [0.648, 0.769,
            0.525, 0.716, 0.752, 0.199, 0.095, 0.222, 0.767, 0.029, 0.244, 0.527,
            0.496, 0.691, 0.487, 0.83, 0.546, 0.102, 0.845, 0.096, 0.744, 0.758,
            0.092, 0.289, 0.139, 0.005, 0.204, 0.245, 0.528, 0.607, 0.446, 0.029,
            0.686, 0.558, 0.705, 0.451, 0.87, 0.404, 0.824, 0.727, 0.058, 0.283,
            0.512, 0.682, 0.027, 0.026, 0.809, 0.669, 0.241, 0.103, 0.101, 0.225,
            0.989, 0.662, 0.917, 0.972, 0.93, 0.447, 0.318, 0.434, 0.437, 0.036,
            0.009, 0.96], [0.726, 0.418, 0.404, 0.244, 0.618, 0.356, 0.07, 0.842,
            0.137, 0.967, 0.465, 0.811, 0.027, 0.704, 0.935, 0.546, 0.92, 0.125,
            0.917, 0.089, 0.463, 0.929, 0.289, 0.721, 0.368, 0.837, 0.14, 0.431,
            0.495, 0.75, 0.484, 0.083, 0.431, 0.392, 0.177, 0.303, 0.013, 0.317,
            0.593, 0.047, 0.695, 0.185, 0.633, 0.825, 0.203, 0.619, 0.597, 0.152,
            0.899, 0.061, 0.512, 0.67, 0.82, 0.52, 0.743, 0.07, 0.99, 0.119,
            0.949, 0.284, 0.529, 0.65, 0.523, 0.059]]

        Supported metric type. Default is "L2" (L2, IP, HAMMING, TANIMOTO) [L2]: L2

        sqrt [False]: True

        Timeout(optional) []:
    """
    leftVectorMeta = {}
    if click.confirm("Import left operator vectors from existing collection?"):
        left_ids = click.prompt("The vectors' ids on the left of operator")
        left_collection = click.prompt(
            "The vectors' collection name on the left of operator"
        )
        left_partition = click.prompt(
            "The vectors' partition name on the left of operator"
        )
        left_field = click.prompt("The vectors' field name on the left of operator")
        leftVectorMeta["vec_type"] = "import"
        leftVectorMeta["ids"] = left_ids
        leftVectorMeta["collection"] = left_collection
        leftVectorMeta["partition"] = left_partition
        leftVectorMeta["field"] = left_field
    else:
        left_type = click.prompt(
            "The vector's type",
            type=click.Choice(["float_vectors", "bin_vectors"]),
            default="float_vectors",
        )
        left_vectors = click.prompt("Left vectors")
        leftVectorMeta["vec_type"] = "raw"
        leftVectorMeta["type"] = left_type
        leftVectorMeta["vectors"] = left_vectors
    rightVectorMeta = {}
    if click.confirm("Import right operator vectors from existing collection?"):
        right_ids = click.prompt("The vectors' ids on the right of operator")
        right_collection = click.prompt(
            "The vectors' collection name on the right of operator"
        )
        right_partition = click.prompt(
            "The vectors' partition name on the right of operator"
        )
        right_field = click.prompt("The vectors' field name on the right of operator")
        rightVectorMeta["vec_type"] = "import"
        rightVectorMeta["ids"] = right_ids
        rightVectorMeta["collection"] = right_collection
        rightVectorMeta["partition"] = right_partition
        rightVectorMeta["field"] = right_field
    else:
        right_type = click.prompt(
            "The vector's type",
            type=click.Choice(["float_vectors", "bin_vectors"]),
            default="float_vectors",
        )
        right_vectors = click.prompt("Right vectors")
        rightVectorMeta["vec_type"] = "raw"
        rightVectorMeta["type"] = right_type
        rightVectorMeta["vectors"] = right_vectors
    metric_type = click.prompt(
        'Supported metric type. Default is "L2"',
        default="L2",
        type=click.Choice(MetricTypes),
    )
    sqrt = None
    if metric_type in ["L2"]:
        sqrt = click.prompt("sqrt", type=bool, default=False)
    dim = None
    if metric_type in ["HAMMING", "TANIMOTO"]:
        dim = click.prompt(
            "Set this value if dimension is not a multiple of 8, otherwise the dimension will be calculted by list length",
            type=int,
            default=None,
        )
    timeout = click.prompt("Timeout(optional)", default="")
    try:
        calcParams = validateCalcParams(
            leftVectorMeta, rightVectorMeta, metric_type, sqrt, dim, timeout
        )
        result = obj.calcDistance(
            calcParams["vectors_left"],
            calcParams["vectors_right"],
            calcParams["params"],
            calcParams["timeout"],
        )
    except ParameterException as pe:
        click.echo("Error!\n{}".format(str(pe)))
    except ConnectException as ce:
        click.echo("Error!\n{}".format(str(ce)))
    else:
        click.echo(
            "\n======\nReturn type:\n"
            + "Assume the vectors_left: L_1, L_2, L_3\n"
            + "Assume the vectors_right: R_a, R_b\n"
            + 'Distance between L_n and R_m we called "D_n_m"\n'
            + "The returned distances are arranged like this:\n"
            + "   [[D_1_a, D_1_b],\n"
            + "   [D_2_a, D_2_b],\n"
            + "   [D_3_a, D_3_b]]\n"
            + '\nNote: if some vectors doesn\'t exist in collection, the returned distance is "-1.0"\n'
            + "======\n"
        )
        click.echo("Result:\n")
        click.echo(result)


@calcUtils.command("mkts_from_hybridts")
@click.option(
    "-h",
    "--hybridts",
    "hybridts",
    help="The original hybrid timestamp used to generate a new hybrid timestamp. Non-negative interger range from 0 to 18446744073709551615.",
    type=int,
)
@click.option(
    "-m",
    "--milliseconds",
    "milliseconds",
    help="Incremental time interval. The unit of time is milliseconds.",
    type=float,
    default=0.0,
)
# @click.option(
#     "-d",
#     "--delta",
#     "delta",
#     help="A duration expressing the difference between two date, time, or datetime instances to microsecond resolution.",
# )
@click.pass_obj
def hybridts2mkts(obj, hybridts, milliseconds):
    """Generate a hybrid timestamp based on an existing hybrid timestamp, timedelta and incremental time internval."""
    res = obj.mkts_from_hybridts(hybridts, milliseconds)
    click.echo(res)


@calcUtils.command("mkts_from_unixtime")
@click.option(
    "-e",
    "--epoch",
    "epoch",
    help="The known Unix Epoch time used to generate a hybrid timestamp. The Unix Epoch time is the number of seconds that have elapsed since January 1, 1970 (midnight UTC/GMT).",
    type=float,
)
@click.option(
    "-m",
    "--milliseconds",
    "milliseconds",
    help="Incremental time interval. The unit of time is milliseconds.",
    type=float,
    default=0.0,
)
@click.pass_obj
def unixtime2mkts(obj, epoch, milliseconds):
    """Generate a hybrid timestamp based on Unix Epoch time, timedelta and incremental time internval."""
    res = obj.mkts_from_unixtime(epoch, milliseconds)
    click.echo(res)


@calcUtils.command("hybridts_to_unixtime")
@click.option(
    "-h",
    "--hybridts",
    "hybridts",
    help="The known hybrid timestamp to convert to UNIX Epoch time. Non-negative interger range from 0 to 18446744073709551615.",
    type=int,
)
@click.pass_obj
def hybridts2unixtime(obj, hybridts):
    """Convert a hybrid timestamp to UNIX Epoch time ignoring the logic part."""
    res = obj.hybridts_to_unixtime(hybridts)
    click.echo(res)


@cli.command("load_balance")
@click.option(
    "-s",
    "--src-node-id",
    "src_node_id",
    help="The source query node id to balance.",
    type=int,
)
@click.option(
    "-d",
    "--dst-node-id",
    "dst_node_ids",
    help="[Multiple] - The destination query node ids to balance",
    type=int,
    multiple=True,
)
@click.option(
    "-ss",
    "--sealed-segment-ids",
    "sealed_segment_ids",
    help="[Multiple] - Sealed segment ids to balance.",
    type=int,
)
@click.option(
    "-t",
    "--timeout",
    "timeout",
    help="[Optional] - The timeout for this method, unit: second",
    default=None,
    type=int,
)
@click.pass_obj
def loadBalance(obj, src_node_id, dst_node_ids, sealed_segment_ids, timeout):
    """Do load balancing operation from source query node to destination query node."""
    res = obj.loadBalance(src_node_id, dst_node_ids, sealed_segment_ids, timeout)
    click.echo(res)


@cli.command("compact")
@click.option("-c", "--collection-name", "collectionName", help="Collection name")
@click.option(
    "-t",
    "--timeout",
    "timeout",
    help="[Optional] - An optional duration of time in seconds to allow for the RPC. If timeout is not set, the client keeps waiting until the server responds or an error occurs.",
    default=None,
    type=float,
)
@click.pass_obj
def compactCollection(obj, collectionName, timeout):
    """Compact merge the small segments in a collection."""
    obj.compactCollection(collectionName, timeout)


@cli.command("exit")
def quitapp():
    """Exit the CLI."""
    global quitapp
    quitapp = True


quitapp = False  # global flag
comp = Completer()


def runCliPrompt():
    args = sys.argv
    if args and (args[-1] == "--version"):
        print(f"Milvus_CLI v{getPackageVersion()}")
        return
    try:
        print(WELCOME_MSG)
        while not quitapp:
            import readline

            readline.set_completer_delims(" \t\n;")
            readline.parse_and_bind("tab: complete")
            readline.set_completer(comp.complete)
            astr = input("milvus_cli > ")
            try:
                cli(astr.split())
            except SystemExit:
                # trap argparse error message
                # print('error', SystemExit)
                continue
            except ParameterException as pe:
                click.echo(message=f"{str(pe)}", err=True)
            except ConnectException as ce:
                click.echo(
                    message="Connect to milvus Error!\nPlease check your connection.",
                    err=True,
                )
            except Exception as e:
                click.echo(message=f"Error occurred!\n{str(e)}", err=True)
        print(EXIT_MSG)
    except (KeyboardInterrupt, EOFError):
        print(EXIT_MSG)
        sys.exit(0)


if __name__ == "__main__":
    runCliPrompt()
