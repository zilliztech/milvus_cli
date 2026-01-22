from .helper_cli import create, getList, delete, show, update, transfer
import click


@create.command("resource_group")
@click.option(
    "-n",
    "--name",
    "name",
    help="Resource group name.",
    required=True,
    type=str,
)
@click.pass_obj
def create_resource_group(obj, name):
    """
    Create a new resource group.

    USAGE:
        milvus_cli > create resource_group -n <name>

    OPTIONS:
        -n, --name    Name for the new resource group (required)

    INTERACTIVE PROMPTS:
        requests.node_num    Requested number of nodes
        limits.node_num      Maximum number of nodes

    EXAMPLES:
        milvus_cli > create resource_group -n rg1
        requests.node_num: 1
        limits.node_num: 3

    NOTES:
        - Resource group names must be unique
        - Default resource group '__default_resource_group' cannot be deleted

    SEE ALSO:
        list resource_groups, show resource_group, delete resource_group
    """
    try:
        use_config = click.confirm("Configure node limits?", default=False)
        config = None
        if use_config:
            from pymilvus.client.types import ResourceGroupConfig
            requests_node_num = click.prompt("requests.node_num", default=1, type=int)
            limits_node_num = click.prompt("limits.node_num", default=1, type=int)
            config = ResourceGroupConfig(
                requests={"node_num": requests_node_num},
                limits={"node_num": limits_node_num},
            )
        result = obj.resource_group.create_resource_group(name, config)
        click.echo(result)
    except Exception as e:
        click.echo(message=e, err=True)


@getList.command("resource_groups")
@click.pass_obj
def list_resource_groups(obj):
    """
    List all resource groups.

    USAGE:
        milvus_cli > list resource_groups

    OUTPUT:
        Lists all resource group names.

    EXAMPLES:
        milvus_cli > list resource_groups

    SEE ALSO:
        create resource_group, show resource_group
    """
    try:
        result = obj.resource_group.list_resource_groups()
        click.echo(obj.formatter.format_list(result, header="Resource Group"))
    except Exception as e:
        click.echo(message=e, err=True)


@show.command("resource_group")
@click.option(
    "-n",
    "--name",
    "name",
    help="Resource group name.",
    required=True,
    type=str,
)
@click.pass_obj
def describe_resource_group(obj, name):
    """
    Show resource group details.

    USAGE:
        milvus_cli > show resource_group -n <name>

    OPTIONS:
        -n, --name    Resource group name (required)

    OUTPUT:
        Displays resource group information including:
        - name: Resource group name
        - capacity: Total capacity
        - num_available_node: Available nodes
        - num_loaded_replica: Loaded replicas
        - num_outgoing_node: Outgoing nodes
        - num_incoming_node: Incoming nodes

    EXAMPLES:
        milvus_cli > show resource_group -n rg1

    SEE ALSO:
        list resource_groups, create resource_group
    """
    try:
        result = obj.resource_group.describe_resource_group(name)
        click.echo(f"Resource Group: {name}")
        click.echo(result)
    except Exception as e:
        click.echo(message=e, err=True)


@delete.command("resource_group")
@click.option(
    "-n",
    "--name",
    "name",
    help="Resource group name.",
    required=True,
    type=str,
)
@click.pass_obj
def drop_resource_group(obj, name):
    """
    Delete a resource group.

    USAGE:
        milvus_cli > delete resource_group -n <name>

    OPTIONS:
        -n, --name    Resource group name to delete (required)

    WARNING:
        The default resource group '__default_resource_group' cannot be deleted.

    EXAMPLES:
        milvus_cli > delete resource_group -n rg1

    SEE ALSO:
        list resource_groups, create resource_group
    """
    if name == "__default_resource_group":
        click.echo("Error: Cannot delete the default resource group!", err=True)
        return

    click.echo(
        f"Warning!\nYou are trying to delete resource group '{name}'. This action cannot be undone!\n"
    )
    if not click.confirm("Do you want to continue?"):
        return

    try:
        result = obj.resource_group.drop_resource_group(name)
        click.echo(result)
    except Exception as e:
        click.echo(message=e, err=True)


@update.command("resource_group")
@click.option(
    "-n",
    "--name",
    "name",
    help="Resource group name.",
    required=True,
    type=str,
)
@click.pass_obj
def update_resource_group(obj, name):
    """
    Update resource group configuration.

    USAGE:
        milvus_cli > update resource_group -n <name>

    OPTIONS:
        -n, --name    Resource group name to update (required)

    INTERACTIVE PROMPTS:
        requests.node_num    Requested number of nodes
        limits.node_num      Maximum number of nodes

    EXAMPLES:
        milvus_cli > update resource_group -n rg1
        requests.node_num: 2
        limits.node_num: 5

    SEE ALSO:
        show resource_group, create resource_group
    """
    try:
        from pymilvus.client.types import ResourceGroupConfig
        requests_node_num = click.prompt("requests.node_num", default=1, type=int)
        limits_node_num = click.prompt("limits.node_num", default=1, type=int)
        config = ResourceGroupConfig(
            requests={"node_num": requests_node_num},
            limits={"node_num": limits_node_num},
        )
        result = obj.resource_group.update_resource_groups({name: config})
        click.echo(result)
    except Exception as e:
        click.echo(message=e, err=True)


@transfer.command("replica")
@click.pass_obj
def transfer_replica(obj):
    """
    Transfer replicas between resource groups.

    USAGE:
        milvus_cli > transfer replica

    INTERACTIVE PROMPTS:
        Source group       Source resource group name
        Target group       Target resource group name
        Collection name    Collection to transfer replicas for
        Number of replicas Number of replicas to transfer

    EXAMPLES:
        milvus_cli > transfer replica
        Source group: rg1
        Target group: rg2
        Collection name: my_collection
        Number of replicas: 1

    SEE ALSO:
        show resource_group, list resource_groups
    """
    try:
        resource_groups = obj.resource_group.list_resource_groups()
        source_group = click.prompt(
            "Source resource group",
            type=click.Choice(resource_groups),
        )
        target_group = click.prompt(
            "Target resource group",
            type=click.Choice(resource_groups),
        )
        collection_name = click.prompt(
            "Collection name",
            type=click.Choice(obj.collection.list_collections()),
        )
        num_replicas = click.prompt("Number of replicas to transfer", default=1, type=int)

        result = obj.resource_group.transfer_replica(
            source_group, target_group, collection_name, num_replicas
        )
        click.echo(result)
    except Exception as e:
        click.echo(message=e, err=True)
