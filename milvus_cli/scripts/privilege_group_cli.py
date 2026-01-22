from .helper_cli import create, getList, delete, grant, revoke
from milvus_cli.Types import Privileges
import click


@create.command("privilege_group")
@click.option(
    "-n",
    "--name",
    "name",
    help="Privilege group name.",
    required=True,
    type=str,
)
@click.pass_obj
def create_privilege_group(obj, name):
    """
    Create a new privilege group.

    USAGE:
        milvus_cli > create privilege_group -n <name>

    OPTIONS:
        -n, --name    Name for the new privilege group (required)

    EXAMPLES:
        milvus_cli > create privilege_group -n read_only_group

    NOTES:
        - Privilege group names must be unique
        - After creating a group, use 'grant privilege_group' to add privileges

    SEE ALSO:
        list privilege_groups, grant privilege_group, delete privilege_group
    """
    try:
        result = obj.privilege_group.create_privilege_group(name)
        click.echo(result)
    except Exception as e:
        click.echo(message=e, err=True)


@getList.command("privilege_groups")
@click.pass_obj
def list_privilege_groups(obj):
    """
    List all privilege groups.

    USAGE:
        milvus_cli > list privilege_groups

    OUTPUT:
        Lists all privilege groups with their privileges.

    EXAMPLES:
        milvus_cli > list privilege_groups

    SEE ALSO:
        create privilege_group, grant privilege_group
    """
    try:
        result = obj.privilege_group.list_privilege_groups()
        if not result:
            click.echo("No privilege groups found.")
            return
        for group in result:
            # Handle both dict and object responses
            if isinstance(group, dict):
                group_name = group.get("privilege_group_name", group.get("group_name", "Unknown"))
                privs = group.get("privileges", [])
            else:
                group_name = group.privilege_group_name
                privs = group.privileges if group.privileges else []

            click.echo(f"Group: {group_name}")
            if privs:
                # Handle privileges as list of strings or objects
                if privs and isinstance(privs[0], str):
                    privilege_names = privs
                else:
                    privilege_names = [p.name if hasattr(p, 'name') else str(p) for p in privs]
                click.echo(f"  Privileges: {', '.join(privilege_names)}")
            else:
                click.echo("  Privileges: (none)")
            click.echo()
    except Exception as e:
        click.echo(message=e, err=True)


@grant.command("privilege_group")
@click.option(
    "-n",
    "--name",
    "name",
    help="Privilege group name.",
    required=True,
    type=str,
)
@click.option(
    "-p",
    "--privileges",
    "privileges",
    help="Comma-separated list of privileges to add.",
    required=True,
    type=str,
)
@click.pass_obj
def add_privileges_to_group(obj, name, privileges):
    """
    Add privileges to a privilege group.

    USAGE:
        milvus_cli > grant privilege_group -n <name> -p <privileges>

    OPTIONS:
        -n, --name        Privilege group name (required)
        -p, --privileges  Comma-separated list of privileges (required)

    AVAILABLE PRIVILEGES:
        CreateIndex, DropIndex, IndexDetail, Load, GetLoadingProgress,
        GetLoadState, Release, Insert, Delete, Upsert, Search, Flush,
        GetFlushState, Query, GetStatistics, Compaction, Import,
        LoadBalance, CreatePartition, DropPartition, ShowPartitions,
        HasPartition, All, CreateCollection, DropCollection,
        DescribeCollection, ShowCollections, RenameCollection, FlushAll,
        CreateOwnership, DropOwnership, SelectOwnership, ManageOwnership,
        CreateResourceGroup, DropResourceGroup, DescribeResourceGroup,
        ListResourceGroups, TransferNode, TransferReplica, CreateDatabase,
        DropDatabase, ListDatabases, CreateAlias, DropAlias, DescribeAlias,
        ListAliases, UpdateUser, SelectUser, *

    EXAMPLES:
        milvus_cli > grant privilege_group -n read_only_group -p Query,Search
        milvus_cli > grant privilege_group -n admin_group -p "*"

    SEE ALSO:
        revoke privilege_group, list privilege_groups
    """
    try:
        privilege_list = [p.strip() for p in privileges.split(",")]
        # Validate privileges
        invalid_privileges = [p for p in privilege_list if p not in Privileges]
        if invalid_privileges:
            click.echo(f"Error: Invalid privileges: {', '.join(invalid_privileges)}", err=True)
            click.echo(f"Available privileges: {', '.join(Privileges)}")
            return
        result = obj.privilege_group.add_privileges_to_group(name, privilege_list)
        click.echo(result)
    except Exception as e:
        click.echo(message=e, err=True)


@revoke.command("privilege_group")
@click.option(
    "-n",
    "--name",
    "name",
    help="Privilege group name.",
    required=True,
    type=str,
)
@click.option(
    "-p",
    "--privileges",
    "privileges",
    help="Comma-separated list of privileges to remove.",
    required=True,
    type=str,
)
@click.pass_obj
def remove_privileges_from_group(obj, name, privileges):
    """
    Remove privileges from a privilege group.

    USAGE:
        milvus_cli > revoke privilege_group -n <name> -p <privileges>

    OPTIONS:
        -n, --name        Privilege group name (required)
        -p, --privileges  Comma-separated list of privileges to remove (required)

    EXAMPLES:
        milvus_cli > revoke privilege_group -n read_only_group -p Delete,Insert

    SEE ALSO:
        grant privilege_group, list privilege_groups
    """
    try:
        privilege_list = [p.strip() for p in privileges.split(",")]
        # Validate privileges
        invalid_privileges = [p for p in privilege_list if p not in Privileges]
        if invalid_privileges:
            click.echo(f"Error: Invalid privileges: {', '.join(invalid_privileges)}", err=True)
            click.echo(f"Available privileges: {', '.join(Privileges)}")
            return
        result = obj.privilege_group.remove_privileges_from_group(name, privilege_list)
        click.echo(result)
    except Exception as e:
        click.echo(message=e, err=True)


@delete.command("privilege_group")
@click.option(
    "-n",
    "--name",
    "name",
    help="Privilege group name.",
    required=True,
    type=str,
)
@click.option(
    "--yes", "-y",
    is_flag=True,
    help="Skip confirmation prompt",
)
@click.pass_obj
def drop_privilege_group(obj, name, yes):
    """
    Delete a privilege group.

    USAGE:
        milvus_cli > delete privilege_group -n <name>

    OPTIONS:
        -n, --name    Privilege group name to delete (required)

    WARNING:
        This action cannot be undone. Any roles using this privilege group
        will lose those permissions.

    EXAMPLES:
        milvus_cli > delete privilege_group -n read_only_group
        milvus_cli > delete privilege_group -n read_only_group --yes

    SEE ALSO:
        list privilege_groups, create privilege_group
    """
    if not yes:
        click.echo(
            f"Warning!\nYou are trying to delete privilege group '{name}'. This action cannot be undone!\n"
        )
        if not click.confirm("Do you want to continue?"):
            return

    try:
        result = obj.privilege_group.drop_privilege_group(name)
        click.echo(result)
    except Exception as e:
        click.echo(message=e, err=True)
