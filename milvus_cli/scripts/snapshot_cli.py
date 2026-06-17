from .helper_cli import create, getList, delete, show
import click


@create.command("snapshot")
@click.option("-n", "--name", "name", help="Snapshot name.", required=True, type=str)
@click.option("-c", "--collection-name", "collectionName", help="Collection name.", required=True, type=str)
@click.option("-d", "--description", "description", help="[Optional] Description.", default="", type=str)
@click.pass_obj
def create_snapshot(obj, name, collectionName, description):
    """Create a new snapshot for a collection."""
    try:
        result = obj.snapshot.create_snapshot(name, collectionName, description)
        click.echo(result)
    except Exception as e:
        click.echo(message=e, err=True)


@getList.command("snapshots")
@click.option("-c", "--collection-name", "collectionName", help="[Optional] Collection name.", default="", type=str)
@click.pass_obj
def list_snapshots(obj, collectionName):
    """List all snapshots."""
    try:
        result = obj.snapshot.list_snapshots(collectionName)
        click.echo(obj.formatter.format_list(result, header="Snapshot"))
    except Exception as e:
        click.echo(message=e, err=True)


@show.command("snapshot")
@click.option("-n", "--name", "name", help="Snapshot name.", required=True, type=str)
@click.option("-c", "--collection-name", "collectionName", help="Collection name.", required=True, type=str)
@click.pass_obj
def describe_snapshot(obj, name, collectionName):
    """Show snapshot details."""
    try:
        result = obj.snapshot.describe_snapshot(name, collectionName)
        click.echo(result)
    except Exception as e:
        click.echo(message=e, err=True)


@delete.command("snapshot")
@click.option("-n", "--name", "name", help="Snapshot name.", required=True, type=str)
@click.option("-c", "--collection-name", "collectionName", help="Collection name.", required=True, type=str)
@click.option("--yes", is_flag=True, default=False, help="Skip confirmation prompt.")
@click.pass_obj
def drop_snapshot(obj, name, collectionName, yes):
    """Delete a snapshot."""
    if not yes:
        click.echo(
            f"Warning!\nYou are trying to delete snapshot '{name}'. This action cannot be undone!\n"
        )
        if not click.confirm("Do you want to continue?"):
            return
    try:
        result = obj.snapshot.drop_snapshot(name, collectionName)
        click.echo(result)
    except Exception as e:
        click.echo(message=e, err=True)
