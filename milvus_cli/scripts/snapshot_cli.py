from .helper_cli import create, getList, delete, show
import click


@create.command("snapshot")
@click.option(
    "-n",
    "--name",
    "name",
    help="Snapshot name.",
    required=True,
    type=str,
)
@click.pass_obj
def create_snapshot(obj, name):
    """Create a new snapshot."""
    try:
        result = obj.snapshot.create_snapshot(name)
        click.echo(result)
    except Exception as e:
        click.echo(message=e, err=True)


@getList.command("snapshots")
@click.pass_obj
def list_snapshots(obj):
    """List all snapshots."""
    try:
        result = obj.snapshot.list_snapshots()
        click.echo(obj.formatter.format_list(result, header="Snapshot"))
    except Exception as e:
        click.echo(message=e, err=True)


@show.command("snapshot")
@click.option(
    "-n",
    "--name",
    "name",
    help="Snapshot name.",
    required=True,
    type=str,
)
@click.pass_obj
def describe_snapshot(obj, name):
    """Show snapshot details."""
    try:
        result = obj.snapshot.describe_snapshot(name)
        click.echo(result)
    except Exception as e:
        click.echo(message=e, err=True)


@delete.command("snapshot")
@click.option(
    "-n",
    "--name",
    "name",
    help="Snapshot name.",
    required=True,
    type=str,
)
@click.option("--yes", is_flag=True, default=False, help="Skip confirmation prompt.")
@click.pass_obj
def drop_snapshot(obj, name, yes):
    """Delete a snapshot."""
    if not yes:
        click.echo(
            f"Warning!\nYou are trying to delete snapshot '{name}'. This action cannot be undone!\n"
        )
        if not click.confirm("Do you want to continue?"):
            return

    try:
        result = obj.snapshot.drop_snapshot(name)
        click.echo(result)
    except Exception as e:
        click.echo(message=e, err=True)


@click.command("restore_snapshot")
@click.option(
    "-n",
    "--name",
    "name",
    help="Snapshot name.",
    required=True,
    type=str,
)
@click.option("--yes", is_flag=True, default=False, help="Skip confirmation prompt.")
@click.pass_obj
def restore_snapshot(obj, name, yes):
    """Restore a snapshot."""
    if not yes:
        click.echo(
            f"Warning!\nYou are trying to restore snapshot '{name}'. This action cannot be undone!\n"
        )
        if not click.confirm("Do you want to continue?"):
            return

    try:
        result = obj.snapshot.restore_snapshot(name)
        click.echo(result)
    except Exception as e:
        click.echo(message=e, err=True)


@click.command("show_restore_state")
@click.option(
    "-n",
    "--name",
    "name",
    help="Snapshot name.",
    required=True,
    type=str,
)
@click.pass_obj
def show_restore_state(obj, name):
    """Show restore snapshot state."""
    try:
        result = obj.snapshot.get_restore_snapshot_state(name)
        click.echo(result)
    except Exception as e:
        click.echo(message=e, err=True)


@click.command("list_restore_jobs")
@click.pass_obj
def list_restore_jobs(obj):
    """List all restore snapshot jobs."""
    try:
        result = obj.snapshot.list_restore_snapshot_jobs()
        click.echo(obj.formatter.format_list(result, header="Restore Job"))
    except Exception as e:
        click.echo(message=e, err=True)


@click.command("pin_snapshot")
@click.option(
    "-n",
    "--name",
    "name",
    help="Snapshot name.",
    required=True,
    type=str,
)
@click.pass_obj
def pin_snapshot(obj, name):
    """Pin snapshot data."""
    try:
        result = obj.snapshot.pin_snapshot_data(name)
        click.echo(result)
    except Exception as e:
        click.echo(message=e, err=True)


@click.command("unpin_snapshot")
@click.option(
    "-n",
    "--name",
    "name",
    help="Snapshot name.",
    required=True,
    type=str,
)
@click.pass_obj
def unpin_snapshot(obj, name):
    """Unpin snapshot data."""
    try:
        result = obj.snapshot.unpin_snapshot_data(name)
        click.echo(result)
    except Exception as e:
        click.echo(message=e, err=True)
