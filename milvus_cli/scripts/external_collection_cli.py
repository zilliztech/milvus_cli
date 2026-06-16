from .helper_client_cli import cli, getList, show
import click


@cli.command("refresh_external_collection")
@click.option(
    "-c",
    "--collection-name",
    "collectionName",
    help="Collection name.",
    required=True,
    type=str,
)
@click.pass_obj
def refresh_external_collection(obj, collectionName):
    """Refresh external collection."""
    try:
        result = obj.external_collection.refresh_external_collection(collectionName)
        click.echo(result)
    except Exception as e:
        click.echo(message=e, err=True)


@show.command("refresh_external_collection_progress")
@click.option(
    "-c",
    "--collection-name",
    "collectionName",
    help="Collection name.",
    required=True,
    type=str,
)
@click.pass_obj
def show_refresh_external_collection_progress(obj, collectionName):
    """Show refresh external collection progress."""
    try:
        result = obj.external_collection.get_refresh_external_collection_progress(
            collectionName
        )
        click.echo(result)
    except Exception as e:
        click.echo(message=e, err=True)


@getList.command("refresh_external_collection_jobs")
@click.pass_obj
def list_refresh_external_collection_jobs(obj):
    """List refresh external collection jobs."""
    try:
        result = obj.external_collection.list_refresh_external_collection_jobs()
        click.echo(result)
    except Exception as e:
        click.echo(message=e, err=True)
