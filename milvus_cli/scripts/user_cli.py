from helper_cli import create, getList, delete
import click


@create.command("user")
@click.option("-u", "--username", "username", help="The username of milvus user.")
@click.option("-p", "--password", "password", help="The password of milvus user.")
@click.option(
    "-a",
    "--alias",
    "alias",
    help="The connection alias name.",
    type=str,
)
@click.pass_obj
def create_user(obj, username, password, alias=None):
    """
    Create user.

    Example:

        milvus_cli > create user -u zilliz -p zilliz
    """
    try:
        click.echo(obj.user.create_user(username, password, alias))
        click.echo(obj.user.list_users(alias))
    except Exception as e:
        click.echo(message=e, err=True)


@getList.command("users")
@click.option(
    "-a",
    "--alias",
    "alias",
    help="The connection alias name.",
    type=str,
)
@click.pass_obj
def list_users(obj, alias=None):
    """List all users in Milvus"""
    try:
        click.echo(obj.user.list_users(alias))
    except Exception as e:
        click.echo(message=e, err=True)


@delete.command("user")
@click.option("-u", "--username", "username", help="The username of milvus user.")
@click.option(
    "-a",
    "--alias",
    "alias",
    help="The connection alias name.",
    type=str,
)
@click.pass_obj
def deleteUser(obj, username, alias):
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
        result = obj.user.delete_user(username, alias)
        click.echo(result)
        click.echo(obj.user.list_users(alias))
    except Exception as e:
        click.echo(message=e, err=True)
