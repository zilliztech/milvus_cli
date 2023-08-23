from helper_cli import create, getList, delete
import click


@create.command("user")
@click.option("-u", "--username", "username", help="The username of milvus user.")
@click.option("-p", "--password", "password", help="The password of milvus user.")
@click.pass_obj
def create_user(obj, username, password):
    """
    Create user.

    Example:

        milvus_cli > create user -u zilliz -p zilliz
    """
    try:
        click.echo(
            obj.user.create_user(
                username,
                password,
            )
        )
        click.echo(obj.user.list_users())
    except Exception as e:
        click.echo(message=e, err=True)


@getList.command("users")
@click.pass_obj
def list_users(obj):
    """List all users in Milvus
    Example:

        milvus_cli >list users
    """
    try:
        click.echo(obj.user.list_users())
    except Exception as e:
        click.echo(message=e, err=True)


@delete.command("user")
@click.option("-u", "--username", "username", help="The username of milvus user.")
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
        result = obj.user.delete_user(
            username,
        )
        click.echo(result)
        click.echo(obj.user.list_users())
    except Exception as e:
        click.echo(message=e, err=True)
