from .helper_cli import create, getList, delete, show, update
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


@show.command("user")
@click.option("-u", "--username", "username", help="The username of milvus user.", required=True)
@click.pass_obj
def describe_user(obj, username):
    """
    Show user details and assigned roles.

    USAGE:
        milvus_cli > show user -u <username>

    OPTIONS:
        -u, --username    Username to describe (required)

    OUTPUT:
        Displays user information including:
        - username: User name
        - roles: List of roles assigned to the user

    EXAMPLES:
        milvus_cli > show user -u admin

    SEE ALSO:
        list users, create user, grant role
    """
    try:
        result = obj.user.describe_user(username)
        click.echo(f"User: {username}")
        click.echo(f"Details: {result}")
    except Exception as e:
        click.echo(message=e, err=True)


@update.command("password")
@click.option("-u", "--username", "username", help="The username of milvus user.", required=True)
@click.pass_obj
def update_password(obj, username):
    """
    Update user password.

    USAGE:
        milvus_cli > update password -u <username>

    OPTIONS:
        -u, --username    Username whose password to update (required)

    INTERACTIVE PROMPTS:
        Old password      Current password
        New password      New password
        Confirm password  Confirm new password

    EXAMPLES:
        milvus_cli > update password -u admin

    SEE ALSO:
        show user, create user
    """
    try:
        old_password = click.prompt("Old password", hide_input=True)
        new_password = click.prompt("New password", hide_input=True)
        confirm_password = click.prompt("Confirm new password", hide_input=True)

        if new_password != confirm_password:
            click.echo("Error: New passwords do not match!", err=True)
            return

        result = obj.user.update_password(username, old_password, new_password)
        click.echo(result)
    except Exception as e:
        click.echo(message=e, err=True)
