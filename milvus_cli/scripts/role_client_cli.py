from .helper_client_cli import create, getList, delete, grant, revoke
import click
import os
import sys

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from Types import Privileges


@create.command("role")
@click.option("-r", "--roleName", "roleName", help="The role name of milvus role.")
@click.pass_obj
def create_role(obj, roleName):
    """
    Create a new role.

    Example:

      milvus_cli > create role -r role1
    """
    try:
        click.echo(obj.role.createRole(roleName))
        obj.role.listRoles()
    except Exception as e:
        click.echo(message=e, err=True)


@getList.command("roles")
@click.pass_obj
def list_roles(obj):
    """List all roles in Milvus
    Example:

        milvus_cli >list roles
    """
    try:
        obj.role.listRoles()
    except Exception as e:
        click.echo(message=e, err=True)


@delete.command("role")
@click.option("-r", "--roleName", "roleName", help="The role name of milvus role.")
@click.pass_obj
def drop_role(obj, roleName):
    """
    Drop role.

    Example:

      milvus_cli > drop role -r role1
    """
    try:
        obj.role.dropRole(roleName)
        obj.role.listRoles()
    except Exception as e:
        click.echo(message=e, err=True)


@grant.command("role")
@click.option("-r", "--roleName", "roleName", help="The role name of milvus role.")
@click.option("-u", "--username", "username", help="The username of milvus user.")
@click.pass_obj
def add_role_users(obj, roleName, username):
    """
    Add user to role.

    Example:

      milvus_cli > grant role -r role1 -u user1
    """
    try:
        obj.role.grantRole(roleName, username)
    except Exception as e:
        click.echo(message=e, err=True)


@revoke.command("role")
@click.option("-r", "--roleName", "roleName", help="The role name of milvus role.")
@click.option("-u", "--username", "username", help="The username of milvus user.")
@click.pass_obj
def drop_role_users(obj, roleName, username):
    """
    Drop user from role.

    Example:

      milvus_cli > revoke role -r role1 -u user1
    """
    try:
        obj.role.revokeRole(roleName, username)
    except Exception as e:
        click.echo(message=e, err=True)


@grant.command("privilege")
@click.pass_obj
def grant_privilege(obj):
    """
    Grant privilege to role.

    Example:

      milvus_cli > grant privilege
    """
    try:
        roleName = click.prompt("Role name")
        print(roleName)
        objectType = click.prompt(
            "The type of object for which the privilege is to be assigned.",
            type=click.Choice(["Global", "Collection", "User"]),
        )
        objectName = click.prompt(
            "The name of the object to control access for", type=str
        )
        privilege = click.prompt(
            "The name of the privilege to assign.",
            type=click.Choice(
                Privileges,
                case_sensitive=True,
            ),
        )
        dbName = click.prompt(
            "The name of the database to which the object belongs.", default="default"
        )
        obj.role.grantPrivilege(roleName, objectName, objectType, privilege, dbName)
        obj.role.listGrants(roleName, objectName, objectType)
    except Exception as e:
        click.echo(message=e, err=True)


@revoke.command("privilege")
@click.pass_obj
def revoke_privilege(obj):
    """
    Revoke privilege from role.

    Example:

      milvus_cli > revoke privilege
    """
    try:
        roleName = click.prompt("Role name")
        objectType = click.prompt(
            "The type of object for which the privilege is to be assigned.",
            type=click.Choice(["Global", "Collection", "User"]),
        )
        objectName = click.prompt(
            "The name of the object to control access for", type=str
        )
        privilege = click.prompt(
            "The name of the privilege to assign.",
            type=click.Choice(
                Privileges,
                case_sensitive=True,
            ),
        )
        dbName = click.prompt(
            "The name of the database to which the object belongs.", default="default"
        )
        obj.role.revokePrivilege(roleName, objectName, objectType, privilege, dbName)
        obj.role.listGrants(roleName, objectName, objectType)
    except Exception as e:
        click.echo(message=e, err=True)


@getList.command("grants")
@click.option("-r", "--roleName", "roleName", help="The role name of milvus role.")
@click.option(
    "-o", "--objectName", "objectName", help="The object name of milvus object."
)
@click.option(
    "-t", "--objectType", "objectType", help="The object type of milvus object."
)
@click.pass_obj
def list_grants(obj, roleName, objectName, objectType):
    """
    List all grants in Milvus

    Example:

        milvus_cli > list grants -r role1 -o object1 -t Collection
    """
    try:
        obj.role.listGrants(roleName, objectName, objectType)
    except Exception as e:
        click.echo(message=e, err=True)
