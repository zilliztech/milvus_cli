from pymilvus import connections, Role, utility
from tabulate import tabulate


class MilvusRole(object):
    def createRole(self, roleName):
        try:
            role = Role(name=roleName)
            role.create()
        except Exception as e:
            raise Exception(f"Create role error!{str(e)}")
        else:
            return f"Create role successfully!"

    def listRoles(self):
        try:
            res = utility.list_roles(include_user_info=True)
            data = [[role.role_name, role.users] for role in res.groups]
            print(tabulate(data, headers=["roleName", "users"], tablefmt="pretty"))
        except Exception as e:
            raise Exception(f"List role error!{str(e)}")
        else:
            return res.groups

    def dropRole(self, roleName):
        try:
            role = Role(name=roleName)
            role.drop()
        except Exception as e:
            raise Exception(f"Drop role error!{str(e)}")
        else:
            return f"Drop role successfully!"

    def grantRole(self, roleName, username):
        try:
            role = Role(name=roleName)
            role.add_user(username=username)
            users = role.get_users()
            print(tabulate([users], headers=["users"], tablefmt="pretty"))
        except Exception as e:
            raise Exception(f"Grant role error!{str(e)}")
        else:
            return users

    def revokeRole(self, roleName, username):
        try:
            role = Role(name=roleName)
            role.remove_user(username=username)
            users = role.get_users()
            print(tabulate([users], headers=["users"], tablefmt="pretty"))
        except Exception as e:
            raise Exception(f"Revoke role error!{str(e)}")
        else:
            return users

    def grantPrivilege(
        self, roleName, objectName, objectType, privilege, dbName="default"
    ):
        try:
            role = Role(name=roleName)
            role.grant(
                object=objectType,
                privilege=privilege,
                object_name=objectName,
                db_name=dbName,
            )
        except Exception as e:
            raise Exception(f"Grant privilege error!{str(e)}")
        else:
            return f"Grant privilege successfully!"

    def revokePrivilege(
        self, roleName, objectName, objectType, privilege, dbName="default"
    ):
        try:
            role = Role(name=roleName)
            role.revoke(
                object=objectType,
                privilege=privilege,
                object_name=objectName,
                db_name=dbName,
            )
        except Exception as e:
            raise Exception(f"Revoke privilege error!{str(e)}")
        else:
            return f"Revoke privilege successfully!"

    def listGrants(self, roleName, objectName, objectType):
        try:
            role = Role(name=roleName)
            grants = role.list_grant(object=objectType, object_name=objectName)
            headers = [
                "Object",
                "Object Name",
                "DB Name",
                "Role Name",
                "Grantor Name",
                "Privilege",
            ]
            data = [
                [
                    grant.object,
                    grant.object_name,
                    grant.db_name,
                    grant.role_name,
                    grant.grantor_name,
                    grant.privilege,
                ]
                for grant in grants.groups
            ]

            print(tabulate(data, headers=headers, tablefmt="pretty"))
        except Exception as e:
            raise Exception(f"List grants error!{str(e)}")
        else:
            return data
