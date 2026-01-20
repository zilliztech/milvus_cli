from pymilvus import MilvusClient
from tabulate import tabulate


class MilvusClientRole(object):
    """
    Role operations class based on MilvusClient API
    Used to replace the original Role operations based on ORM API
    """
    
    def __init__(self, connection_client=None):
        """
        Initialize Role client
        
        Args:
            connection_client: MilvusClientConnection instance
        """
        self.connection_client = connection_client

    def _get_client(self):
        """
        Get MilvusClient instance
        
        Returns:
            MilvusClient instance
            
        Raises:
            Exception: If not connected or connection is invalid
        """
        if not self.connection_client:
            raise Exception("Connection client not set!")
        
        client = self.connection_client.get_client()
        if not client:
            raise Exception("Not connected to Milvus! Please connect first.")
        
        return client

    def createRole(self, roleName):
        """
        Create a new role
        
        Args:
            roleName: Name of the role to create
            
        Returns:
            Success message
        """
        try:
            client = self._get_client()
            
            # Create role using MilvusClient API
            client.create_role(role_name=roleName)
            
            return f"Create role successfully!"
            
        except Exception as e:
            raise Exception(f"Create role error!{str(e)}")

    def listRoles(self):
        """
        List all roles with basic information
        
        Returns:
            List of role objects with role_name and privileges attributes
        """
        try:
            client = self._get_client()
            
            # List roles using MilvusClient API
            roles = client.list_roles()
            
            # For each role, get detailed information
            role_objects = []
            for role_name in roles:
                try:
                    role_info = client.describe_role(role_name=role_name)
                    
                    # Create a role object with role_name and privileges
                    role_obj = type('RoleInfo', (), {
                        'role_name': role_name,
                        'privileges': role_info.get('privileges', [])
                    })()
                    
                    role_objects.append(role_obj)
                    
                except Exception:
                    # If describe fails, create basic role object
                    role_obj = type('RoleInfo', (), {
                        'role_name': role_name,
                        'privileges': []
                    })()
                    role_objects.append(role_obj)
            
            # Print formatted table - only show role names
            data = [[role.role_name] for role in role_objects]
            print(tabulate(data, headers=["roleName"], tablefmt="pretty"))
            
            return role_objects
            
        except Exception as e:
            raise Exception(f"List role error!{str(e)}")

    def dropRole(self, roleName):
        """
        Drop a role
        
        Args:
            roleName: Name of the role to drop
            
        Returns:
            Success message
        """
        try:
            client = self._get_client()
            
            # Drop role using MilvusClient API
            client.drop_role(role_name=roleName)
            
            return f"Drop role successfully!"
            
        except Exception as e:
            raise Exception(f"Drop role error!{str(e)}")

    def grantRole(self, roleName, username):
        """
        Grant role to user (add user to role)
        
        Args:
            roleName: Name of the role
            username: Username to grant role to
            
        Returns:
            Success message
        """
        try:
            client = self._get_client()
            
            # Grant role to user using MilvusClient API
            client.grant_role(user_name=username, role_name=roleName)
            
            return f"Role '{roleName}' granted to user '{username}' successfully!"
            
        except Exception as e:
            raise Exception(f"Grant role error!{str(e)}")

    def revokeRole(self, roleName, username):
        """
        Revoke role from user (remove user from role)
        
        Args:
            roleName: Name of the role
            username: Username to revoke role from
            
        Returns:
            Success message
        """
        try:
            client = self._get_client()
            
            # Revoke role from user using MilvusClient API
            client.revoke_role(user_name=username, role_name=roleName)
            
            return f"Role '{roleName}' revoked from user '{username}' successfully!"
            
        except Exception as e:
            raise Exception(f"Revoke role error!{str(e)}")

    def grantPrivilege(self, roleName, objectName, objectType, privilege, dbName="default"):
        """
        Grant privilege to role
        
        Args:
            roleName: Name of the role
            objectName: Name of the object
            objectType: Type of the object (e.g., "Collection", "Global", etc.)
            privilege: Privilege to grant (e.g., "Search", "Insert", etc.)
            dbName: Database name (default: "default")
            
        Returns:
            Success message
        """
        try:
            client = self._get_client()
            
            # Grant privilege using MilvusClient API
            client.grant_privilege(
                role_name=roleName,
                object_type=objectType,
                object_name=objectName,
                privilege=privilege,
                db_name=dbName
            )
            
            return f"Grant privilege successfully!"
            
        except Exception as e:
            raise Exception(f"Grant privilege error!{str(e)}")

    def revokePrivilege(self, roleName, objectName, objectType, privilege, dbName="default"):
        """
        Revoke privilege from role
        
        Args:
            roleName: Name of the role
            objectName: Name of the object
            objectType: Type of the object (e.g., "Collection", "Global", etc.)
            privilege: Privilege to revoke (e.g., "Search", "Insert", etc.)
            dbName: Database name (default: "default")
            
        Returns:
            Success message
        """
        try:
            client = self._get_client()
            
            # Revoke privilege using MilvusClient API
            client.revoke_privilege(
                role_name=roleName,
                object_type=objectType,
                object_name=objectName,
                privilege=privilege,
                db_name=dbName
            )
            
            return f"Revoke privilege successfully!"
            
        except Exception as e:
            raise Exception(f"Revoke privilege error!{str(e)}")

    def listGrants(self, roleName, objectName, objectType):
        """
        List grants for a role on specific object
        
        Args:
            roleName: Name of the role
            objectName: Name of the object
            objectType: Type of the object
            
        Returns:
            List of grant data
        """
        try:
            client = self._get_client()
            
            # Get role information using MilvusClient API
            role_info = client.describe_role(role_name=roleName)
            
            # Extract privileges that match the object
            privileges = role_info.get('privileges', [])
            
            headers = [
                "Object",
                "Object Name", 
                "DB Name",
                "Role Name",
                "Grantor Name",
                "Privilege",
            ]
            
            data = []
            for priv in privileges:
                # Filter by object type and name if specified
                if (objectType and priv.get('object_type') != objectType):
                    continue
                if (objectName and objectName != "*" and priv.get('object_name') != objectName):
                    continue
                
                grant_row = [
                    priv.get('object_type', ''),
                    priv.get('object_name', ''),
                    priv.get('db_name', 'default'),
                    roleName,
                    priv.get('grantor_name', ''),
                    priv.get('privilege', ''),
                ]
                data.append(grant_row)
            
            # Print formatted table like original implementation
            print(tabulate(data, headers=headers, tablefmt="pretty"))
            
            return data
            
        except Exception as e:
            raise Exception(f"List grants error!{str(e)}")

    def describeRole(self, roleName):
        """
        Describe role details
        
        Args:
            roleName: Name of the role
            
        Returns:
            Role information dictionary
        """
        try:
            client = self._get_client()
            
            # Describe role using MilvusClient API
            role_info = client.describe_role(role_name=roleName)
            
            return role_info
            
        except Exception as e:
            raise Exception(f"Describe role error!{str(e)}")

    def hasRole(self, roleName):
        """
        Check if role exists
        
        Args:
            roleName: Name of the role to check
            
        Returns:
            bool: Whether role exists
        """
        try:
            roles = self.listRoleNames()
            return roleName in roles
            
        except Exception as e:
            raise Exception(f"Check role existence error!{str(e)}")

    def listRoleNames(self):
        """
        List all role names
        
        Returns:
            List of role names
        """
        try:
            client = self._get_client()
            
            # List roles using MilvusClient API
            roles = client.list_roles()
            
            return roles if roles else []
            
        except Exception as e:
            raise Exception(f"List role names error!{str(e)}")

    def getRolePrivileges(self, roleName):
        """
        Get privileges associated with a role
        
        Args:
            roleName: Name of the role
            
        Returns:
            List of privileges
        """
        try:
            role_info = self.describeRole(roleName)
            return role_info.get('privileges', [])
            
        except Exception as e:
            raise Exception(f"Get role privileges error!{str(e)}")
