from pymilvus import MilvusClient


class MilvusClientUser(object):
    """
    User operations class based on MilvusClient API
    Used to replace the original User operations based on utility functions
    """
    
    def __init__(self, connection_client=None):
        """
        Initialize User client
        
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

    def create_user(self, username, password):
        """
        Create a new user
        
        Args:
            username: Username for the new user
            password: Password for the new user
            
        Returns:
            Success message
        """
        try:
            client = self._get_client()
            
            # Create user using MilvusClient API
            client.create_user(
                user_name=username,
                password=password
            )
            
            return f"Create user successfully!"
            
        except Exception as e:
            raise Exception(f"Create user error!{str(e)}")

    def list_users(self):
        """
        List all users
        
        Returns:
            List of usernames
        """
        try:
            client = self._get_client()
            
            # List users using MilvusClient API
            users = client.list_users()
            
            # Extract usernames from the response and ensure it's a Python list
            if isinstance(users, list):
                # If users is a list of strings (usernames)
                if users and isinstance(users[0], str):
                    return list(users)  # Ensure it's a standard Python list
                # If users is a list of dictionaries
                elif users and isinstance(users[0], dict):
                    return [user.get('user_name', user.get('name', '')) for user in users]
                else:
                    return list(users)  # Convert to standard Python list
            
            # Convert any other iterable to list, or return empty list
            if users:
                try:
                    return list(users)
                except (TypeError, ValueError):
                    return []
            return []
            
        except Exception as e:
            raise Exception(f"List users error!{str(e)}")

    def delete_user(self, username):
        """
        Delete a user
        
        Args:
            username: Username to delete
            
        Returns:
            Success message
        """
        try:
            client = self._get_client()
            
            # Check if user exists first
            users = self.list_users()
            if username not in users:
                raise Exception(f"User '{username}' does not exist")
            
            # Try to delete user using MilvusClient API
            try:
                client.drop_user(user_name=username)
            except AttributeError:
                # If drop_user method doesn't exist, try alternative method
                try:
                    # Some versions might have delete_user instead
                    client.delete_user(user_name=username)
                except AttributeError:
                    # If no direct delete method exists, we need to revoke all roles first
                    try:
                        # Get user details to see roles
                        user_info = client.describe_user(user_name=username)
                        roles = user_info.get('roles', [])
                        
                        # Revoke all roles from user
                        for role in roles:
                            role_name = role if isinstance(role, str) else role.get('role_name', role.get('name', ''))
                            if role_name:
                                try:
                                    client.revoke_role(user_name=username, role_name=role_name)
                                except:
                                    pass  # Continue even if some roles fail to revoke
                        
                        # Note: Some Milvus versions may not support direct user deletion
                        # In such cases, the user will have no roles but still exist
                        return f"Revoked all roles from user {username} (user still exists but has no permissions)!"
                        
                    except Exception:
                        raise Exception(f"Cannot delete user '{username}' - operation not supported in current Milvus version")
            
            return f"Delete user {username} successfully!"
            
        except Exception as e:
            raise Exception(f"Delete user error!{str(e)}")

    def describe_user(self, username):
        """
        Get user details
        
        Args:
            username: Username to describe
            
        Returns:
            User information dictionary
        """
        try:
            client = self._get_client()
            
            # Check if user exists first
            if not self.has_user(username):
                raise Exception(f"User '{username}' does not exist")
            
            # Describe user using MilvusClient API
            user_info = client.describe_user(user_name=username)
            
            return user_info
            
        except Exception as e:
            raise Exception(f"Describe user error!{str(e)}")

    def update_password(self, username, old_password, new_password):
        """
        Update user password
        
        Args:
            username: Username
            old_password: Current password
            new_password: New password
            
        Returns:
            Success message
        """
        try:
            client = self._get_client()
            
            # Update password using MilvusClient API
            client.update_password(
                user_name=username,
                old_password=old_password,
                new_password=new_password
            )
            
            return f"Update password for user {username} successfully!"
            
        except Exception as e:
            raise Exception(f"Update password error!{str(e)}")

    def grant_role(self, username, role_name):
        """
        Grant a role to user
        
        Args:
            username: Username
            role_name: Role name to grant
            
        Returns:
            Success message
        """
        try:
            client = self._get_client()
            
            # Grant role using MilvusClient API
            client.grant_role(
                user_name=username,
                role_name=role_name
            )
            
            return f"Grant role {role_name} to user {username} successfully!"
            
        except Exception as e:
            raise Exception(f"Grant role error!{str(e)}")

    def revoke_role(self, username, role_name):
        """
        Revoke a role from user
        
        Args:
            username: Username
            role_name: Role name to revoke
            
        Returns:
            Success message
        """
        try:
            client = self._get_client()
            
            # Revoke role using MilvusClient API
            client.revoke_role(
                user_name=username,
                role_name=role_name
            )
            
            return f"Revoke role {role_name} from user {username} successfully!"
            
        except Exception as e:
            raise Exception(f"Revoke role error!{str(e)}")

    def list_user_roles(self, username):
        """
        List roles of a specific user
        
        Args:
            username: Username
            
        Returns:
            List of role names
        """
        try:
            user_info = self.describe_user(username)
            roles = user_info.get('roles', [])
            
            # Extract role names
            if isinstance(roles, list):
                role_names = []
                for role in roles:
                    if isinstance(role, str):
                        role_names.append(role)
                    elif isinstance(role, dict):
                        role_name = role.get('role_name', role.get('name', ''))
                        if role_name:
                            role_names.append(role_name)
                return role_names
            
            return roles if roles else []
            
        except Exception as e:
            raise Exception(f"List user roles error!{str(e)}")

    def has_user(self, username):
        """
        Check if user exists
        
        Args:
            username: Username to check
            
        Returns:
            bool: Whether user exists
        """
        try:
            users = self.list_users()
            return username in users
            
        except Exception as e:
            raise Exception(f"Check user existence error!{str(e)}")
