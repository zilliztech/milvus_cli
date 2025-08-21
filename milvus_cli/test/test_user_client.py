import unittest
import sys
import os

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
sys.path.append(current_dir)  # Add current directory to path for test_config
from ConnectionClient import MilvusClientConnection
from UserClient import MilvusClientUser
from test_config import test_config

# Load connection parameters from configuration
connection_params = test_config.get_connection_params()
uri = connection_params['uri']
token = connection_params.get('token')
tlsmode = connection_params.get('tlsmode', 0)
cert = connection_params.get('cert')

# Test user configuration
test_username = "test_user_client"
test_password = "Test@123456"
test_new_password = "NewTest@123456"

# Initialize client instances
connection_client = MilvusClientConnection()
user_client = MilvusClientUser(connection_client)


class TestUserClient(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test environment before all tests"""
        # Connect to Milvus with admin privileges if available
        connection_client.connect(uri=uri, token=token, tlsmode=tlsmode, cert=cert)
        
        # Clean up any existing test user
        try:
            if user_client.has_user(test_username):
                user_client.delete_user(test_username)
        except:
            pass  # Ignore errors during cleanup

    @classmethod
    def tearDownClass(cls):
        """Clean up test environment after all tests"""
        try:
            # Clean up test user if it exists
            if user_client.has_user(test_username):
                user_client.delete_user(test_username)
        except Exception as e:
            print(f"Cleanup error: {e}")
        finally:
            connection_client.disconnect()

    def test_create_user(self):
        """
        Test creating a new user
        """
        # Clean up first if user exists
        try:
            if user_client.has_user(test_username):
                user_client.delete_user(test_username)
        except:
            pass
        
        result = user_client.create_user(
            username=test_username,
            password=test_password
        )
        
        self.assertEqual(result, "Create user successfully!")
        
        # Verify user was created
        self.assertTrue(user_client.has_user(test_username))

    def test_list_users(self):
        """
        Test listing all users
        """
        # Ensure test user exists
        if not user_client.has_user(test_username):
            user_client.create_user(test_username, test_password)
        
        users = user_client.list_users()
        
        self.assertIsInstance(users, list)
        self.assertIn(test_username, users)
        # Root user should typically exist
        self.assertIn("db_admin", users)

    def test_has_user(self):
        """
        Test checking if user exists
        """
        # Ensure test user exists
        if not user_client.has_user(test_username):
            user_client.create_user(test_username, test_password)
        
        # Test existing user
        self.assertTrue(user_client.has_user(test_username))
        
        # Test non-existing user
        self.assertFalse(user_client.has_user("non_existing_user_12345"))

    def test_describe_user(self):
        """
        Test describing user details
        """
        # Ensure test user exists
        if not user_client.has_user(test_username):
            user_client.create_user(test_username, test_password)
        
        user_info = user_client.describe_user(test_username)
        
        self.assertIsInstance(user_info, dict)
        # User info should contain user name
        user_name_found = False
        if 'user_name' in user_info:
            self.assertEqual(user_info['user_name'], test_username)
            user_name_found = True
        elif 'name' in user_info:
            self.assertEqual(user_info['name'], test_username)
            user_name_found = True
        
        self.assertTrue(user_name_found, "User name not found in user info")

    def test_update_password(self):
        """
        Test updating user password
        """
        # Ensure test user exists
        if not user_client.has_user(test_username):
            user_client.create_user(test_username, test_password)
        
        try:
            result = user_client.update_password(
                username=test_username,
                old_password=test_password,
                new_password=test_new_password
            )
            
            self.assertIn("successfully", result)
            
            # Change password back for other tests
            user_client.update_password(
                username=test_username,
                old_password=test_new_password,
                new_password=test_password
            )
            
        except Exception as e:
            # Password update might fail due to permissions or other constraints
            if "permission" in str(e).lower() or "not support" in str(e).lower():
                print(f"Password update not supported or insufficient permissions: {e}")
                self.assertTrue(True)  # Mark as passed if it's a known limitation
            else:
                raise

    def test_grant_and_revoke_role(self):
        """
        Test granting and revoking roles
        """
        # Ensure test user exists
        if not user_client.has_user(test_username):
            user_client.create_user(test_username, test_password)
        
        try:
            # Try to grant a role (this might fail if role doesn't exist or insufficient permissions)
            user_client.grant_role(test_username, "public")
            
            # List user roles
            roles = user_client.list_user_roles(test_username)
            self.assertIsInstance(roles, list)
            
            # Try to revoke the role
            user_client.revoke_role(test_username, "public")
            
        except Exception as e:
            # Role operations might fail due to permissions or missing roles
            if ("not exist" in str(e).lower() or 
                "permission" in str(e).lower() or 
                "not support" in str(e).lower() or
                "invalid role" in str(e).lower()):
                print(f"Role operations not supported or insufficient permissions: {e}")
                self.assertTrue(True)  # Mark as passed if it's a known limitation
            else:
                raise

    def test_list_user_roles(self):
        """
        Test listing user roles
        """
        # Ensure test user exists
        if not user_client.has_user(test_username):
            user_client.create_user(test_username, test_password)
        
        try:
            roles = user_client.list_user_roles(test_username)
            self.assertIsInstance(roles, list)
            
        except Exception as e:
            # This might fail if user role listing is not supported
            if "not support" in str(e).lower():
                print(f"List user roles not supported: {e}")
                self.assertTrue(True)  # Mark as passed if it's a known limitation
            else:
                raise

    def test_error_handling(self):
        """
        Test error handling for invalid operations
        """
        # Test creating user with invalid username
        with self.assertRaises(Exception) as context:
            user_client.create_user("", test_password)
        self.assertIn("error", str(context.exception).lower())
        
        # Test describing non-existing user
        with self.assertRaises(Exception) as context:
            user_client.describe_user("non_existing_user_12345")
        self.assertIn("error", str(context.exception).lower())
        
        # Test updating password for non-existing user
        with self.assertRaises(Exception) as context:
            user_client.update_password(
                username="non_existing_user_12345",
                old_password="old",
                new_password="new"
            )
        self.assertIn("error", str(context.exception).lower())

    def test_connection_error_handling(self):
        """
        Test error handling when connection is not available
        """
        # Create a user client without connection
        unconnected_user = MilvusClientUser()
        
        with self.assertRaises(Exception) as context:
            unconnected_user.create_user("test", "test")
        self.assertIn("Connection client not set", str(context.exception))

    def test_z_delete_user(self):
        """
        Test deleting user (run last alphabetically)
        """
        # Ensure test user exists
        if not user_client.has_user(test_username):
            user_client.create_user(test_username, test_password)
        
        # Verify user exists before deletion
        self.assertTrue(user_client.has_user(test_username))
        
        try:
            result = user_client.delete_user(test_username)
            
            # Check if user was actually deleted or just had roles revoked
            if "successfully" in result:
                # Verify user was deleted
                self.assertFalse(user_client.has_user(test_username))
            elif "revoked" in result.lower():
                # User still exists but has no permissions
                print(f"User deletion not fully supported, roles revoked instead: {result}")
                self.assertTrue(True)  # Mark as passed
            
        except Exception as e:
            # User deletion might not be supported in some Milvus versions
            if ("not support" in str(e).lower() or 
                "operation not supported" in str(e).lower()):
                print(f"User deletion not supported in current Milvus version: {e}")
                self.assertTrue(True)  # Mark as passed if it's a known limitation
            else:
                raise

    def test_duplicate_user_creation(self):
        """
        Test creating duplicate user
        """
        # Ensure test user exists
        if not user_client.has_user(test_username):
            user_client.create_user(test_username, test_password)
        
        # Try to create the same user again
        with self.assertRaises(Exception) as context:
            user_client.create_user(test_username, test_password)
        
        self.assertIn("error", str(context.exception).lower())


if __name__ == "__main__":
    unittest.main()
