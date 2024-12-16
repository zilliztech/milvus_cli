import unittest
import sys
import os

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
from Connection import MilvusConnection
from Role import MilvusRole
from User import MilvusUser

uri = "http://localhost:19530"
roleName = "test_role_zilliz"
username = "test_zilliz"
objectName = "a"


milvusConnection = MilvusConnection()
role = MilvusRole()
user = MilvusUser()


class TestRole(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print("tear up")
        milvusConnection.connect(uri=uri, token="root:Milvus")
        user.create_user(username=username, password="123456")
        role.createRole(roleName=roleName)

    @classmethod
    def tearDownClass(cls):
        role.revokeRole(roleName=roleName, username=username)
        user.delete_user(username=username)
        role.dropRole(roleName=roleName)
        milvusConnection.disconnect()

    def test_create_role(self):
        data = role.listRoles()
        self.assertIn(roleName, [role.role_name for role in data])

    def test_grant_role(self):
        users = role.grantRole(roleName=roleName, username=username)
        self.assertIn(username, users)

    def test_revoke_role(self):
        users = role.revokeRole(roleName=roleName, username=username)
        self.assertNotIn(username, users)

    def test_grant_privilege(self):
        role.grantPrivilege(
            roleName=roleName,
            objectName=objectName,
            objectType="Collection",
            privilege="CreateIndex",
        )
        data = role.listGrants(
            roleName=roleName, objectName=objectName, objectType="Collection"
        )
        self.assertIn("CreateIndex", [priv[len(priv) - 1] for priv in data])

    def test_revoke_privilege(self):
        role.revokePrivilege(
            roleName=roleName,
            objectName=objectName,
            objectType="Collection",
            privilege="CreateIndex",
        )
        data = role.listGrants(
            roleName=roleName, objectName=objectName, objectType="Collection"
        )
        self.assertNotIn("CreateIndex", [priv[len(priv) - 1] for priv in data])

    def test_drop_role(self):
        tempName = "a"
        role.createRole(roleName=tempName)
        role.dropRole(roleName=tempName)
        data = role.listRoles()
        self.assertNotIn(tempName, [role.role_name for role in data])


if __name__ == "__main__":
    unittest.main()
