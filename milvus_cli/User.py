from pymilvus import create_user, list_usernames, delete_user


class MilvusUser(object):
    alias = "default"

    def create_user(self, username, password, alias=None):
        try:
            tempAlias = alias if alias else self.alias
            create_user(username, password, using=tempAlias)
        except Exception as e:
            raise Exception(f"Create user error!{str(e)}")
        else:
            return f"Create user successfully!"

    def list_users(self, alias=None):
        try:
            tempAlias = alias if alias else self.alias
            res = list_usernames(using=tempAlias)
        except Exception as e:
            raise Exception(f"List users error!{str(e)}")
        else:
            return res

    def delete_user(self, username, alias=None):
        try:
            tempAlias = alias if alias else self.alias
            res = delete_user(username, using=tempAlias)
        except Exception as e:
            raise Exception(f"Delete user error!{str(e)}")
        else:
            return f"Delete user {username} successfully!"
