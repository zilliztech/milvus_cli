from pymilvus import create_user, list_usernames, delete_user


class MilvusUser(object):
    def create_user(self, username, password):
        try:
            create_user(username, password)
        except Exception as e:
            raise Exception(f"Create user error!{str(e)}")
        else:
            return f"Create user successfully!"

    def list_users(self):
        try:
            res = list_usernames()
        except Exception as e:
            raise Exception(f"List users error!{str(e)}")
        else:
            return res

    def delete_user(self, username):
        try:
            res = delete_user(username)
        except Exception as e:
            raise Exception(f"Delete user error!{str(e)}")
        else:
            return f"Delete user {username} successfully!"
