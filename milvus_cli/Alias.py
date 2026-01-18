from pymilvus import utility


class MilvusAlias(object):
    alias = "default"

    def create_alias(self, collectionName, aliasName):
        try:
            utility.create_alias(
                collection_name=collectionName,
                alias=aliasName,
            )
            return f"Create alias {aliasName} successfully!"
        except Exception as e:
            raise Exception(f"Create alias error!{str(e)}")

    def list_aliases(
        self,
        collectionName=None,
    ):
        try:
            if collectionName:
                res = utility.list_aliases(
                    collection_name=collectionName,
                )
                return res
            else:
                # When no collection name is specified, list all aliases
                # Get all aliases by iterating through the connection
                conn = utility._get_connection("default")
                res = conn.list_aliases(collection_name="")
                return res
        except Exception as e:
            raise Exception(f"List alias error!{str(e)}")

    def drop_alias(
        self,
        aliasName,
    ):
        try:
            utility.drop_alias(
                alias=aliasName,
            )
            return f"Drop alias {aliasName} successfully!"
        except Exception as e:
            raise Exception(f"Drop alias error!{str(e)}")

    def alter_alias(
        self,
        aliasName,
        collectionName,
    ):
        try:
            utility.alter_alias(
                alias=aliasName,
                collection_name=collectionName,
            )
            return f"Alter alias {aliasName} successfully!"
        except Exception as e:
            raise Exception(f"Alter alias error!{str(e)}")
