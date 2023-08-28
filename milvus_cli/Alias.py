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
        collectionName,
    ):
        try:
            res = utility.list_aliases(
                collection_name=collectionName,
            )
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
