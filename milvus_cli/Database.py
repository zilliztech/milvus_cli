from pymilvus import db


class MilvusDatabase:
    def create_database(self, dbName=None):
        try:
            db.create_database(db_name=dbName)
            return f"Create database {dbName} successfully!"
        except Exception as e:
            raise Exception(f"Create database error!{str(e)}")

    def list_databases(self):
        try:
            res = db.list_database()
            return res
        except Exception as e:
            raise Exception(f"List database error!{str(e)}")

    def drop_database(self, dbName=None):
        try:
            db.drop_database(
                db_name=dbName,
            )
            return f"Drop database {dbName} successfully!"
        except Exception as e:
            raise Exception(f"Drop database error!{str(e)}")

    def using_database(self, dbName=None):
        try:
            db.using_database(db_name=dbName)
            return f"Using database {dbName} successfully!"
        except Exception as e:
            raise Exception(f"Using database error!{str(e)}")
