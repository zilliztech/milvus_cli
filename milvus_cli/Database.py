from pymilvus import db

class MilvusDatabase():
  alias = "default"
  def create_database(self,dbName=None,alias=None):
    tempAlias = alias if alias else self.alias
    try:
      db.create_database(db_name=dbName,using=tempAlias)
      return f"Create database {dbName} successfully!"
    except Exception as e:
      raise f"Create database error!{str(e)}"
    
  def list_databases(self,alias=None):
    tempAlias = alias if alias else self.alias
    try:
      res = db.list_database(using=tempAlias)
      return res
    except Exception as e:
      raise f"List database error!{str(e)}"
  
  def drop_database(self,dbName=None,alias=None):
    tempAlias = alias if alias else self.alias
    try:
      db.drop_database(db_name=dbName,using=tempAlias)
      return f"Drop database {dbName} successfully!"
    except Exception as e:
      raise f"Drop database error!{str(e)}"
    
  def using_database(self,dbName=None,alias=None):
    tempAlias = alias if alias else self.alias
    try:
      db.using_database(db_name=dbName,using=tempAlias)
      return f"Using database {dbName} successfully!"
    except Exception as e:
      raise f"Using database error!{str(e)}"