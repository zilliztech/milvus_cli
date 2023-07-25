from pymilvus import __version__,connections,list_collections
from Types import ConnectException
from tabulate import tabulate

class MilvusConnection(object):
  uri = "127.0.0.1:19530"
  alias = "default"

  def connect(self,
          alias='',
          uri=None,
          username=None,
          password=None):
    self.alias = alias
    self.uri = uri
    trimUsername = None if username is None else username.strip()
    trimPwd = None if password is None else password.strip()

    try:
      res = connections.connect(alias= self.alias,
                            uri=self.uri,
                            user=trimUsername,
                            password=trimPwd
                            )  
      return res
    except Exception as e:
        raise ConnectException(f"Connect to Milvus error!{str(e)}")
  
  def checkConnection(self,alias=None):
    try:
      tempAlias = alias if alias else self.alias  
      collections = list_collections(timeout=10.0, using=tempAlias)
      return collections
    except Exception as e:
      raise ConnectException(f"Connect to Milvus error!{str(e)}")
    
  def showConnection(self, alias=None, showAll=False):
    tempAlias = alias if alias else self.alias
    allConnections = connections.list_connections()

    if showAll:
        return tabulate(allConnections,
                        headers=["Alias"],
                        tablefmt="pretty")
    aliasList = map(lambda x: x[0], allConnections)

    if tempAlias in aliasList:
        address, user = connections.get_connection_addr(tempAlias).values()
        return tabulate(
            [["Address", address], ["User", user], ["Alias", tempAlias]],
            tablefmt="pretty",
        )
    else:
        return "Connection not found!"
  
  def disconnect(self,alias=None):
    tempAlias = alias if alias else self.alias
    try:
      connections.disconnect(alias=tempAlias)
      return f"Disconnect from {tempAlias} successfully!"
    except Exception as e:
      raise f"Disconnect from {tempAlias} error!{str(e)}"

