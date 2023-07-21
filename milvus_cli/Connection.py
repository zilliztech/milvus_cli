from pymilvus import __version__,connections,list_collections
from Types import ConnectException
from tabulate import tabulate

class MilvusConnection(object):
  host = "127.0.0.1"
  port = 19530
  alias = "default"

  def connect(self,
          alias='',
          host=None,
          port=None,
          secure=False,
          username=None,
          password=None):
    self.alias = alias
    self.host = host
    self.port = port
    trimUsername = None if username is None else username.strip()
    trimPwd = None if password is None else password.strip()

    try:
      res = connections.connect(alias= self.alias,
                            host=self.host,
                            port=self.port,
                            user=trimUsername,
                            password=trimPwd,
                            secure=secure)
      return res
    except Exception as e:
        raise ConnectException(f"Connect to Milvus error!{str(e)}")
  
  def checkConnection(self):
    try:
        list_collections(timeout=10.0, using=self.alias)
    except Exception as e:
        raise ConnectException(f"Connect to Milvus error!{str(e)}")
    
  def showConnection(self, alias=None, showAll=False):
    tempAlias = alias if alias else self.alias
    allConnections = connections.list_connections()
    if showAll:
        return tabulate(allConnections,
                        headers=["Alias", "Instance"],
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

