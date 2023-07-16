from pymilvus import __version__
from Types import ConnectException

class MilvusConnection(object):
  host = "127.0.0.1"
  port = 19530
  alias = "default"

  def connect(self,
          alias=None,
          host=None,
          port=None,
          secure=False,
          username=None,
          password=None):
    print('---in')
    self.alias = alias
    self.host = host
    self.port = port
    trimUsername = None if username is None else username.strip()
    trimPwd = None if password is None else password.strip()

    from pymilvus import connections

    try:
      print('-123')
      return connections.connect(self.alias,
                            host=self.host,
                            port=self.port,
                            user=trimUsername,
                            password=trimPwd,
                            secure=secure)
    except Exception as e:
        raise ConnectException(f"Connect to Milvus error!{str(e)}")

a = MilvusConnection()
a.connect(host='127.0.0.1',port=19530)