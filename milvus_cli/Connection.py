from pymilvus import connections
from Types import ConnectException


class MilvusConnection(object):
    uri = "127.0.0.1:19530"
    alias = "default"

    def connect(self, uri=None, token=None, tlsmode=0, cert=None):
        self.uri = uri
        trimToken = None if token is None else token.strip()
        trimcert = None if cert is None else cert.strip()

        try:
            if tlsmode == 0:
                res = connections.connect(alias=self.alias, uri=self.uri, token=trimToken)
            elif tlsmode == 1:
                if not cert:
                    res = connections.connect(alias=self.alias, uri=self.uri, token=trimToken, secure=True)
                else:
                    res = connections.connect(alias=self.alias, uri=self.uri, token=trimToken, secure=True, server_pem_path=trimcert)
            elif tlsmode == 2:
                raise NotImplementedError("two-way encryption (tlsmode == 2) is not implemented yet")
            return res
        except Exception as e:
            raise ConnectException(f"Connect to Milvus error!{str(e)}")

    def showConnection(self, showAll=False):
        tempAlias = self.alias
        try:
            allConnections = connections.list_connections()
            if showAll:
                return allConnections

            aliasList = map(lambda x: x[0], allConnections)

            if tempAlias in aliasList:
                response = connections.get_connection_addr(tempAlias).values()
                return response
            else:
                return "Connection not found!"
        except Exception as e:
            raise Exception(f"Show connection error!{str(e)}")

    def disconnect(self):
        try:
            connections.disconnect(alias=self.alias)
            return f"Disconnect from {self.alias} successfully!"
        except Exception as e:
            raise Exception(f"Disconnect from {self.alias} error!{str(e)}")
