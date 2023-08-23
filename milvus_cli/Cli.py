from Connection import MilvusConnection
from Database import MilvusDatabase
from Collection import MilvusCollection
from Index import MilvusIndex
from Data import MilvusData
from User import MilvusUser
from Alias import MilvusAlias
from Partition import MilvusPartition
from pymilvus import __version__


class MilvusCli(object):
    connection = MilvusConnection()
    database = MilvusDatabase()
    collection = MilvusCollection()
    index = MilvusIndex()
    data = MilvusData()
    user = MilvusUser()
    alias = MilvusAlias()
    partition = MilvusPartition()
