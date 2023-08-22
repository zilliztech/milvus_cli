from Collection import getTargetCollection
from tabulate import tabulate


class MilvusPartition(object):
    alias = "default"

    def create_partition(self, collectionName, description, partitionName, alias=None):
        try:
            collection = getTargetCollection(collectionName=collectionName, alias=alias)
            return collection.create_partition(partitionName, description=description)
        except Exception as e:
            raise Exception(f"Create partition error!{str(e)}")

    def describe_partition(self, collectionName, partitionName, alias=None):
        collection = getTargetCollection(collectionName=collectionName, alias=alias)
        partition = collection.partition(partitionName)

        return partition

    def delete_partition(self, collectionName, partitionName, alias=None):
        try:
            collection = getTargetCollection(collectionName, alias=alias)
            collection.drop_partition(partitionName)
            return self.list_partition_names(collectionName, alias=alias)
        except Exception as e:
            raise Exception(f"Delete partition error!{str(e)}")

    def list_partition_names(self, collectionName, alias=None):
        target = getTargetCollection(collectionName, alias)
        result = target.partitions
        return [i.name for i in result]

    def load_partition(self, collectionName, partitionName, alias=None):
        try:
            target = self.describe_partition(collectionName, partitionName, alias)
            print("---", target)
            target.load()
            return f"Load partition {partitionName} successfully!"
        except Exception as e:
            raise Exception(f"Load partition error!{str(e)}")

    def release_partition(self, collectionName, partitionName, alias=None):
        try:
            targetPartition = self.describe_partition(
                collectionName, partitionName, alias
            )
            targetPartition.release()
            return f"Release partition {partitionName} successfully!"
        except Exception as e:
            raise Exception(f"Release partition error!{str(e)}")
