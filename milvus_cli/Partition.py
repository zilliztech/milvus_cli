from Collection import getTargetCollection
from tabulate import tabulate


class MilvusPartition(object):
    def create_partition(self, collectionName, description, partitionName):
        try:
            collection = getTargetCollection(
                collectionName=collectionName,
            )
            return collection.create_partition(partitionName, description=description)
        except Exception as e:
            raise Exception(f"Create partition error!{str(e)}")

    def describe_partition(self, collectionName, partitionName):
        collection = getTargetCollection(
            collectionName=collectionName,
        )
        partition = collection.partition(partitionName)

        return partition

    def delete_partition(self, collectionName, partitionName):
        try:
            collection = getTargetCollection(
                collectionName,
            )
            collection.drop_partition(partitionName)
            return self.list_partition_names(
                collectionName,
            )
        except Exception as e:
            raise Exception(f"Delete partition error!{str(e)}")

    def list_partition_names(self, collectionName):
        target = getTargetCollection(
            collectionName,
        )
        result = target.partitions
        return [i.name for i in result]

    def load_partition(self, collectionName, partitionName):
        try:
            target = self.describe_partition(
                collectionName,
                partitionName,
            )
            target.load()
            return f"Load partition {partitionName} successfully!"
        except Exception as e:
            raise Exception(f"Load partition error!{str(e)}")

    def release_partition(self, collectionName, partitionName):
        try:
            targetPartition = self.describe_partition(
                collectionName,
                partitionName,
            )
            targetPartition.release()
            return f"Release partition {partitionName} successfully!"
        except Exception as e:
            raise Exception(f"Release partition error!{str(e)}")
