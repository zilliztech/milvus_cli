from Collection import getTargetCollection
from tabulate import tabulate


class MilvusData:
    def insert(self, collectionName, data, partitionName=None, timeout=None):
        collection = getTargetCollection(
            collectionName,
        )
        result = collection.insert(data, partition_name=partitionName, timeout=timeout)
        return result

    def query(
        self,
        collectionName,
        queryParameters,
    ):
        collection = getTargetCollection(
            collectionName,
        )
        res = collection.query(**queryParameters)
        return res

    def delete_entities(
        self,
        expr,
        collectionName,
        partition_name=None,
    ):
        collection = getTargetCollection(
            collectionName,
        )
        result = collection.delete(expr, partition_name=partition_name)
        return result

    def search(self, collectionName, searchParameters, prettierFormat=True):
        collection = getTargetCollection(
            collectionName,
        )
        res = collection.search(**searchParameters)
        if not prettierFormat:
            return res
        hits = res[0]
        results = []
        for hits in res:
            results += [
                tabulate(
                    map(lambda x: [x.id, x.distance, x.score], hits),
                    headers=["Index", "ID", "Distance", "Score"],
                    tablefmt="grid",
                    showindex=True,
                )
            ]
        return tabulate(
            map(lambda x: [x.id, x.distance], hits),
            headers=["Index", "ID", "Distance"],
            tablefmt="grid",
            showindex=True,
        )
        # return res
