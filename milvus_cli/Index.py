from Collection import getTargetCollection
from tabulate import tabulate


class MilvusIndex(object):
    alias = "default"

    def create_index(
        self,
        collectionName,
        fieldName,
        indexName,
        indexType,
        metricType,
        params,
        alias,
    ):
        try:
            tempAlias = alias if alias else self.alias
            targetCollection = getTargetCollection(
                collectionName=collectionName, alias=tempAlias
            )
        except Exception as e:
            raise Exception(f"Get collection detail error!{str(e)}")
        formatParams = {}
        for param in params:
            paramList = param.split(":")
            [paramName, paramValue] = paramList
            formatParams[paramName] = paramValue

        indexParams = {
            "index_type": indexType,
            "params": formatParams,
            "metric_type": metricType,
        }
        try:
            res = targetCollection.create_index(
                field_name=fieldName,
                index_params=indexParams,
                index_name=indexName,
            )
            return res
        except Exception as e:
            raise Exception(f"Create index error!{str(e)}")

    def get_index_details(self, collectionName, indexName, alias):
        tempAlias = alias if alias else self.alias
        collection = getTargetCollection(collectionName, tempAlias)

        index = collection.index(index_name=indexName)
        if not index:
            return "No index!"
        rows = []
        rows.append(["Corresponding Collection", index.collection_name])
        rows.append(["Corresponding Field", index.field_name])
        rows.append(["Index Name", index.index_name])
        rows.append(["Index Type", index.params["index_type"]])
        rows.append(["Metric Type", index.params["metric_type"]])
        params = index.params["params"]
        paramsDetails = "\n- ".join(map(lambda k: f"{k[0]}: {k[1]}", params.items()))
        rows.append(["Params", paramsDetails])
        return tabulate(rows, tablefmt="grid")

    def drop_index(self, collectionName, indexName, alias, timeout=None):
        tempAlias = alias if alias else self.alias
        collection = getTargetCollection(collectionName, tempAlias)
        collection.drop_index(index_name=indexName, timeout=timeout)
        return self.list_indexes(collectionName, tempAlias)

    def has_index(self, collectionName, indexName, alias, timeout=None):
        tempAlias = alias if alias else self.alias
        collection = getTargetCollection(collectionName, tempAlias)
        return collection.has_index(index_name=indexName, timeout=timeout)

    def list_indexes(self, collectionName, alias):
        target = getTargetCollection(collectionName, alias)
        result = target.indexes

        rows = list(
            map(
                lambda x: [
                    x.field_name,
                    x.index_name,
                    x._index_params["index_type"],
                    x._index_params["metric_type"],
                    x._index_params["params"],
                ],
                result,
            )
        )
        return tabulate(
            rows,
            headers=["Field Name", "Index Name", "Index Type", "Metric Type", "Params"],
            tablefmt="grid",
            showindex=True,
        )
