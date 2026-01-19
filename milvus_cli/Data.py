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

    def upsert(self, collectionName, data, partitionName=None, timeout=None):
        collection = getTargetCollection(
            collectionName,
        )
        result = collection.upsert(data, partition_name=partitionName, timeout=timeout)
        return result

    def search(self, collectionName, searchParameters, prettierFormat=True):
        collection = getTargetCollection(
            collectionName,
        )
        res = collection.search(**searchParameters)
        # only support search by single vector
        hits = res[0]
        results = []
        results = tabulate(
            [[hit.id, hit.distance, str(hit.entity)] for hit in hits],
            headers=["ID", "Distance", "Entity"],
            tablefmt="grid",
        )
        return results

    def hybrid_search(self, collectionName, requests, rerank, limit, output_fields=None):
        collection = getTargetCollection(
            collectionName,
        )
        res = collection.hybrid_search(requests, rerank, limit, output_fields=output_fields)
        # Format results
        if res and len(res) > 0:
            hits = res[0]
            results = tabulate(
                [[hit.id, hit.distance, str(hit.entity)] for hit in hits],
                headers=["ID", "Distance", "Entity"],
                tablefmt="grid",
            )
            return results
        return "No results found."

    def query_iterator(self, collectionName, batch_size, expr=None, output_fields=None, partition_names=None):
        collection = getTargetCollection(
            collectionName,
        )
        return collection.query_iterator(
            batch_size=batch_size,
            expr=expr,
            output_fields=output_fields,
            partition_names=partition_names,
        )

    def search_iterator(self, collectionName, batch_size, data, ann_field, param, limit, expr=None, output_fields=None):
        collection = getTargetCollection(
            collectionName,
        )
        return collection.search_iterator(
            data=data,
            ann_field=ann_field,
            param=param,
            batch_size=batch_size,
            limit=limit,
            expr=expr,
            output_fields=output_fields,
        )

    def get_by_ids(self, collectionName, ids, output_fields=None):
        collection = getTargetCollection(
            collectionName,
        )
        expr = f"{collection.primary_field.name} in {ids}"
        res = collection.query(expr=expr, output_fields=output_fields)
        return res

    def delete_by_ids(self, collectionName, ids, partition_name=None):
        collection = getTargetCollection(
            collectionName,
        )
        expr = f"{collection.primary_field.name} in {ids}"
        result = collection.delete(expr, partition_name=partition_name)
        return result

    def bulk_insert(self, collectionName, files, partition_name=None):
        from pymilvus import utility

        task_id = utility.do_bulk_insert(
            collection_name=collectionName, partition_name=partition_name, files=files
        )
        return task_id

    def get_bulk_insert_state(self, task_id):
        from pymilvus import utility

        state = utility.get_bulk_insert_state(task_id)
        return state

    def list_bulk_insert_tasks(self, limit=None, collectionName=None):
        from pymilvus import utility

        tasks = utility.list_bulk_insert_tasks(limit=limit, collection_name=collectionName)
        return tasks
