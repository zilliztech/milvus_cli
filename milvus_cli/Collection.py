from pymilvus import (
    list_collections,
    has_collection,
    utility,
    Collection,
    DataType,
    FieldSchema,
    CollectionSchema,
)
from tabulate import tabulate
from Types import DataTypeByNum


def getTargetCollection(
    collectionName,
):
    try:
        target = Collection(collectionName)
    except Exception as e:
        raise Exception(f"Get collection error!{str(e)}")
    else:
        return target


class MilvusCollection(object):
    def create_collection(
        self,
        collectionName,
        primaryField,
        fields,
        autoId=None,
        description=None,
        isDynamic=None,
        consistencyLevel="Bounded",
        shardsNum=1,
        functions=None,
    ):
        schema = CollectionSchema(
            fields=fields,
            primary_field=primaryField,
            auto_id=autoId,
            description=description,
            _enable_dynamic_field=isDynamic,
            functions=functions,
        )

        collection = Collection(
            name=collectionName,
            schema=schema,
            consistency_level=consistencyLevel,
            shards_num=shardsNum,
        )
        return self.get_collection_details(collection=collection)

    def list_collections(
        self,
    ):
        try:
            res = list_collections()
        except Exception as e:
            raise Exception(f"List collection error!{str(e)}")
        else:
            return res

    def drop_collection(self, collectionName=None):
        try:
            target = getTargetCollection(collectionName=collectionName)
            target.drop()
        except Exception as e:
            raise Exception(f"Delete collection error!{str(e)}")
        else:
            return f"Drop collection {collectionName} successfully!"

    def has_collection(self, collectionName=None):
        try:
            res = has_collection(collection_name=collectionName)
        except Exception as e:
            raise Exception(f"Has collection error!{str(e)}")
        else:
            return res

    def load_collection(self, collectionName=None):
        try:
            target = getTargetCollection(collectionName=collectionName)
            target.load()
        except Exception as e:
            raise Exception(f"Load collection error!{str(e)}")
        else:
            return f"Load collection {collectionName} successfully!"

    def release_collection(self, collectionName=None):
        try:
            target = getTargetCollection(collectionName=collectionName)
            target.release()
        except Exception as e:
            raise Exception(f"Release collection error!{str(e)}")
        else:
            return f"Release collection {collectionName} successfully!"

    def rename_collection(self, collectionName=None, newName=None):
        try:
            utility.rename_collection(
                old_collection_name=collectionName,
                new_collection_name=newName,
            )
        except Exception as e:
            raise Exception(f"Rename collection error!{str(e)}")
        else:
            return f"Rename collection {collectionName} to {newName} successfully!"

    def get_collection_details(self, collectionName="", collection=None):
        try:
            target = collection or getTargetCollection(
                collectionName=collectionName,
            )
        except Exception as e:
            raise Exception(f"Get collection detail error!{str(e)}")
        rows = []
        schema = target.schema
        partitions = target.partitions
        indexes = target.indexes
        fieldSchemaDetails = ""

        for fieldSchema in schema.fields:
            _name = f"{'*' if fieldSchema.is_primary else ''}{fieldSchema.name}"

            _type = DataTypeByNum[fieldSchema.dtype]
            _desc = fieldSchema.description
            _params = fieldSchema.params
            _is_function_output = fieldSchema.is_function_output
            _dim = _params.get("dim")
            _params_desc = f"dim: {_dim}" if _dim else ""
            _params_desc += (
                f", Is function output: {_is_function_output}"
                if _is_function_output
                else ""
            )

            _element_type = fieldSchema.element_type
            _max_length = _params.get("max_length")
            _max_capacity = _params.get("max_capacity")
            _enable_match = _params.get("enable_match")
            _enable_analyzer = _params.get("enable_analyzer")
            _analyzer_params = _params.get("analyzer_params")

            _params_desc += f", max_capacity: {_max_capacity}" if _max_capacity else ""
            _params_desc += f", element_type: {_element_type}" if _element_type else ""
            _params_desc += f", max_length: {_max_length}" if _max_length else ""
            _params_desc += f", enable_match: {_enable_match}" if _enable_match else ""
            _params_desc += (
                f", enable_analyzer: {_enable_analyzer}" if _enable_analyzer else ""
            )
            _params_desc += (
                f", analyzer_params: {_analyzer_params}" if _analyzer_params else ""
            )

            fieldSchemaDetails += f"\n - {_name} {_type} {_params_desc} {_desc}"
        schemaDetails = """Description: {}\n\nAuto ID: {}\n\nFields(* is the primary field):{}""".format(
            schema.description, schema.auto_id, fieldSchemaDetails
        )
        partitionDetails = "  - " + "\n- ".join(map(lambda x: x.name, partitions))
        indexesDetails = "  - " + "\n- ".join(map(lambda x: x.field_name, indexes))
        rows.append(["Name", target.name])
        rows.append(["Description", target.description])
        rows.append(["Is Empty", target.is_empty])
        rows.append(["Entities", target.num_entities])
        rows.append(["Primary Field", target.primary_field.name])
        rows.append(["Schema", schemaDetails])
        rows.append(["Partitions", partitionDetails])
        rows.append(["Indexes", indexesDetails])
        return tabulate(rows, tablefmt="grid")

    def get_entities_count(self, collectionName):
        col = getTargetCollection(collectionName)
        return col.num_entities

    def show_loading_progress(
        self,
        collectionName=None,
    ):
        try:
            self.get_entities_count(
                collectionName,
            )
            return utility.loading_progress(
                collectionName,
                None,
            )
        except Exception as e:
            raise Exception(f"Show loading progress error!{str(e)}")

    def list_field_names(self, collectionName):
        target = getTargetCollection(collectionName)
        result = target.schema.fields
        return [i.name for i in result]

    def list_fields_info(self, collectionName):
        target = getTargetCollection(collectionName)
        result = target.schema.fields
        return [
            {
                "name": i.name,
                "type": DataTypeByNum[i.dtype],
                "autoId": i.auto_id,
                "isFunctionOut": i.is_function_output,
            }
            for i in result
        ]

    def flush(self, collectionName, timeout=None):
        try:
            target = getTargetCollection(collectionName)
            target.flush(timeout=timeout)
        except Exception as e:
            raise Exception(f"Flush collection error!{str(e)}")
        else:
            return f"Flush collection {collectionName} successfully!"

    def compact(self, collectionName, timeout=None):
        try:
            target = getTargetCollection(collectionName)
            compaction_id = target.compact(timeout=timeout)
        except Exception as e:
            raise Exception(f"Compact collection error!{str(e)}")
        else:
            return {"message": f"Compact collection {collectionName} successfully!", "compaction_id": compaction_id}

    def get_compaction_state(self, collectionName, compactionId, timeout=None):
        try:
            target = getTargetCollection(collectionName)
            state = target.get_compaction_state(compactionId, timeout=timeout)
        except Exception as e:
            raise Exception(f"Get compaction state error!{str(e)}")
        else:
            return state

    def wait_for_compaction_completed(self, collectionName, compactionId, timeout=None):
        try:
            target = getTargetCollection(collectionName)
            target.wait_for_compaction_completed(compactionId, timeout=timeout)
        except Exception as e:
            raise Exception(f"Wait for compaction completed error!{str(e)}")
        else:
            return f"Compaction {compactionId} completed!"

    def get_compaction_plans(self, collectionName, compactionId, timeout=None):
        try:
            target = getTargetCollection(collectionName)
            plans = target.get_compaction_plans(compactionId, timeout=timeout)
        except Exception as e:
            raise Exception(f"Get compaction plans error!{str(e)}")
        else:
            return plans

    def get_replicas(self, collectionName, timeout=None):
        try:
            target = getTargetCollection(collectionName)
            replicas = target.get_replicas(timeout=timeout)
        except Exception as e:
            raise Exception(f"Get replicas error!{str(e)}")
        else:
            return replicas

    def truncate(self, collectionName, timeout=None):
        try:
            utility.truncate_collection(collectionName, timeout=timeout)
        except Exception as e:
            raise Exception(f"Truncate collection error!{str(e)}")
        else:
            return f"Truncate collection {collectionName} successfully!"

    def load_state(self, collectionName, partitionName=None):
        try:
            state = utility.load_state(collectionName, partitionName)
        except Exception as e:
            raise Exception(f"Get load state error!{str(e)}")
        else:
            return state

    def wait_for_loading_complete(self, collectionName, partitionNames=None, timeout=None):
        try:
            utility.wait_for_loading_complete(collectionName, partitionNames, timeout=timeout)
        except Exception as e:
            raise Exception(f"Wait for loading complete error!{str(e)}")
        else:
            return f"Loading {collectionName} completed!"
