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
    ):
        fieldList = []
        for field in fields:
            [fieldName, fieldType, *restData] = field.split(":")
            upperFieldType = fieldType.upper()
            if upperFieldType in [
                "BINARY_VECTOR",
                "FLOAT_VECTOR",
                "BFLOAT16_VECTOR",
                "FLOAT16_VECTOR",
            ]:
                fieldList.append(
                    FieldSchema(
                        name=fieldName,
                        dtype=DataType[upperFieldType],
                        dim=int(restData[0]),
                    )
                )
            elif upperFieldType == "VARCHAR":
                fieldList.append(
                    FieldSchema(
                        name=fieldName,
                        dtype=DataType[upperFieldType],
                        max_length=restData[0],
                    )
                )
            elif upperFieldType == "ARRAY":
                upperElementType = restData[1].upper()
                max_capacity = restData[0]
                maxLength = restData[2] if len(restData) == 3 else None
                fieldList.append(
                    FieldSchema(
                        name=fieldName,
                        dtype=DataType[upperFieldType],
                        element_type=DataType[upperElementType],
                        max_capacity=max_capacity,
                        max_length=maxLength,
                    )
                )

            else:
                fieldList.append(
                    FieldSchema(
                        name=fieldName,
                        dtype=DataType[upperFieldType],
                        description=restData[0],
                    )
                )
        schema = CollectionSchema(
            fields=fieldList,
            primary_field=primaryField,
            auto_id=autoId,
            description=description,
            _enable_dynamic_field=isDynamic,
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
            _dim = _params.get("dim")
            _params_desc = f"dim: {_dim}" if _dim else ""
            if fieldSchema.dtype == DataType.ARRAY:
                _max_length = _params.get("max_length")
                _element_type = fieldSchema.element_type
                _max_capacity = _params.get("max_capacity")
                _params_desc = (
                    f"max_capacity: {_max_capacity},element_type: {_element_type}"
                )
                _params_desc += f",max_length: {_max_length}" if _max_length else ""

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
