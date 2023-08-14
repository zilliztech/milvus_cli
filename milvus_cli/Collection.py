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
from milvus_cli.Types import DataTypeByNum


class MilvusCollection(object):
    alias = "default"

    def create_collection(
        self,
        collectionName,
        primaryField,
        fields,
        autoId=None,
        description=None,
        isDynamic=None,
        consistencyLevel=None,
        shardsNum=2,
        alias=None,
    ):
        fieldList = []
        for field in fields:
            [fieldName, fieldType, fieldData] = field.split(":")
            upperFieldType = fieldType.upper()
            if upperFieldType in ["BINARY_VECTOR", "FLOAT_VECTOR"]:
                fieldList.append(
                    FieldSchema(
                        name=fieldName,
                        dtype=DataType[upperFieldType],
                        dim=int(fieldData),
                    )
                )
            elif upperFieldType == "VARCHAR":
                fieldList.append(
                    FieldSchema(
                        name=fieldName,
                        dtype=DataType[upperFieldType],
                        max_length=fieldData,
                    )
                )
            else:
                fieldList.append(
                    FieldSchema(
                        name=fieldName,
                        dtype=DataType[upperFieldType],
                        description=fieldData,
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
            using=alias if alias else self.alias,
        )
        return self.get_collection_details(collection=collection)

    def list_collections(self, alias=None):
        tempAlias = alias if alias else self.alias
        try:
            res = list_collections(using=tempAlias)
        except Exception as e:
            raise Exception(f"List collection error!{str(e)}")
        else:
            return res

    def getTargetCollection(self, collectionName, alias=None):
        try:
            tempAlias = alias if alias else self.alias
            target = Collection(collectionName, using=tempAlias)
        except Exception as e:
            raise Exception(f"Get collection error!{str(e)}")
        else:
            return target

    def drop_collection(self, alias=None, collectionName=None):
        try:
            target = self.getTargetCollection(
                collectionName=collectionName, alias=alias
            )
            target.drop()
        except Exception as e:
            raise Exception(f"Delete collection error!{str(e)}")
        else:
            return f"Drop collection {collectionName} successfully!"

    def has_collection(self, alias=None, collectionName=None):
        try:
            res = has_collection(
                collection_name=collectionName, using=alias if alias else self.alias
            )
        except Exception as e:
            raise Exception(f"Has collection error!{str(e)}")
        else:
            return res

    def rename_collection(self, alias=None, collectionName=None, newName=None):
        try:
            utility.rename_collection(
                old_collection_name=collectionName,
                new_collection_name=newName,
                using=alias if alias else self.alias,
            )
        except Exception as e:
            raise Exception(f"Rename collection error!{str(e)}")
        else:
            return f"Rename collection {collectionName} to {newName} successfully!"

    def get_collection_details(self, collectionName="", alias=None, collection=None):
        try:
            target = collection or self.getTargetCollection(
                collectionName=collectionName, alias=alias
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