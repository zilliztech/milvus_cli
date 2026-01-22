from Types import ParameterException
from Types import (
    FieldDataTypes,
    IndexTypes,
    IndexTypesMap,
    SearchParams,
    MetricTypes,
    Operators,
)
from Fs import readCsvFile


def validateParamsByCustomFunc(customFunc, errMsg, *params):
    try:
        customFunc(*params)
    except Exception as e:
        raise ParameterException(f"{errMsg}")


def validateCollectionParameter(collectionName, primaryField, fields):
    if not collectionName:
        raise ParameterException("Missing collection name.")
    if not primaryField:
        raise ParameterException("Missing primary field.")
    if not fields:
        raise ParameterException("Missing fields.")
    fieldNames = []
    for field in fields:
        fieldList = field.split(":")
        if len(fieldList) < 3:
            raise ParameterException(
                'Field should contain three parameters(array field need more) concatenated by ":".'
            )
        [fieldName, fieldType, *restData] = fieldList
        upperFieldType = fieldType.upper()
        fieldNames.append(fieldName)
        if upperFieldType not in FieldDataTypes:
            raise ParameterException(
                "Invalid field data type, should be one of {}".format(
                    str(FieldDataTypes)
                )
            )
        if upperFieldType in [
            "BINARY_VECTOR",
            "FLOAT_VECTOR",
            "FLOAT16_VECTOR",
            "BFLOAT16_VECTOR",
        ]:
            try:
                int(restData[0])
            except ValueError as e:
                raise ParameterException(
                    """{} Vector's dim should be int.""".format(upperFieldType)
                )
    # Dedup field name.
    newNames = list(set(fieldNames))
    if not (len(newNames) == len(fieldNames)):
        raise ParameterException("Field names are duplicated.")
    if primaryField not in fieldNames:
        raise ParameterException(
            """Primary field name doesn't exist in input fields."""
        )


def validateIndexParameter(indexType, metricType, params):
    if indexType not in IndexTypes:
        raise ParameterException(
            "Invalid index type, should be one of {}".format(str(IndexTypes))
        )
    if metricType not in MetricTypes:
        raise ParameterException(
            "Invalid index metric type, should be one of {}".format(str(MetricTypes))
        )
    # if not params:
    #     raise ParameterException('Missing params')
    paramNames = []
    buildingParameters = IndexTypesMap[indexType]["index_building_parameters"]
    for param in params:
        paramList = param.split(":")
        if not (len(paramList) == 2):
            raise ParameterException(
                'Params should contain two parameters concatenated by ":".'
            )
        [paramName, paramValue] = paramList
        paramNames.append(paramName)
        if paramName not in buildingParameters:
            raise ParameterException(
                "Invalid index param, should be one of {}".format(
                    str(buildingParameters)
                )
            )
        try:
            int(paramValue)
        except ValueError as e:
            raise ParameterException("""Index param's value should be int.""")
    # Dedup field name.
    newNames = list(set(paramNames))
    if not (len(newNames) == len(paramNames)):
        raise ParameterException("Index params are duplicated.")


def validateSearchParams(
    data,
    annsField,
    metricType,
    params,
    limit,
    expr,
    outputFields,
    roundDecimal,
    hasIndex=True,
    guarantee_timestamp=None,
    partitionNames=None,
):
    import json

    result = {}
    # Validate data
    try:
        if ".csv" in data:
            csvData = readCsvFile(data, withCol=False)
            result["data"] = csvData["data"][0]
        else:
            result["data"] = data
    except Exception as e:
        raise ParameterException(
            'Format(list[list[float]]) "Data" error! {}'.format(str(e))
        )
    # Validate annsField
    if not annsField:
        raise ParameterException("annsField is empty!")
    result["anns_field"] = annsField
    if hasIndex:
        # Validate metricType
        if metricType not in MetricTypes:
            raise ParameterException(
                "Invalid index metric type, should be one of {}".format(
                    str(MetricTypes)
                )
            )
        # Validate params
        paramDict = {}
        if type(params) == str:
            paramsList = params.replace(" ", "").split(",")
        else:
            paramsList = params
        for param in paramsList:
            if not param:
                continue
            paramList = param.split(":")
            if not (len(paramList) == 2):
                raise ParameterException(
                    'Params should contain two parameters concatenated by ":".'
                )
            [paramName, paramValue] = paramList
            if paramName not in SearchParams:
                raise ParameterException(
                    "Invalid search parameter, should be one of {}".format(
                        str(SearchParams)
                    )
                )
            try:
                if paramName != "group_by_field":
                    paramDict[paramName] = int(paramValue)
            except ValueError as e:
                raise ParameterException("""Search parameter's value should be int.""")
        result["param"] = {"metric_type": metricType}

        if paramDict.keys():
            result["param"]["params"] = paramDict
    else:
        result["param"] = {}
    #  Validate limit
    try:
        result["limit"] = int(limit)
    except Exception as e:
        raise ParameterException('Format(int) "limit" error! {}'.format(str(e)))
    # Validate expr
    result["expr"] = expr if expr else None
    if roundDecimal:
        result["round_decimal"] = int(roundDecimal)
    #  Validate guarantee_timestamp and travel_timestamp
    if guarantee_timestamp:
        try:
            result["guarantee_timestamp"] = int(guarantee_timestamp)
        except Exception as e:
            raise ParameterException(
                'Format(int) "guarantee_timestamp" error! {}'.format(str(e))
            )
    if not outputFields:
        result["output_fields"] = None
    else:
        # Clean up field names - strip brackets, quotes, and whitespace
        raw_list = outputFields.replace(" ", "").split(",")
        nameList = []
        for name in raw_list:
            # Strip brackets and quotes that user might have entered
            name = name.strip().strip("[]'\"")
            if name:
                nameList.append(name)
        result["output_fields"] = nameList if nameList else None
    if not partitionNames:
        result["partition_names"] = None
    else:
        nameList = partitionNames.replace(" ", "").split(",")
        result["partition_names"] = nameList
    return result


def validateQueryParams(
    expr, outputFields, timeout, guarantee_timestamp, graceful_time, partitionNames=None
):
    result = {}
    if not expr:
        raise ParameterException("expr is empty!")
    # if ' in ' not in expr:
    if not any(map(lambda x: x in expr, Operators)):
        raise ParameterException(
            f'The query expression only accepts "<field_name> <oprator in {Operators}> [<value>, ...]"!'
        )
    result["expr"] = expr
    if not outputFields:
        result["output_fields"] = None
    else:
        # Clean up field names - strip brackets, quotes, and whitespace
        raw_list = outputFields.replace(" ", "").split(",")
        nameList = []
        for name in raw_list:
            # Strip brackets and quotes that user might have entered
            name = name.strip().strip("[]'\"")
            if name:
                nameList.append(name)
        result["output_fields"] = nameList if nameList else None
    if not partitionNames:
        result["partition_names"] = None
    else:
        nameList = partitionNames.replace(" ", "").split(",")
        result["partition_names"] = nameList
    result["timeout"] = float(timeout) if timeout else None
    if guarantee_timestamp:
        result["guarantee_timestamp"] = guarantee_timestamp
    if graceful_time and (graceful_time != 5):
        result["graceful_time"] = graceful_time

    return result


def validateVectorMeta(vectorMeta):
    import json

    vec_type = vectorMeta["vec_type"]
    result = {}
    if vec_type == "import":
        ids_str = vectorMeta["ids"]
        try:
            ids = json.loads(ids_str.replace("'", "").replace('"', ""))
        except Exception as e:
            raise ParameterException('Format(list[int]) "ids" error! {}'.format(str(e)))
        collection_str = vectorMeta["collection"]
        partition_str = vectorMeta["partition"]
        field_str = vectorMeta["field"]
        if not collection_str or not partition_str or not field_str:
            raise ParameterException(f"Collection/Partition/Field should not be empty!")
        result["ids"] = ids
        result["collection"] = collection_str
        result["partition"] = partition_str
        result["field"] = field_str
    if vec_type == "raw":
        vectors_str = vectorMeta["vectors"]
        if vectorMeta["type"] == "float_vectors":
            try:
                vectors = json.loads(vectors_str.replace("'", "").replace('"', ""))
            except Exception as e:
                raise ParameterException(
                    'Format(list[float]) "ids" error! {}'.format(str(e))
                )
            else:
                result[vectorMeta["type"]] = vectors
        elif vectorMeta["type"] == "bin_vectors":
            # """[b'\x94', b'N', b'\xca']"""
            noWhiteSpace = vectors_str.strip()
            noSquareBrackets = noWhiteSpace[1:-1]
            # "b'\x94', b'N', b'Ê'"
            strList = noSquareBrackets.split(", ")
            binMap = map(lambda x: x[2:-1].encode("unicode_escape"), strList)
            result[vectorMeta["type"]] = list(binMap)
    return result
