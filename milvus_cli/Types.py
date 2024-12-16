from functools import reduce


class ParameterException(Exception):
    "Custom Exception for parameters checking."

    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return str(self.msg)


class ConnectException(Exception):
    "Custom Exception for milvus connection."

    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return str(self.msg)


FiledDataTypes = [
    "BOOL",
    "INT8",
    "INT16",
    "INT32",
    "INT64",
    "FLOAT",
    "DOUBLE",
    "VARCHAR",
    "BINARY_VECTOR",
    "FLOAT_VECTOR",
    "FLOAT16_VECTOR",
    "BFLOAT16_VECTOR",
    "SPARSE_FLOAT_VECTOR",
    "JSON",
    "ARRAY",
]

IndexTypes = [
    "FLAT",
    "IVF_FLAT",
    "IVF_SQ8",
    "IVF_PQ",
    "RNSG",
    "HNSW",
    "ANNOY",
    "AUTOINDEX",
    "DISKANN",
    "GPU_IVF_FLAT",
    "GPU_IVF_PQ",
    "SPARSE_INVERTED_INDEX",
    "SPARSE_WAND",
    "SCANN",
    "STL_SORT",
    "Trie",
    "INVERTED",
    "",
]

IndexParams = [
    "nlist",
    "m",
    "nbits",
    "M",
    "efConstruction",
    "n_trees",
    "PQM",
]

IndexTypesMap = {
    "FLAT": {
        "index_building_parameters": [],
        "search_parameters": ["metric_type"],
    },
    "IVF_FLAT": {
        "index_building_parameters": ["nlist"],
        "search_parameters": ["nprobe"],
    },
    "IVF_SQ8": {
        "index_building_parameters": ["nlist"],
        "search_parameters": ["nprobe"],
    },
    "IVF_PQ": {
        "index_building_parameters": ["nlist", "m", "nbits"],
        "search_parameters": ["nprobe"],
    },
    "RNSG": {
        "index_building_parameters": [
            "out_degree",
            "candidate_pool_size",
            "search_length",
            "knng",
        ],
        "search_parameters": ["search_length"],
    },
    "HNSW": {
        "index_building_parameters": ["M", "efConstruction"],
        "search_parameters": ["ef"],
    },
    "ANNOY": {
        "index_building_parameters": ["n_trees"],
        "search_parameters": ["search_k"],
    },
    "AUTOINDEX": {
        "index_building_parameters": [],
        "search_parameters": [],
    },
    "DISKANN": {
        "index_building_parameters": [],
        "search_parameters": [],
    },
    "GPU_IVF_FLAT": {
        "index_building_parameters": ["nlist"],
        "search_parameters": ["nprobe"],
    },
    "GPU_IVF_PQ": {
        "index_building_parameters": ["nlist", "m", "nbits"],
        "search_parameters": ["nprobe"],
    },
    "SCANN": {
        "index_building_parameters": ["nlist", "with_raw_data"],
        "search_parameters": ["nprobe", "reorder_k"],
    },
    "SPARSE_INVERTED_INDEX": {
        "index_building_parameters": ["drop_ratio_build"],
        "search_parameters": ["drop_ratio_search"],
    },
    "SPARSE_WAND": {
        "index_building_parameters": ["drop_ratio_build"],
        "search_parameters": ["drop_ratio_search"],
    },
}

DupSearchParams = reduce(
    lambda x, y: x + IndexTypesMap[y]["search_parameters"], IndexTypesMap.keys(), []
)
SearchParams = list(dict.fromkeys(DupSearchParams))
SearchParams.append("group_by_field")

MetricTypes = [
    "L2",
    "IP",
    "HAMMING",
    "TANIMOTO",
    "COSINE",
    "",
]

DataTypeByNum = {
    0: "NONE",
    1: "BOOL",
    2: "INT8",
    3: "INT16",
    4: "INT32",
    5: "INT64",
    10: "FLOAT",
    11: "DOUBLE",
    20: "STRING",
    21: "VARCHAR",
    22: "ARRAY",
    23: "JSON",
    100: "BINARY_VECTOR",
    101: "FLOAT_VECTOR",
    102: "FLOAT16_VECTOR",
    103: "BFLOAT16_VECTOR",
    104: "SPARSE_FLOAT_VECTOR",
    999: "UNKNOWN",
}

Operators = ["<", "<=", ">", ">=", "==", "!=", "in"]

Privileges = [
    "CreateIndex",
    "DropIndex",
    "IndexDetail",
    "Load",
    "GetLoadingProgress",
    "GetLoadState",
    "Release",
    "Insert",
    "Delete",
    "Upsert",
    "Search",
    "Flush",
    "GetFlushState",
    "Query",
    "GetStatistics",
    "Compaction",
    "Import",
    "LoadBalance",
    "CreatePartition",
    "DropPartition",
    "ShowPartitions",
    "HasPartition",
    "All",
    "CreateCollection",
    "DropCollection",
    "DescribeCollection",
    "ShowCollections",
    "RenameCollection",
    "FlushAll",
    "CreateOwnership",
    "DropOwnership",
    "SelectOwnership",
    "ManageOwnership",
    "CreateResourceGroup",
    "DropResourceGroup",
    "DescribeResourceGroup",
    "ListResourceGroups",
    "TransferNode",
    "TransferReplica",
    "CreateDatabase",
    "DropDatabase",
    "ListDatabases",
    "CreateAlias",
    "DropAlias",
    "DescribeAlias",
    "ListAliases",
    "UpdateUser",
    "SelectUser",
    "*",
]
