#!/usr/bin/env python3
"""
Test script for Hybrid Search and Query Iterator functionality.

This script tests:
1. Query Iterator - iterate through query results in batches
2. Search Iterator - iterate through search results in batches
3. Hybrid Search - multi-vector search with reranking (Weighted/RRF)

Prerequisites:
- Milvus server running on localhost:19530
- pymilvus installed

Usage:
    python tests/test_hybrid_search_query_iterator.py
"""

import random
import numpy as np
from pymilvus import (
    connections,
    Collection,
    FieldSchema,
    CollectionSchema,
    DataType,
    utility,
    MilvusClient,
    AnnSearchRequest,
    WeightedRanker,
    RRFRanker,
)

# Configuration
COLLECTION_NAME = "test_hybrid_search_iterator"
DIM_DENSE = 128  # Dense vector dimension
DIM_SPARSE = 64  # For demonstration, using smaller sparse vectors
NUM_ENTITIES = 10000
BATCH_SIZE = 1000

# Milvus connection settings
MILVUS_HOST = "localhost"
MILVUS_PORT = "19530"
MILVUS_URI = f"http://{MILVUS_HOST}:{MILVUS_PORT}"


def create_collection():
    """Create a collection with multiple vector fields for hybrid search testing."""
    print(f"\n{'='*60}")
    print("Creating collection with multiple vector fields...")
    print(f"{'='*60}")

    # Drop existing collection if exists
    if utility.has_collection(COLLECTION_NAME):
        print(f"Dropping existing collection: {COLLECTION_NAME}")
        utility.drop_collection(COLLECTION_NAME)

    # Define schema with multiple vector fields
    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=False),
        FieldSchema(name="category", dtype=DataType.VARCHAR, max_length=100),
        FieldSchema(name="price", dtype=DataType.FLOAT),
        FieldSchema(name="dense_vector", dtype=DataType.FLOAT_VECTOR, dim=DIM_DENSE),
        FieldSchema(name="dense_vector_2", dtype=DataType.FLOAT_VECTOR, dim=DIM_DENSE),
    ]

    schema = CollectionSchema(
        fields=fields,
        description="Test collection for hybrid search and query iterator",
        enable_dynamic_field=True
    )

    collection = Collection(name=COLLECTION_NAME, schema=schema)
    print(f"Collection '{COLLECTION_NAME}' created successfully!")
    print(f"Schema: {[f.name for f in fields]}")

    return collection


def create_indexes(collection: Collection):
    """Create indexes on vector fields."""
    print(f"\n{'='*60}")
    print("Creating indexes on vector fields...")
    print(f"{'='*60}")

    # Index for dense_vector
    index_params_dense = {
        "metric_type": "L2",
        "index_type": "IVF_FLAT",
        "params": {"nlist": 128}
    }

    collection.create_index(
        field_name="dense_vector",
        index_params=index_params_dense,
        index_name="idx_dense_vector"
    )
    print("Index created for 'dense_vector'")

    # Index for dense_vector_2
    collection.create_index(
        field_name="dense_vector_2",
        index_params=index_params_dense,
        index_name="idx_dense_vector_2"
    )
    print("Index created for 'dense_vector_2'")

    print("All indexes created successfully!")


def insert_data(collection: Collection):
    """Insert test data into the collection."""
    print(f"\n{'='*60}")
    print(f"Inserting {NUM_ENTITIES} entities...")
    print(f"{'='*60}")

    categories = ["electronics", "clothing", "books", "home", "sports"]

    # Insert in batches
    total_inserted = 0
    batch_num = 0

    for start_id in range(0, NUM_ENTITIES, BATCH_SIZE):
        end_id = min(start_id + BATCH_SIZE, NUM_ENTITIES)
        batch_size = end_id - start_id

        data = [
            list(range(start_id, end_id)),  # id
            [random.choice(categories) for _ in range(batch_size)],  # category
            [random.uniform(10.0, 1000.0) for _ in range(batch_size)],  # price
            [[random.random() for _ in range(DIM_DENSE)] for _ in range(batch_size)],  # dense_vector
            [[random.random() for _ in range(DIM_DENSE)] for _ in range(batch_size)],  # dense_vector_2
        ]

        collection.insert(data)
        total_inserted += batch_size
        batch_num += 1
        print(f"Batch {batch_num}: Inserted {batch_size} entities (Total: {total_inserted})")

    collection.flush()
    print(f"\nTotal entities inserted: {total_inserted}")
    print(f"Collection row count: {collection.num_entities}")


def load_collection(collection: Collection):
    """Load the collection into memory."""
    print(f"\n{'='*60}")
    print("Loading collection into memory...")
    print(f"{'='*60}")

    collection.load()
    print("Collection loaded successfully!")


def run_query_iterator_test(collection: Collection):
    """Test query iterator functionality."""
    print(f"\n{'='*60}")
    print("TEST: Query Iterator")
    print(f"{'='*60}")

    # Test 1: Basic query iterator
    print("\n--- Test 1: Basic Query Iterator ---")
    expr = "price > 500"
    output_fields = ["id", "category", "price"]
    batch_size = 500
    limit = 2000

    print(f"Filter: {expr}")
    print(f"Output fields: {output_fields}")
    print(f"Batch size: {batch_size}")
    print(f"Limit: {limit}")

    iterator = collection.query_iterator(
        expr=expr,
        output_fields=output_fields,
        batch_size=batch_size,
        limit=limit,
    )

    total_count = 0
    batch_count = 0

    while True:
        result = iterator.next()
        if not result:
            break
        batch_count += 1
        total_count += len(result)
        print(f"  Batch {batch_count}: {len(result)} results (Total: {total_count})")

        # Show first few results from first batch
        if batch_count == 1:
            print(f"  Sample results from batch 1:")
            for i, item in enumerate(result[:3]):
                print(f"    [{i}] id={item['id']}, category={item['category']}, price={item['price']:.2f}")

    iterator.close()
    print(f"\nQuery iterator completed: {total_count} total results in {batch_count} batches")

    # Test 2: Query iterator with category filter
    print("\n--- Test 2: Query Iterator with Category Filter ---")
    expr = "category == 'electronics' and price > 200"

    print(f"Filter: {expr}")

    iterator = collection.query_iterator(
        expr=expr,
        output_fields=["id", "category", "price"],
        batch_size=1000,
        limit=0,  # No limit
    )

    total_count = 0
    batch_count = 0

    while True:
        result = iterator.next()
        if not result:
            break
        batch_count += 1
        total_count += len(result)

    iterator.close()
    print(f"Found {total_count} electronics items with price > 200 in {batch_count} batches")

    return True


def run_search_iterator_test(collection: Collection):
    """Test search iterator functionality."""
    print(f"\n{'='*60}")
    print("TEST: Search Iterator")
    print(f"{'='*60}")

    # Generate random query vector
    query_vector = [random.random() for _ in range(DIM_DENSE)]

    # Test 1: Basic search iterator
    print("\n--- Test 1: Basic Search Iterator ---")

    search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
    batch_size = 500
    limit = 2000

    print(f"Vector field: dense_vector")
    print(f"Batch size: {batch_size}")
    print(f"Limit: {limit}")

    iterator = collection.search_iterator(
        data=[query_vector],
        anns_field="dense_vector",
        param=search_params,
        batch_size=batch_size,
        limit=limit,
        output_fields=["id", "category", "price"],
    )

    total_count = 0
    batch_count = 0

    while True:
        result = iterator.next()
        if not result:
            break
        batch_count += 1
        total_count += len(result)
        print(f"  Batch {batch_count}: {len(result)} results (Total: {total_count})")

        # Show first few results from first batch
        if batch_count == 1:
            print(f"  Sample results from batch 1:")
            for i, hit in enumerate(result[:3]):
                print(f"    [{i}] id={hit.id}, distance={hit.distance:.4f}, category={hit.entity.get('category')}")

    iterator.close()
    print(f"\nSearch iterator completed: {total_count} total results in {batch_count} batches")

    # Test 2: Search iterator with filter
    print("\n--- Test 2: Search Iterator with Filter ---")
    expr = "category == 'books'"

    print(f"Filter: {expr}")

    iterator = collection.search_iterator(
        data=[query_vector],
        anns_field="dense_vector",
        param=search_params,
        batch_size=500,
        limit=1000,
        expr=expr,
        output_fields=["id", "category", "price"],
    )

    total_count = 0
    batch_count = 0

    while True:
        result = iterator.next()
        if not result:
            break
        batch_count += 1
        total_count += len(result)

    iterator.close()
    print(f"Found {total_count} 'books' items in {batch_count} batches")

    return True


def run_hybrid_search_test(collection: Collection):
    """Test hybrid search with multiple vector fields."""
    print(f"\n{'='*60}")
    print("TEST: Hybrid Search")
    print(f"{'='*60}")

    # Generate random query vectors
    query_vector_1 = [random.random() for _ in range(DIM_DENSE)]
    query_vector_2 = [random.random() for _ in range(DIM_DENSE)]

    # Test 1: Hybrid search with Weighted Ranker
    print("\n--- Test 1: Hybrid Search with Weighted Ranker ---")

    search_params = {"metric_type": "L2", "params": {"nprobe": 10}}

    # Create ANN search requests for each vector field
    request_1 = AnnSearchRequest(
        data=[query_vector_1],
        anns_field="dense_vector",
        param=search_params,
        limit=100,
    )

    request_2 = AnnSearchRequest(
        data=[query_vector_2],
        anns_field="dense_vector_2",
        param=search_params,
        limit=100,
    )

    # Weighted ranker with different weights
    weights = [0.7, 0.3]
    rerank = WeightedRanker(*weights)

    print(f"Request 1: dense_vector, limit=100")
    print(f"Request 2: dense_vector_2, limit=100")
    print(f"Reranker: WeightedRanker with weights {weights}")

    results = collection.hybrid_search(
        reqs=[request_1, request_2],
        rerank=rerank,
        limit=10,
        output_fields=["id", "category", "price"],
    )

    print(f"\nTop 10 results with Weighted Ranker:")
    for i, hit in enumerate(results[0]):
        print(f"  [{i+1}] id={hit.id}, distance={hit.distance:.4f}, category={hit.entity.get('category')}, price={hit.entity.get('price'):.2f}")

    # Test 2: Hybrid search with RRF Ranker
    print("\n--- Test 2: Hybrid Search with RRF Ranker ---")

    rrf_k = 60
    rerank_rrf = RRFRanker(k=rrf_k)

    print(f"Reranker: RRFRanker with k={rrf_k}")

    results = collection.hybrid_search(
        reqs=[request_1, request_2],
        rerank=rerank_rrf,
        limit=10,
        output_fields=["id", "category", "price"],
    )

    print(f"\nTop 10 results with RRF Ranker:")
    for i, hit in enumerate(results[0]):
        print(f"  [{i+1}] id={hit.id}, distance={hit.distance:.4f}, category={hit.entity.get('category')}, price={hit.entity.get('price'):.2f}")

    # Test 3: Hybrid search with filter expression
    print("\n--- Test 3: Hybrid Search with Filter Expression ---")

    expr = "price > 500 and category in ['electronics', 'sports']"
    print(f"Filter: {expr}")

    request_1_filtered = AnnSearchRequest(
        data=[query_vector_1],
        anns_field="dense_vector",
        param=search_params,
        limit=100,
        expr=expr,
    )

    request_2_filtered = AnnSearchRequest(
        data=[query_vector_2],
        anns_field="dense_vector_2",
        param=search_params,
        limit=100,
        expr=expr,
    )

    results = collection.hybrid_search(
        reqs=[request_1_filtered, request_2_filtered],
        rerank=WeightedRanker(0.5, 0.5),
        limit=10,
        output_fields=["id", "category", "price"],
    )

    print(f"\nTop 10 filtered results:")
    for i, hit in enumerate(results[0]):
        print(f"  [{i+1}] id={hit.id}, distance={hit.distance:.4f}, category={hit.entity.get('category')}, price={hit.entity.get('price'):.2f}")

    return True


def run_hybrid_search_with_milvus_client_test():
    """Test hybrid search using MilvusClient API."""
    print(f"\n{'='*60}")
    print("TEST: Hybrid Search with MilvusClient API")
    print(f"{'='*60}")

    client = MilvusClient(uri=MILVUS_URI)

    # Generate random query vectors
    query_vector_1 = [random.random() for _ in range(DIM_DENSE)]
    query_vector_2 = [random.random() for _ in range(DIM_DENSE)]

    search_params = {"metric_type": "L2", "params": {"nprobe": 10}}

    # Create ANN search requests
    request_1 = AnnSearchRequest(
        data=[query_vector_1],
        anns_field="dense_vector",
        param=search_params,
        limit=100,
    )

    request_2 = AnnSearchRequest(
        data=[query_vector_2],
        anns_field="dense_vector_2",
        param=search_params,
        limit=100,
    )

    # Test with WeightedRanker
    print("\n--- MilvusClient Hybrid Search ---")

    results = client.hybrid_search(
        collection_name=COLLECTION_NAME,
        reqs=[request_1, request_2],
        ranker=WeightedRanker(0.6, 0.4),
        limit=10,
        output_fields=["id", "category", "price"],
    )

    print(f"Top 10 results:")
    for i, hit in enumerate(results[0]):
        print(f"  [{i+1}] id={hit['id']}, distance={hit['distance']:.4f}, category={hit['entity'].get('category')}")

    client.close()
    return True


def cleanup(collection: Collection):
    """Clean up test resources."""
    print(f"\n{'='*60}")
    print("Cleaning up...")
    print(f"{'='*60}")

    collection.release()
    utility.drop_collection(COLLECTION_NAME)
    print(f"Collection '{COLLECTION_NAME}' dropped.")


def main():
    """Main test execution."""
    print("="*60)
    print("Hybrid Search and Query Iterator Test Script")
    print("="*60)

    # Connect to Milvus
    print(f"\nConnecting to Milvus at {MILVUS_HOST}:{MILVUS_PORT}...")
    connections.connect(host=MILVUS_HOST, port=MILVUS_PORT)
    print("Connected successfully!")

    collection = None

    try:
        # Setup
        collection = create_collection()
        create_indexes(collection)
        insert_data(collection)
        load_collection(collection)

        # Run tests
        results = {}

        results["Query Iterator"] = run_query_iterator_test(collection)
        results["Search Iterator"] = run_search_iterator_test(collection)
        results["Hybrid Search (ORM)"] = run_hybrid_search_test(collection)
        results["Hybrid Search (MilvusClient)"] = run_hybrid_search_with_milvus_client_test()

        # Summary
        print(f"\n{'='*60}")
        print("TEST SUMMARY")
        print(f"{'='*60}")
        for test_name, passed in results.items():
            status = "PASSED" if passed else "FAILED"
            print(f"  {test_name}: {status}")

        all_passed = all(results.values())
        print(f"\nOverall: {'ALL TESTS PASSED' if all_passed else 'SOME TESTS FAILED'}")

    except Exception as e:
        print(f"\nError during test execution: {e}")
        import traceback
        traceback.print_exc()

    finally:
        # Cleanup
        if collection:
            cleanup(collection)

        connections.disconnect("default")
        print("\nDisconnected from Milvus.")


if __name__ == "__main__":
    main()
