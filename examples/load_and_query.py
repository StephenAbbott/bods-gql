"""End-to-end example: Load BODS data into BigQuery and run GQL queries.

Prerequisites:
    1. pip install bods-gql
    2. Google Cloud credentials configured (gcloud auth application-default login)
    3. BigQuery Enterprise or Enterprise Plus edition (required for graph queries)

Usage:
    python examples/load_and_query.py --project YOUR_PROJECT --bods-file data.json
"""

from __future__ import annotations

import argparse
import sys

from bods_gql.converter.bigquery_loader import BigQueryLoader
from bods_gql.converter.mapper import map_statements
from bods_gql.converter.reader import read_statements
from bods_gql.queries import circular_ownership, corporate_groups, ubo_detection


def main():
    parser = argparse.ArgumentParser(description="Load BODS data and run GQL queries")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--dataset", default="bods_ownership", help="BigQuery dataset name")
    parser.add_argument("--bods-file", required=True, help="Path to BODS JSON/JSONL file")
    parser.add_argument("--location", default="US", help="BigQuery dataset location")
    args = parser.parse_args()

    # Step 1: Read and map BODS data
    print(f"Reading BODS file: {args.bods_file}")
    statements = read_statements(args.bods_file)
    result = map_statements(statements)
    print(f"Mapped: {len(result.entity_nodes)} entities, "
          f"{len(result.person_nodes)} persons, "
          f"{len(result.ownership_edges)} relationships")

    if result.errors:
        print(f"WARNING: {len(result.errors)} mapping errors")

    # Step 2: Load into BigQuery
    full_dataset = f"{args.project}.{args.dataset}"
    loader = BigQueryLoader(args.project, args.dataset)

    print(f"\nCreating dataset {full_dataset}...")
    loader.create_dataset_if_needed(args.location)

    print("Loading tables...")
    counts = loader.load_tables(result)
    for table, count in counts.items():
        print(f"  {table}: {count} rows")

    # Step 3: Create property graph
    print("\nCreating property graph...")
    loader.create_property_graph()
    print("Property graph 'OwnershipGraph' created.")

    # Step 4: Show example queries
    print("\n" + "=" * 60)
    print("EXAMPLE GQL QUERIES")
    print("=" * 60)

    print("\n--- Find top-level parents ---")
    query = corporate_groups.top_level_parents(full_dataset, limit=10)
    print(query)

    print("\n--- Detect circular ownership ---")
    query = circular_ownership.find_cycles(full_dataset)
    print(query)

    print("\n--- Find UBOs (pure GQL) ---")
    query = ubo_detection.find_ubos_gql(full_dataset)
    print(query)

    # Step 5: Run a query
    print("\n" + "=" * 60)
    print("RUNNING: Top-level parents query")
    print("=" * 60)
    try:
        query = corporate_groups.top_level_parents(full_dataset, limit=10)
        results = loader.run_gql_query(query)
        if results:
            for row in results:
                print(f"  {row.get('name', 'N/A')} "
                      f"({row.get('jurisdiction_code', 'N/A')}) - "
                      f"{row.get('subsidiary_count', 0)} subsidiaries")
        else:
            print("  No results (dataset may not have hierarchical ownership)")
    except Exception as e:
        print(f"  Query failed: {e}")
        print("  (Graph queries require BigQuery Enterprise edition)")


if __name__ == "__main__":
    main()
