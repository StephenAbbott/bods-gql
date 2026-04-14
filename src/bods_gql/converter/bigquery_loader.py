"""Load BODS node/edge tables into BigQuery and create property graph.

Handles the full pipeline: BODS JSON/JSONL -> mapped tables -> BigQuery
tables -> CREATE PROPERTY GRAPH.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from google.cloud import bigquery

from bods_gql.converter.mapper import MappingResult
from bods_gql.graph_schema.property_graph import (
    generate_create_graph_ddl,
    generate_drop_graph_ddl,
)

if TYPE_CHECKING:
    pass


ENTITY_SCHEMA = [
    bigquery.SchemaField("record_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("statement_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("statement_date", "STRING"),
    bigquery.SchemaField("record_status", "STRING"),
    bigquery.SchemaField("declaration_subject", "STRING"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("entity_type", "STRING"),
    bigquery.SchemaField("entity_subtype", "STRING"),
    bigquery.SchemaField("entity_type_details", "STRING"),
    bigquery.SchemaField("is_component", "BOOLEAN"),
    bigquery.SchemaField("jurisdiction_name", "STRING"),
    bigquery.SchemaField("jurisdiction_code", "STRING"),
    bigquery.SchemaField("founding_date", "STRING"),
    bigquery.SchemaField("dissolution_date", "STRING"),
    bigquery.SchemaField("uri", "STRING"),
    bigquery.SchemaField("primary_identifier_id", "STRING"),
    bigquery.SchemaField("primary_identifier_scheme", "STRING"),
    bigquery.SchemaField("registered_address", "STRING"),
    bigquery.SchemaField("registered_country", "STRING"),
    bigquery.SchemaField("publisher_name", "STRING"),
    bigquery.SchemaField("publication_date", "STRING"),
    bigquery.SchemaField("bods_version", "STRING"),
    bigquery.SchemaField("identifiers_json", "STRING"),
    bigquery.SchemaField("addresses_json", "STRING"),
    bigquery.SchemaField("alternate_names_json", "STRING"),
    bigquery.SchemaField("source_json", "STRING"),
    bigquery.SchemaField("annotations_json", "STRING"),
    bigquery.SchemaField("node_label", "STRING"),
]

PERSON_SCHEMA = [
    bigquery.SchemaField("record_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("statement_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("statement_date", "STRING"),
    bigquery.SchemaField("record_status", "STRING"),
    bigquery.SchemaField("declaration_subject", "STRING"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("family_name", "STRING"),
    bigquery.SchemaField("given_name", "STRING"),
    bigquery.SchemaField("person_type", "STRING"),
    bigquery.SchemaField("is_component", "BOOLEAN"),
    bigquery.SchemaField("birth_date", "STRING"),
    bigquery.SchemaField("death_date", "STRING"),
    bigquery.SchemaField("nationality_code", "STRING"),
    bigquery.SchemaField("pep_status", "STRING"),
    bigquery.SchemaField("publisher_name", "STRING"),
    bigquery.SchemaField("publication_date", "STRING"),
    bigquery.SchemaField("bods_version", "STRING"),
    bigquery.SchemaField("names_json", "STRING"),
    bigquery.SchemaField("identifiers_json", "STRING"),
    bigquery.SchemaField("addresses_json", "STRING"),
    bigquery.SchemaField("nationalities_json", "STRING"),
    bigquery.SchemaField("political_exposure_json", "STRING"),
    bigquery.SchemaField("source_json", "STRING"),
    bigquery.SchemaField("annotations_json", "STRING"),
    bigquery.SchemaField("node_label", "STRING"),
]

EDGE_SCHEMA = [
    bigquery.SchemaField("record_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("statement_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("interested_party", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("subject", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("statement_date", "STRING"),
    bigquery.SchemaField("record_status", "STRING"),
    bigquery.SchemaField("declaration_subject", "STRING"),
    bigquery.SchemaField("is_component", "BOOLEAN"),
    bigquery.SchemaField("interest_types", "STRING"),
    bigquery.SchemaField("is_beneficial_ownership", "BOOLEAN"),
    bigquery.SchemaField("direct_or_indirect", "STRING"),
    bigquery.SchemaField("share_exact", "FLOAT64"),
    bigquery.SchemaField("share_minimum", "FLOAT64"),
    bigquery.SchemaField("share_maximum", "FLOAT64"),
    bigquery.SchemaField("share_exclusive_minimum", "FLOAT64"),
    bigquery.SchemaField("share_exclusive_maximum", "FLOAT64"),
    bigquery.SchemaField("interest_start_date", "STRING"),
    bigquery.SchemaField("interest_end_date", "STRING"),
    bigquery.SchemaField("publisher_name", "STRING"),
    bigquery.SchemaField("publication_date", "STRING"),
    bigquery.SchemaField("bods_version", "STRING"),
    bigquery.SchemaField("interests_json", "STRING"),
    bigquery.SchemaField("component_records_json", "STRING"),
    bigquery.SchemaField("source_json", "STRING"),
    bigquery.SchemaField("annotations_json", "STRING"),
    bigquery.SchemaField("subject_unspecified_json", "STRING"),
    bigquery.SchemaField("interested_party_unspecified_json", "STRING"),
    bigquery.SchemaField("edge_label", "STRING"),
]


class BigQueryLoader:
    """Load BODS data into BigQuery and create a property graph."""

    def __init__(self, project: str, dataset: str):
        self.project = project
        self.dataset = dataset
        self.full_dataset = f"{project}.{dataset}"
        self.client = bigquery.Client(project=project)

    def create_dataset_if_needed(self, location: str = "US") -> None:
        """Create the BigQuery dataset if it doesn't exist."""
        dataset_ref = bigquery.Dataset(self.full_dataset)
        dataset_ref.location = location
        self.client.create_dataset(dataset_ref, exists_ok=True)

    def load_tables(
        self,
        mapping_result: MappingResult,
        entity_table: str = "entity_nodes",
        person_table: str = "person_nodes",
        edge_table: str = "ownership_edges",
    ) -> dict[str, int]:
        """Load mapped BODS data into BigQuery tables.

        Returns dict with row counts per table.
        """
        counts = {}

        # Load entity nodes
        if mapping_result.entity_nodes:
            rows = [node.to_dict() for node in mapping_result.entity_nodes]
            table_ref = f"{self.full_dataset}.{entity_table}"
            job_config = bigquery.LoadJobConfig(
                schema=ENTITY_SCHEMA,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )
            job = self.client.load_table_from_json(rows, table_ref, job_config=job_config)
            job.result()
            counts["entity_nodes"] = len(rows)

        # Load person nodes
        if mapping_result.person_nodes:
            rows = [node.to_dict() for node in mapping_result.person_nodes]
            table_ref = f"{self.full_dataset}.{person_table}"
            job_config = bigquery.LoadJobConfig(
                schema=PERSON_SCHEMA,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )
            job = self.client.load_table_from_json(rows, table_ref, job_config=job_config)
            job.result()
            counts["person_nodes"] = len(rows)

        # Load ownership edges
        if mapping_result.ownership_edges:
            rows = [edge.to_dict() for edge in mapping_result.ownership_edges]
            table_ref = f"{self.full_dataset}.{edge_table}"
            job_config = bigquery.LoadJobConfig(
                schema=EDGE_SCHEMA,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )
            job = self.client.load_table_from_json(rows, table_ref, job_config=job_config)
            job.result()
            counts["ownership_edges"] = len(rows)

        return counts

    def create_property_graph(
        self,
        graph_name: str = "OwnershipGraph",
        entity_table: str = "entity_nodes",
        person_table: str = "person_nodes",
        edge_table: str = "ownership_edges",
    ) -> None:
        """Create the property graph over the loaded tables."""
        ddl = generate_create_graph_ddl(
            dataset=self.full_dataset,
            graph_name=graph_name,
            entity_table=entity_table,
            person_table=person_table,
            edge_table=edge_table,
        )
        self.client.query(ddl).result()

    def drop_property_graph(self, graph_name: str = "OwnershipGraph") -> None:
        """Drop the property graph (tables remain)."""
        ddl = generate_drop_graph_ddl(self.full_dataset, graph_name)
        self.client.query(ddl).result()

    def run_gql_query(self, query: str) -> list[dict]:
        """Execute a GQL query and return results as dicts."""
        result = self.client.query(query).result()
        return [dict(row) for row in result]
