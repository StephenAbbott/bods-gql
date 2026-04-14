"""Generate BigQuery CREATE PROPERTY GRAPH DDL for BODS v0.4 data.

Produces the schema that creates a property graph view over BODS node
and edge tables in BigQuery, enabling GQL queries.
"""

from __future__ import annotations


def generate_create_graph_ddl(
    dataset: str,
    graph_name: str = "OwnershipGraph",
    entity_table: str = "entity_nodes",
    person_table: str = "person_nodes",
    edge_table: str = "ownership_edges",
) -> str:
    """Generate CREATE PROPERTY GRAPH DDL for a BODS ownership graph.

    Args:
        dataset: BigQuery dataset ID (e.g. "my_project.my_dataset").
        graph_name: Name for the property graph.
        entity_table: Name of the entity node table.
        person_table: Name of the person node table.
        edge_table: Name of the ownership edge table.

    Returns:
        BigQuery SQL DDL statement.
    """
    return f"""CREATE OR REPLACE PROPERTY GRAPH `{dataset}`.{graph_name}
  NODE TABLES (
    `{dataset}`.{entity_table}
      KEY (record_id)
      LABEL Entity
      PROPERTIES (
        record_id,
        statement_id,
        statement_date,
        record_status,
        declaration_subject,
        name,
        entity_type,
        entity_subtype,
        is_component,
        jurisdiction_name,
        jurisdiction_code,
        founding_date,
        dissolution_date,
        uri,
        primary_identifier_id,
        primary_identifier_scheme,
        registered_address,
        registered_country,
        publisher_name,
        identifiers_json,
        addresses_json
      ),
    `{dataset}`.{person_table}
      KEY (record_id)
      LABEL Person
      PROPERTIES (
        record_id,
        statement_id,
        statement_date,
        record_status,
        declaration_subject,
        name,
        family_name,
        given_name,
        person_type,
        is_component,
        birth_date,
        death_date,
        nationality_code,
        pep_status,
        publisher_name,
        names_json,
        identifiers_json,
        addresses_json,
        nationalities_json
      )
  )
  EDGE TABLES (
    `{dataset}`.{edge_table}
      KEY (record_id)
      SOURCE KEY (interested_party) REFERENCES Entity (record_id)
      DESTINATION KEY (subject) REFERENCES Entity (record_id)
      LABEL HAS_INTEREST
      PROPERTIES (
        record_id,
        statement_id,
        statement_date,
        record_status,
        is_component,
        interest_types,
        is_beneficial_ownership,
        direct_or_indirect,
        share_exact,
        share_minimum,
        share_maximum,
        share_exclusive_minimum,
        share_exclusive_maximum,
        interest_start_date,
        interest_end_date,
        interests_json,
        component_records_json
      ),
    `{dataset}`.{edge_table} AS PersonOwnsEntity
      KEY (record_id)
      SOURCE KEY (interested_party) REFERENCES Person (record_id)
      DESTINATION KEY (subject) REFERENCES Entity (record_id)
      LABEL HAS_INTEREST
      PROPERTIES (
        record_id,
        statement_id,
        statement_date,
        record_status,
        is_component,
        interest_types,
        is_beneficial_ownership,
        direct_or_indirect,
        share_exact,
        share_minimum,
        share_maximum,
        share_exclusive_minimum,
        share_exclusive_maximum,
        interest_start_date,
        interest_end_date,
        interests_json,
        component_records_json
      )
  );"""


def generate_create_graph_for_bodsdata(
    source: str = "gleif_version_0_4",
) -> str:
    """Generate CREATE PROPERTY GRAPH DDL for existing bodsdata BigQuery datasets.

    The bodsdata project stores BODS data in a flattened relational schema.
    This generates DDL that maps those tables to a property graph.

    Args:
        source: Dataset name in the bodsdata project.
            Options: "gleif_version_0_4", "uk_version_0_4"
    """
    dataset = f"bodsdata.{source}"

    return f"""-- Property graph over existing bodsdata BigQuery tables
-- Dataset: {dataset}
--
-- NOTE: The bodsdata tables use a flattened schema. Entity and relationship
-- statements are in separate tables. Person statements (if present in the
-- source) are included in entity_statement with entityType indicators.
--
-- This DDL creates a graph view over the existing tables without copying data.

CREATE OR REPLACE PROPERTY GRAPH `{dataset}`.OwnershipGraph
  NODE TABLES (
    `{dataset}`.entity_statement
      KEY (statementId)
      LABEL Entity
      PROPERTIES (
        statementId,
        declarationSubject,
        statementDate,
        recordId,
        recordStatus,
        name,
        entityType,
        jurisdiction,
        foundingDate
      )
  )
  EDGE TABLES (
    `{dataset}`.relationship_statement
      KEY (statementId)
      SOURCE KEY (interestedParty) REFERENCES Entity (recordId)
      DESTINATION KEY (subject) REFERENCES Entity (recordId)
      LABEL HAS_INTEREST
      PROPERTIES (
        statementId,
        declarationSubject,
        statementDate,
        recordId,
        recordStatus
      )
  );

-- NOTE: Interest details (share percentages, interest types) are in the
-- separate relationship_recorddetails_interests table. To include them
-- in graph queries, join via GRAPH_TABLE + SQL:
--
-- SELECT g.*, i.type, i.share_exact, i.share_minimum
-- FROM GRAPH_TABLE(
--   `{dataset}`.OwnershipGraph
--   MATCH (a:Entity)-[r:HAS_INTEREST]->(b:Entity)
--   RETURN a.name AS owner_name, b.name AS subject_name,
--          r.statementId AS rel_statement_id
-- ) g
-- LEFT JOIN `{dataset}`.relationship_recorddetails_interests i
--   ON g.rel_statement_id = i.statementId;
"""


def generate_drop_graph_ddl(dataset: str, graph_name: str = "OwnershipGraph") -> str:
    """Generate DROP PROPERTY GRAPH DDL."""
    return f"DROP PROPERTY GRAPH IF EXISTS `{dataset}`.{graph_name};"
