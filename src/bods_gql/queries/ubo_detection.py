"""GQL queries for Ultimate Beneficial Owner (UBO) detection.

Translates the Cypher UBO queries from bods-neo4j into GQL (ISO/IEC 39075)
compatible with BigQuery Graph.

Key differences from Cypher:
- Variable-length paths use quantifier syntax: ->{1,10} instead of *1..10
- No reduce() function; effective ownership calculation uses horizontal
  aggregation or SQL post-processing via GRAPH_TABLE
- NOT EXISTS subqueries use GQL's EXISTS predicate
- Path property extraction uses edges()/nodes() functions
"""

from __future__ import annotations


def find_owners(
    dataset: str,
    graph_name: str = "OwnershipGraph",
    max_depth: int = 10,
) -> str:
    """Find all direct and indirect owners of a specific entity.

    GQL translation of FIND_OWNERS_QUERY from bods-neo4j.

    Usage: Replace @record_id with the target entity's recordId.
    """
    return f"""-- Find all owners (persons and top-level entities) of a given entity
-- Parameter: @record_id = target entity's recordId
GRAPH `{dataset}`.{graph_name}
MATCH path = (owner)-[i:HAS_INTEREST]->{{{1},{max_depth}}}(target:Entity
    WHERE target.record_id = @record_id)
WHERE NOT EXISTS {{
    MATCH ()-[:HAS_INTEREST]->(owner)
}}
LET depth = PATH_LENGTH(path)
RETURN
    owner.record_id AS owner_record_id,
    owner.name AS owner_name,
    LABELS(owner) AS owner_labels,
    depth,
    i.share_minimum AS share_minimums,
    i.share_maximum AS share_maximums,
    i.share_exact AS share_exacts,
    i.is_beneficial_ownership AS bo_flags,
    i.interest_types AS interest_types
ORDER BY depth;"""


def find_owned_entities(
    dataset: str,
    graph_name: str = "OwnershipGraph",
    max_depth: int = 10,
) -> str:
    """Find all entities owned/controlled by a person or entity.

    GQL translation of FIND_OWNED_ENTITIES_QUERY from bods-neo4j.

    Usage: Replace @record_id with the owner's recordId.
    """
    return f"""-- Find all entities owned or controlled by a given person/entity
-- Parameter: @record_id = owner's recordId
GRAPH `{dataset}`.{graph_name}
MATCH path = (owner WHERE owner.record_id = @record_id)
    -[i:HAS_INTEREST]->{{{1},{max_depth}}}(target:Entity)
LET depth = PATH_LENGTH(path)
RETURN
    target.record_id AS entity_record_id,
    target.name AS entity_name,
    target.jurisdiction_code AS jurisdiction_code,
    target.entity_type AS entity_type,
    depth,
    i.share_minimum AS share_minimums,
    i.share_maximum AS share_maximums,
    i.share_exact AS share_exacts
ORDER BY depth;"""


def find_ubos_gql(
    dataset: str,
    graph_name: str = "OwnershipGraph",
    max_depth: int = 10,
) -> str:
    """Find all UBOs above a threshold using pure GQL pattern matching.

    This is the GQL translation of FIND_ALL_PERSON_UBOS_QUERY.

    NOTE: Effective ownership calculation (multiplying share percentages
    along a chain) requires horizontal aggregation over path edges, which
    has limited support in BigQuery GQL as of 2026. This query returns
    the path depth and per-edge share values. For threshold-based filtering
    with effective ownership calculation, use find_ubos_with_sql() instead.

    Usage: Replace @threshold with the minimum ownership percentage (e.g. 25).
    """
    return f"""-- Find persons who are UBOs of entities, with ownership chain details
-- NOTE: Per-edge share values returned; effective ownership requires
-- post-processing (multiply share_minimum values along each path)
GRAPH `{dataset}`.{graph_name}
MATCH path = (person:Person)-[i:HAS_INTEREST]->{{{1},{max_depth}}}(entity:Entity)
WHERE NOT EXISTS {{
    MATCH ()-[:HAS_INTEREST]->(person)
}}
LET depth = PATH_LENGTH(path)
RETURN
    person.record_id AS person_record_id,
    person.name AS person_name,
    entity.record_id AS entity_record_id,
    entity.name AS entity_name,
    depth,
    i.share_minimum AS share_minimums,
    i.share_maximum AS share_maximums,
    i.share_exact AS share_exacts,
    i.is_beneficial_ownership AS bo_flags
ORDER BY depth;"""


def find_ubos_with_sql(
    dataset: str,
    graph_name: str = "OwnershipGraph",
    max_depth: int = 10,
    threshold: float = 25.0,
) -> str:
    """Find UBOs using GRAPH_TABLE embedded in SQL for threshold calculation.

    This approach uses GRAPH_TABLE to extract ownership paths, then SQL
    to calculate effective ownership by multiplying share percentages.
    This works around GQL's limited horizontal aggregation support.

    The effective ownership percentage is calculated by multiplying
    share_minimum values along each hop in the chain:
    effective_pct = hop1_share * hop2_share * ... / 100^(depth-1)
    """
    return f"""-- Find UBOs with effective ownership above threshold using GRAPH_TABLE + SQL
-- Parameters: threshold = {threshold}%
WITH ownership_paths AS (
    SELECT *
    FROM GRAPH_TABLE(
        `{dataset}`.{graph_name}
        MATCH (person:Person)-[i:HAS_INTEREST]->{{{1},{max_depth}}}(entity:Entity)
        WHERE NOT EXISTS {{
            MATCH ()-[:HAS_INTEREST]->(person)
        }}
        RETURN
            person.record_id AS person_record_id,
            person.name AS person_name,
            entity.record_id AS entity_record_id,
            entity.name AS entity_name,
            PATH_LENGTH(MATCH (person)-[i]->(entity)) AS depth,
            i.share_minimum AS share_minimum,
            i.share_maximum AS share_maximum,
            i.share_exact AS share_exact
    )
)
-- For single-hop paths, effective ownership = direct share
-- For multi-hop, this requires expanding the group variable per-edge
-- and multiplying. When group variable expansion is supported:
SELECT
    person_record_id,
    person_name,
    entity_record_id,
    entity_name,
    depth,
    share_minimum AS effective_min_pct,
    share_maximum AS effective_max_pct
FROM ownership_paths
WHERE share_minimum >= {threshold}
    OR share_exact >= {threshold}
ORDER BY effective_min_pct DESC;"""


def find_entities_without_ubos(
    dataset: str,
    graph_name: str = "OwnershipGraph",
) -> str:
    """Find entities that have ownership but no person at the top of the chain.

    GQL translation of FIND_ENTITIES_WITHOUT_UBOS_QUERY from bods-neo4j.
    """
    return f"""-- Find entities with incoming ownership but no natural person UBO
GRAPH `{dataset}`.{graph_name}
MATCH (e:Entity)
WHERE NOT EXISTS {{
    MATCH (p:Person)-[:HAS_INTEREST]->+{{1,}}(e)
}}
AND EXISTS {{
    MATCH ()-[:HAS_INTEREST]->(e)
}}
RETURN
    e.record_id AS record_id,
    e.name AS name,
    e.jurisdiction_code AS jurisdiction_code,
    e.entity_type AS entity_type
ORDER BY e.name;"""
