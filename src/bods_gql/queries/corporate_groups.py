"""GQL queries for corporate group mapping.

Translates the Cypher corporate group queries from bods-neo4j into GQL.

Key GQL patterns:
- Bidirectional traversal uses undirected edge patterns: -[]-
- TRAIL ensures cycle-free traversal for group discovery
- Variable-length paths with quantifiers: ->{0,N} or -{0,N}-
"""

from __future__ import annotations


def corporate_group(
    dataset: str,
    graph_name: str = "OwnershipGraph",
    max_depth: int = 20,
) -> str:
    """Find complete corporate group from any starting entity.

    GQL translation of CORPORATE_GROUP_QUERY from bods-neo4j.
    Uses bidirectional TRAIL traversal to find all connected members.

    Usage: Replace @record_id with any entity's recordId in the group.
    """
    return f"""-- Find all members of a corporate group (bidirectional traversal)
-- Parameter: @record_id = any entity's recordId within the group
GRAPH `{dataset}`.{graph_name}
MATCH TRAIL (start:Entity WHERE start.record_id = @record_id)
    -[:HAS_INTEREST]-{{0,{max_depth}}}(member)
RETURN DISTINCT
    member.record_id AS record_id,
    member.name AS name,
    LABELS(member) AS labels
ORDER BY member.name;"""


def top_level_parents(
    dataset: str,
    graph_name: str = "OwnershipGraph",
    limit: int = 100,
) -> str:
    """Find root entities with no inbound ownership that own other entities.

    GQL translation of TOP_LEVEL_PARENTS_QUERY from bods-neo4j.
    """
    return f"""-- Find top-level parent entities (own others, not owned by anyone)
GRAPH `{dataset}`.{graph_name}
MATCH (parent:Entity)-[:HAS_INTEREST]->+(subsidiary:Entity)
WHERE NOT EXISTS {{
    MATCH ()-[:HAS_INTEREST]->(parent)
}}
RETURN
    parent.record_id AS record_id,
    parent.name AS name,
    parent.entity_type AS entity_type,
    parent.jurisdiction_code AS jurisdiction_code,
    COUNT(DISTINCT subsidiary) AS subsidiary_count
ORDER BY subsidiary_count DESC
LIMIT {limit};"""


def group_jurisdiction_analysis(
    dataset: str,
    graph_name: str = "OwnershipGraph",
    max_depth: int = 20,
) -> str:
    """Analyse jurisdictional breakdown of a corporate group.

    GQL translation of GROUP_JURISDICTION_ANALYSIS_QUERY from bods-neo4j.

    Usage: Replace @record_id with any entity's recordId in the group.
    """
    return f"""-- Jurisdictional breakdown of a corporate group
-- Parameter: @record_id = any entity's recordId within the group
GRAPH `{dataset}`.{graph_name}
MATCH TRAIL (start:Entity WHERE start.record_id = @record_id)
    -[:HAS_INTEREST]-{{0,{max_depth}}}(member:Entity)
RETURN
    member.jurisdiction_code AS jurisdiction,
    COUNT(DISTINCT member) AS entity_count
GROUP BY jurisdiction
ORDER BY entity_count DESC;"""


def group_metrics(
    dataset: str,
    graph_name: str = "OwnershipGraph",
    max_depth: int = 20,
) -> str:
    """Summary metrics for a corporate group.

    GQL translation of GROUP_METRICS_QUERY from bods-neo4j.

    Usage: Replace @record_id with any entity's recordId in the group.
    """
    return f"""-- Summary metrics for a corporate group
-- Parameter: @record_id = any entity's recordId within the group
GRAPH `{dataset}`.{graph_name}
MATCH TRAIL path = (start:Entity WHERE start.record_id = @record_id)
    -[:HAS_INTEREST]-{{0,{max_depth}}}(member)
RETURN
    COUNT(DISTINCT member) AS total_members,
    MAX(PATH_LENGTH(path)) AS max_depth;"""


def all_groups(
    dataset: str,
    graph_name: str = "OwnershipGraph",
    limit: int = 100,
) -> str:
    """Find all corporate groups ordered by size.

    GQL translation of ALL_GROUPS_QUERY from bods-neo4j.
    """
    return f"""-- Find all corporate groups ordered by size
GRAPH `{dataset}`.{graph_name}
MATCH (parent:Entity)-[i:HAS_INTEREST]->+(sub:Entity)
WHERE NOT EXISTS {{
    MATCH ()-[:HAS_INTEREST]->(parent)
}}
RETURN
    parent.record_id AS parent_record_id,
    parent.name AS parent_name,
    parent.jurisdiction_code AS jurisdiction_code,
    COUNT(DISTINCT sub) AS subsidiary_count
GROUP BY parent_record_id, parent_name, jurisdiction_code
ORDER BY subsidiary_count DESC
LIMIT {limit};"""
