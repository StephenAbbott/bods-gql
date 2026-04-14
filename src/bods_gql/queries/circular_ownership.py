"""GQL queries for circular ownership detection.

Translates the Cypher circular ownership queries from bods-neo4j into GQL.

Key GQL patterns:
- Cycle detection uses variable reuse: MATCH (e)-[]->{2,N}(e)
  where the same variable 'e' appears as both start and end node.
- ELEMENT_ID() ensures distinct entity identification in deduplication.
"""

from __future__ import annotations


def find_cycles(
    dataset: str,
    graph_name: str = "OwnershipGraph",
    max_depth: int = 10,
) -> str:
    """Detect all circular ownership chains.

    GQL translation of FIND_CYCLES_QUERY from bods-neo4j.
    Variable reuse (e...e) detects cycles where ownership chains
    return to the starting entity.
    """
    return f"""-- Detect circular ownership chains (entity owns itself through a chain)
GRAPH `{dataset}`.{graph_name}
MATCH path = (e:Entity)-[:HAS_INTEREST]->{{{2},{max_depth}}}(e)
LET cycle_length = PATH_LENGTH(path)
RETURN DISTINCT
    e.record_id AS entity_record_id,
    e.name AS entity_name,
    e.jurisdiction_code AS jurisdiction_code,
    cycle_length
ORDER BY cycle_length;"""


def check_entity_cycle(
    dataset: str,
    graph_name: str = "OwnershipGraph",
    max_depth: int = 10,
) -> str:
    """Check if a specific entity is involved in a circular ownership chain.

    GQL translation of CHECK_ENTITY_CYCLE_QUERY from bods-neo4j.

    Usage: Replace @record_id with the entity's recordId to check.
    """
    return f"""-- Check if a specific entity is in a circular ownership chain
-- Parameter: @record_id = entity's recordId to check
GRAPH `{dataset}`.{graph_name}
MATCH path = (e:Entity WHERE e.record_id = @record_id)
    -[i:HAS_INTEREST]->{{{2},{max_depth}}}(e)
LET cycle_length = PATH_LENGTH(path)
RETURN
    cycle_length,
    i.share_minimum AS share_minimums
ORDER BY cycle_length;"""


def mutual_ownership(
    dataset: str,
    graph_name: str = "OwnershipGraph",
) -> str:
    """Find reciprocal ownership pairs (A owns B and B owns A).

    GQL translation of MUTUAL_OWNERSHIP_QUERY from bods-neo4j.
    Uses ELEMENT_ID comparison to avoid duplicate pairs.
    """
    return f"""-- Find mutual/reciprocal ownership pairs
GRAPH `{dataset}`.{graph_name}
MATCH (a:Entity)-[r1:HAS_INTEREST]->(b:Entity)-[r2:HAS_INTEREST]->(a)
WHERE ELEMENT_ID(a) < ELEMENT_ID(b)
RETURN
    a.record_id AS entity_a_record_id,
    a.name AS entity_a_name,
    b.record_id AS entity_b_record_id,
    b.name AS entity_b_name,
    r1.share_minimum AS a_owns_b_min,
    r1.share_maximum AS a_owns_b_max,
    r2.share_minimum AS b_owns_a_min,
    r2.share_maximum AS b_owns_a_max
ORDER BY a.name;"""


def cycle_stats(
    dataset: str,
    graph_name: str = "OwnershipGraph",
    max_depth: int = 10,
) -> str:
    """Summary statistics on circular ownership in the dataset.

    GQL translation of CYCLE_STATS_QUERY from bods-neo4j.
    """
    return f"""-- Summary statistics on circular ownership
GRAPH `{dataset}`.{graph_name}
MATCH path = (e:Entity)-[:HAS_INTEREST]->{{{2},{max_depth}}}(e)
LET cycle_length = PATH_LENGTH(path)
RETURN
    COUNT(DISTINCT e) AS entities_in_cycles,
    MIN(cycle_length) AS shortest_cycle,
    MAX(cycle_length) AS longest_cycle,
    AVG(cycle_length) AS avg_cycle_length,
    COUNT(path) AS total_cycle_paths;"""
