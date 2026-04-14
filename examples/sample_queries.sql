-- ============================================================================
-- BODS v0.4 GQL Query Reference
-- ============================================================================
--
-- Complete set of GQL queries for beneficial ownership analysis.
-- These queries work against the bods-gql property graph schema
-- (entity_nodes, person_nodes, ownership_edges tables).
--
-- Replace @record_id parameters with actual recordId values.
-- Replace `project.dataset` with your BigQuery dataset.
-- ============================================================================


-- ============================================================================
-- UBO DETECTION
-- ============================================================================

-- 1. Find all owners of a specific entity
GRAPH `project.dataset`.OwnershipGraph
MATCH path = (owner)-[i:HAS_INTEREST]->{1,10}(target:Entity
    WHERE target.record_id = @record_id)
WHERE NOT EXISTS {
    MATCH ()-[:HAS_INTEREST]->(owner)
}
LET depth = PATH_LENGTH(path)
RETURN
    owner.record_id AS owner_record_id,
    owner.name AS owner_name,
    LABELS(owner) AS owner_labels,
    depth,
    i.share_minimum AS share_minimums,
    i.share_maximum AS share_maximums,
    i.interest_types AS interest_types
ORDER BY depth;


-- 2. Find all entities owned by a person or entity
GRAPH `project.dataset`.OwnershipGraph
MATCH path = (owner WHERE owner.record_id = @record_id)
    -[i:HAS_INTEREST]->{1,10}(target:Entity)
LET depth = PATH_LENGTH(path)
RETURN
    target.record_id AS entity_record_id,
    target.name AS entity_name,
    target.jurisdiction_code AS jurisdiction_code,
    target.entity_type AS entity_type,
    depth,
    i.share_minimum AS share_minimums,
    i.share_exact AS share_exacts
ORDER BY depth;


-- 3. Find all person UBOs (pure GQL, per-edge shares)
GRAPH `project.dataset`.OwnershipGraph
MATCH path = (person:Person)-[i:HAS_INTEREST]->{1,10}(entity:Entity)
WHERE NOT EXISTS {
    MATCH ()-[:HAS_INTEREST]->(person)
}
LET depth = PATH_LENGTH(path)
RETURN
    person.record_id AS person_record_id,
    person.name AS person_name,
    entity.record_id AS entity_record_id,
    entity.name AS entity_name,
    depth,
    i.share_minimum AS share_minimums,
    i.share_maximum AS share_maximums,
    i.is_beneficial_ownership AS bo_flags
ORDER BY depth;


-- 4. Find UBOs with effective ownership threshold (GRAPH_TABLE + SQL)
-- This uses SQL for the share multiplication that GQL cannot yet do natively
WITH ownership_paths AS (
    SELECT *
    FROM GRAPH_TABLE(
        `project.dataset`.OwnershipGraph
        MATCH (person:Person)-[i:HAS_INTEREST]->{1,10}(entity:Entity)
        WHERE NOT EXISTS {
            MATCH ()-[:HAS_INTEREST]->(person)
        }
        RETURN
            person.record_id AS person_record_id,
            person.name AS person_name,
            entity.record_id AS entity_record_id,
            entity.name AS entity_name,
            i.share_minimum AS share_minimum,
            i.share_exact AS share_exact
    )
)
SELECT
    person_record_id,
    person_name,
    entity_record_id,
    entity_name,
    share_minimum AS effective_min_pct
FROM ownership_paths
WHERE share_minimum >= 25.0
    OR share_exact >= 25.0
ORDER BY effective_min_pct DESC;


-- 5. Find entities with no person UBO identified
GRAPH `project.dataset`.OwnershipGraph
MATCH (e:Entity)
WHERE NOT EXISTS {
    MATCH (p:Person)-[:HAS_INTEREST]->+(e)
}
AND EXISTS {
    MATCH ()-[:HAS_INTEREST]->(e)
}
RETURN
    e.record_id AS record_id,
    e.name AS name,
    e.jurisdiction_code AS jurisdiction_code,
    e.entity_type AS entity_type
ORDER BY e.name;


-- ============================================================================
-- CORPORATE GROUPS
-- ============================================================================

-- 6. Map complete corporate group (bidirectional)
GRAPH `project.dataset`.OwnershipGraph
MATCH TRAIL (start:Entity WHERE start.record_id = @record_id)
    -[:HAS_INTEREST]-{0,20}(member)
RETURN DISTINCT
    member.record_id AS record_id,
    member.name AS name,
    LABELS(member) AS labels
ORDER BY member.name;


-- 7. Find top-level parent entities
GRAPH `project.dataset`.OwnershipGraph
MATCH (parent:Entity)-[:HAS_INTEREST]->+(subsidiary:Entity)
WHERE NOT EXISTS {
    MATCH ()-[:HAS_INTEREST]->(parent)
}
RETURN
    parent.record_id AS record_id,
    parent.name AS name,
    parent.entity_type AS entity_type,
    parent.jurisdiction_code AS jurisdiction_code,
    COUNT(DISTINCT subsidiary) AS subsidiary_count
ORDER BY subsidiary_count DESC
LIMIT 100;


-- 8. Jurisdiction analysis for a corporate group
GRAPH `project.dataset`.OwnershipGraph
MATCH TRAIL (start:Entity WHERE start.record_id = @record_id)
    -[:HAS_INTEREST]-{0,20}(member:Entity)
RETURN
    member.jurisdiction_code AS jurisdiction,
    COUNT(DISTINCT member) AS entity_count
GROUP BY jurisdiction
ORDER BY entity_count DESC;


-- 9. Corporate group summary metrics
GRAPH `project.dataset`.OwnershipGraph
MATCH TRAIL path = (start:Entity WHERE start.record_id = @record_id)
    -[:HAS_INTEREST]-{0,20}(member)
RETURN
    COUNT(DISTINCT member) AS total_members,
    MAX(PATH_LENGTH(path)) AS max_depth;


-- 10. Find all corporate groups ordered by size
GRAPH `project.dataset`.OwnershipGraph
MATCH (parent:Entity)-[i:HAS_INTEREST]->+(sub:Entity)
WHERE NOT EXISTS {
    MATCH ()-[:HAS_INTEREST]->(parent)
}
RETURN
    parent.record_id AS parent_record_id,
    parent.name AS parent_name,
    parent.jurisdiction_code AS jurisdiction_code,
    COUNT(DISTINCT sub) AS subsidiary_count
GROUP BY parent_record_id, parent_name, jurisdiction_code
ORDER BY subsidiary_count DESC
LIMIT 100;


-- ============================================================================
-- CIRCULAR OWNERSHIP
-- ============================================================================

-- 11. Detect all circular ownership chains
GRAPH `project.dataset`.OwnershipGraph
MATCH path = (e:Entity)-[:HAS_INTEREST]->{2,10}(e)
LET cycle_length = PATH_LENGTH(path)
RETURN DISTINCT
    e.record_id AS entity_record_id,
    e.name AS entity_name,
    e.jurisdiction_code AS jurisdiction_code,
    cycle_length
ORDER BY cycle_length;


-- 12. Check if specific entity is in a cycle
GRAPH `project.dataset`.OwnershipGraph
MATCH path = (e:Entity WHERE e.record_id = @record_id)
    -[i:HAS_INTEREST]->{2,10}(e)
LET cycle_length = PATH_LENGTH(path)
RETURN
    cycle_length,
    i.share_minimum AS share_minimums
ORDER BY cycle_length;


-- 13. Find mutual/reciprocal ownership pairs
GRAPH `project.dataset`.OwnershipGraph
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
ORDER BY a.name;


-- 14. Circular ownership statistics
GRAPH `project.dataset`.OwnershipGraph
MATCH path = (e:Entity)-[:HAS_INTEREST]->{2,10}(e)
LET cycle_length = PATH_LENGTH(path)
RETURN
    COUNT(DISTINCT e) AS entities_in_cycles,
    MIN(cycle_length) AS shortest_cycle,
    MAX(cycle_length) AS longest_cycle,
    AVG(cycle_length) AS avg_cycle_length,
    COUNT(path) AS total_cycle_paths;
