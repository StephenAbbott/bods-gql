-- ============================================================================
-- BODS v0.4 GQL Queries for Existing bodsdata BigQuery Datasets
-- ============================================================================
--
-- These queries work against the public bodsdata BigQuery project:
--   - bodsdata.gleif_version_0_4 (GLEIF corporate ownership, ~6M relationships)
--   - bodsdata.uk_version_0_4    (UK Companies House PSC data)
--
-- Prerequisites:
--   1. Access to BigQuery with Enterprise or Enterprise Plus edition
--      (required for graph queries)
--   2. Permission to create property graphs in your project
--
-- IMPORTANT: The bodsdata tables use a flattened relational schema different
-- from the bods-gql node/edge table format. These queries show both approaches.
-- ============================================================================


-- ============================================================================
-- PART 1: Create property graph over bodsdata tables
-- ============================================================================

-- Option A: Graph over GLEIF data (corporate ownership only, no natural persons)
CREATE OR REPLACE PROPERTY GRAPH `your_project.your_dataset`.GLEIFOwnershipGraph
  NODE TABLES (
    `bodsdata.gleif_version_0_4`.entity_statement
      KEY (recordId)
      LABEL Entity
      PROPERTIES (
        statementId,
        recordId,
        declarationSubject,
        statementDate,
        recordStatus,
        name,
        entityType,
        jurisdiction,
        foundingDate
      )
  )
  EDGE TABLES (
    `bodsdata.gleif_version_0_4`.relationship_statement
      KEY (recordId)
      SOURCE KEY (interestedParty) REFERENCES Entity (recordId)
      DESTINATION KEY (subject) REFERENCES Entity (recordId)
      LABEL HAS_INTEREST
      PROPERTIES (
        statementId,
        recordId,
        declarationSubject,
        statementDate,
        recordStatus
      )
  );


-- ============================================================================
-- PART 2: Basic GQL queries over bodsdata
-- ============================================================================

-- 2a. Find all direct ownership relationships
GRAPH `your_project.your_dataset`.GLEIFOwnershipGraph
MATCH (owner:Entity)-[r:HAS_INTEREST]->(subject:Entity)
RETURN
    owner.name AS owner_name,
    subject.name AS subject_name,
    owner.jurisdiction AS owner_jurisdiction,
    subject.jurisdiction AS subject_jurisdiction
LIMIT 100;


-- 2b. Find ownership chains up to 3 levels deep
GRAPH `your_project.your_dataset`.GLEIFOwnershipGraph
MATCH path = (top:Entity)-[:HAS_INTEREST]->{1,3}(bottom:Entity)
WHERE NOT EXISTS {
    MATCH ()-[:HAS_INTEREST]->(top)
}
LET depth = PATH_LENGTH(path)
RETURN
    top.name AS top_entity,
    bottom.name AS bottom_entity,
    top.jurisdiction AS top_jurisdiction,
    bottom.jurisdiction AS bottom_jurisdiction,
    depth
ORDER BY depth DESC
LIMIT 100;


-- 2c. Find entities with the most subsidiaries
GRAPH `your_project.your_dataset`.GLEIFOwnershipGraph
MATCH (parent:Entity)-[:HAS_INTEREST]->+(sub:Entity)
WHERE NOT EXISTS {
    MATCH ()-[:HAS_INTEREST]->(parent)
}
RETURN
    parent.name AS parent_name,
    parent.jurisdiction AS jurisdiction,
    COUNT(DISTINCT sub) AS subsidiary_count
GROUP BY parent_name, jurisdiction
ORDER BY subsidiary_count DESC
LIMIT 50;


-- 2d. Detect circular ownership
GRAPH `your_project.your_dataset`.GLEIFOwnershipGraph
MATCH path = (e:Entity)-[:HAS_INTEREST]->{2,5}(e)
LET cycle_length = PATH_LENGTH(path)
RETURN DISTINCT
    e.name AS entity_name,
    e.jurisdiction AS jurisdiction,
    cycle_length
ORDER BY cycle_length
LIMIT 100;


-- 2e. Find mutual ownership pairs
GRAPH `your_project.your_dataset`.GLEIFOwnershipGraph
MATCH (a:Entity)-[r1:HAS_INTEREST]->(b:Entity)-[r2:HAS_INTEREST]->(a)
WHERE ELEMENT_ID(a) < ELEMENT_ID(b)
RETURN
    a.name AS entity_a,
    b.name AS entity_b,
    a.jurisdiction AS jurisdiction_a,
    b.jurisdiction AS jurisdiction_b
LIMIT 100;


-- ============================================================================
-- PART 3: GRAPH_TABLE queries (GQL embedded in SQL)
-- ============================================================================

-- 3a. Join graph results with interest details from separate table
SELECT
    g.owner_name,
    g.subject_name,
    i.type AS interest_type,
    i.share_exact,
    i.share_minimum,
    i.share_maximum,
    i.directOrIndirect
FROM GRAPH_TABLE(
    `your_project.your_dataset`.GLEIFOwnershipGraph
    MATCH (owner:Entity)-[r:HAS_INTEREST]->(subject:Entity)
    RETURN
        owner.name AS owner_name,
        subject.name AS subject_name,
        r.statementId AS rel_statement_id
) g
LEFT JOIN `bodsdata.gleif_version_0_4`.relationship_recorddetails_interests i
    ON g.rel_statement_id = i.statementId
WHERE i.share_minimum IS NOT NULL
ORDER BY i.share_minimum DESC
LIMIT 100;


-- 3b. Cross-jurisdiction ownership analysis
SELECT
    owner_jurisdiction,
    subject_jurisdiction,
    relationship_count
FROM GRAPH_TABLE(
    `your_project.your_dataset`.GLEIFOwnershipGraph
    MATCH (owner:Entity)-[:HAS_INTEREST]->(subject:Entity)
    RETURN
        owner.jurisdiction AS owner_jurisdiction,
        subject.jurisdiction AS subject_jurisdiction,
        COUNT(*) AS relationship_count
    GROUP BY owner_jurisdiction, subject_jurisdiction
)
ORDER BY relationship_count DESC
LIMIT 50;


-- ============================================================================
-- PART 4: Cleanup
-- ============================================================================

-- DROP PROPERTY GRAPH IF EXISTS `your_project.your_dataset`.GLEIFOwnershipGraph;
