# bods-gql

Convert [Beneficial Ownership Data Standard (BODS)](https://standard.openownership.org/) version 0.4 data for querying with [GQL](https://www.gqlstandards.org/) (ISO/IEC 39075) on Google BigQuery.

## What this does

BODS publishes beneficial ownership information as JSON statements about **entities**, **persons**, and **relationships** between them. This tool maps that data into a **property graph** that can be queried with GQL — the ISO standard graph query language — on BigQuery.

It provides:

- **BODS-to-graph mapper** — converts BODS v0.4 JSON/JSONL into node tables (entities, persons) and edge tables (ownership interests)
- **Property graph DDL** — generates `CREATE PROPERTY GRAPH` statements for BigQuery
- **14 GQL queries** — translated from the [Cypher queries in bods-neo4j](https://github.com/StephenAbbott/bods-neo4j), covering UBO detection, corporate group mapping, and circular ownership analysis
- **BigQuery integration** — load BODS data directly into BigQuery and create the property graph
- **Examples for existing data** — ready-to-run queries against the public [bodsdata](https://bods-data.openownership.org/) BigQuery datasets (GLEIF and UK Companies House)

## Background: GQL vs Cypher vs GraphQL

| | GQL (ISO/IEC 39075) | Cypher (Neo4j) | GraphQL |
|---|---|---|---|
| **Type** | Database query language for property graphs | Database query language for property graphs | API query language for web services |
| **Standard** | ISO international standard (April 2024) | Neo4j proprietary (openCypher open-sourced) | Linux Foundation (2015) |
| **Used by** | BigQuery, Spanner, Microsoft Fabric | Neo4j, Memgraph | REST/API layer |
| **Syntax** | `MATCH (a)-[r]->(b)` | `MATCH (a)-[r]->(b)` | `{ user { name } }` |

GQL inherits most of its pattern matching syntax from Cypher. The key differences are in quantified paths (`->{1,10}` vs `*1..10`), explicit `FILTER` statements, and `RETURN ... GROUP BY` for aggregation.

## How it works

### BODS data model to property graph

```
BODS Statement Types          Property Graph
─────────────────────         ──────────────────────
entity statement       →      Entity node
                              (name, type, jurisdiction, identifiers...)

person statement       →      Person node
                              (name, birth_date, nationality, PEP status...)

relationship statement →      HAS_INTEREST edge
                              (share %, interest type, beneficial ownership flag...)
```

The relationship's `interestedParty` becomes the edge **source** and `subject` becomes the edge **destination**: `(interestedParty)-[:HAS_INTEREST]->(subject)`.

### GQL query capabilities

| Analysis | GQL Pattern | Cypher Equivalent |
|---|---|---|
| **UBO detection** | `(person:Person)-[i:HAS_INTEREST]->{1,10}(entity:Entity)` | `(person:Person)-[:HAS_INTEREST*1..10]->(entity:Entity)` |
| **Corporate groups** | `TRAIL (start)-[:HAS_INTEREST]-{0,20}(member)` | `apoc.path.expandConfig(start, {...})` |
| **Circular ownership** | `(e:Entity)-[:HAS_INTEREST]->{2,10}(e)` | `(e:Entity)-[:HAS_INTEREST*2..10]->(e)` |
| **Mutual ownership** | `(a)-[r1:HAS_INTEREST]->(b)-[r2:HAS_INTEREST]->(a)` | Same pattern |
| **Root entities** | `WHERE NOT EXISTS { MATCH ()-[:HAS_INTEREST]->(parent) }` | `WHERE NOT EXISTS { MATCH ()-[:HAS_INTEREST]->(parent) }` |

## Installation

```bash
pip install -e .

# With dev dependencies (pytest)
pip install -e ".[dev]"
```

## Quick start

### 1. Convert BODS data to CSV tables

```bash
bods-gql to-csv data/bods-statements.json --output-dir output/
```

This produces `entity_nodes.csv`, `person_nodes.csv`, and `ownership_edges.csv` ready for BigQuery upload.

### 2. Generate property graph DDL

```bash
# For your own tables
bods-gql schema --dataset my_project.my_dataset

# For existing bodsdata BigQuery datasets
bods-gql bodsdata-schema --source gleif_version_0_4
```

### 3. Generate GQL queries

```bash
# UBO detection
bods-gql query -d my_project.my_dataset -q find-ubos

# Corporate groups
bods-gql query -d my_project.my_dataset -q corporate-group

# Circular ownership
bods-gql query -d my_project.my_dataset -q find-cycles

# All 14 query types:
#   find-owners, find-owned, find-ubos, find-ubos-sql,
#   entities-without-ubos, corporate-group, top-parents,
#   jurisdiction-analysis, group-metrics, all-groups,
#   find-cycles, check-cycle, mutual-ownership, cycle-stats
```

### 4. Load directly into BigQuery (requires credentials)

```bash
bods-gql load data/bods-statements.json \
    --project my-gcp-project \
    --dataset bods_ownership
```

## Using existing bodsdata BigQuery datasets

BODS data is already available in BigQuery via the [bodsdata](https://bods-data.openownership.org/) project:

- `bodsdata.gleif_version_0_4` — GLEIF corporate ownership (~6M relationships, ~4M entities)
- `bodsdata.uk_version_0_4` — UK Companies House PSC data (includes natural persons)

To query this data with GQL:

1. Create a property graph in your own project pointing to the bodsdata tables:

```sql
CREATE OR REPLACE PROPERTY GRAPH `your_project.your_dataset`.GLEIFOwnershipGraph
  NODE TABLES (
    `bodsdata.gleif_version_0_4`.entity_statement
      KEY (recordId)
      LABEL Entity
      PROPERTIES (recordId, name, entityType, jurisdiction, foundingDate)
  )
  EDGE TABLES (
    `bodsdata.gleif_version_0_4`.relationship_statement
      KEY (recordId)
      SOURCE KEY (interestedParty) REFERENCES Entity (recordId)
      DESTINATION KEY (subject) REFERENCES Entity (recordId)
      LABEL HAS_INTEREST
      PROPERTIES (recordId, statementDate, recordStatus)
  );
```

2. Run GQL queries:

```sql
-- Find top-level parent entities with most subsidiaries
GRAPH `your_project.your_dataset`.GLEIFOwnershipGraph
MATCH (parent:Entity)-[:HAS_INTEREST]->+(sub:Entity)
WHERE NOT EXISTS {
    MATCH ()-[:HAS_INTEREST]->(parent)
}
RETURN
    parent.name AS parent_name,
    parent.jurisdiction AS jurisdiction,
    COUNT(DISTINCT sub) AS subsidiary_count
ORDER BY subsidiary_count DESC
LIMIT 20;
```

See [`examples/bigquery_existing_data.sql`](examples/bigquery_existing_data.sql) for more queries.

## Effective ownership calculation

Calculating effective (indirect) ownership requires multiplying share percentages along ownership chains. For example:

```
Person owns 75% of Entity A
Entity A owns 60% of Entity B
→ Person's effective ownership of Entity B = 75% × 60% = 45%
```

In Neo4j/Cypher, this uses `reduce()` over path relationships. In GQL, horizontal aggregation over variable-length path edges has limited support in BigQuery as of 2026. This tool provides two approaches:

1. **Pure GQL** (`find-ubos`) — returns per-edge share values for post-processing
2. **GRAPH_TABLE + SQL** (`find-ubos-sql`) — embeds GQL in SQL for threshold filtering

As BigQuery's GQL support matures, native horizontal aggregation will enable fully in-graph effective ownership calculation.

## Project structure

```
bods-gql/
├── src/bods_gql/
│   ├── cli.py                          # CLI entry point
│   ├── converter/
│   │   ├── reader.py                   # Read BODS JSON/JSONL
│   │   ├── mapper.py                   # Map BODS → node/edge tables
│   │   └── bigquery_loader.py          # Load into BigQuery
│   ├── graph_schema/
│   │   └── property_graph.py           # CREATE PROPERTY GRAPH DDL
│   ├── queries/
│   │   ├── ubo_detection.py            # 5 UBO queries
│   │   ├── corporate_groups.py         # 5 corporate group queries
│   │   └── circular_ownership.py       # 4 circular ownership queries
│   └── utils/
│       └── bods_schema.py              # BODS v0.4 constants
├── tests/                              # 59 tests
├── examples/
│   ├── bigquery_existing_data.sql      # Queries for bodsdata BigQuery
│   ├── sample_queries.sql              # All 14 GQL queries
│   └── load_and_query.py              # End-to-end Python example
└── pyproject.toml
```

## Prerequisites

- Python >= 3.9
- Google Cloud BigQuery **Enterprise or Enterprise Plus** edition (required for graph queries)
- Google Cloud credentials for BigQuery loading (`gcloud auth application-default login`)

## Testing

```bash
pytest
```

### Conformance against the shared BODS v0.4 fixture pack

`tests/test_bods_fixtures_conformance.py` runs the BODS-to-graph mapper against every case in the canonical [**bods-v04-fixtures**](https://pypi.org/project/bods-v04-fixtures/) pack via the [**pytest-bods-v04-fixtures**](https://pypi.org/project/pytest-bods-v04-fixtures/) plugin. Tests are parametrized by fixture name so a failure like `[edge-cases/10-circular-ownership]` points straight at the offending case.

Graph-specific conformance checks include: `MappingResult.errors` stays empty across every canonical fixture (shape divergence caught before data lands in BigQuery); circular ownership emits two distinct mirrored edges; and declared-unknown UBOs (inline `unspecifiedReason`) survive into `interested_party_unspecified_json` rather than being silently dropped.

## Related projects

- [bods-v04-fixtures](https://pypi.org/project/bods-v04-fixtures/) — canonical BODS v0.4 conformance fixture pack ([source](https://github.com/StephenAbbott/bods-fixtures))
- [pytest-bods-v04-fixtures](https://pypi.org/project/pytest-bods-v04-fixtures/) — pytest plugin for parametrizing tests over the fixture pack ([source](https://github.com/StephenAbbott/pytest-bods-fixtures))
- [bods-neo4j](https://github.com/StephenAbbott/bods-neo4j) — Bidirectional BODS v0.4 ↔ Neo4j converter with Cypher queries
- [BODS v0.4 Standard](https://standard.openownership.org/)
- [bodsdata](https://bods-data.openownership.org/) — BODS data analysis tools and BigQuery datasets
- [GQL Standard](https://www.gqlstandards.org/) — ISO/IEC 39075:2024
- [BigQuery Graph](https://cloud.google.com/bigquery/docs/graph-overview) — Google Cloud documentation

## Licence

MIT
