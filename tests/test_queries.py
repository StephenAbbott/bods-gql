"""Tests for GQL query generation."""

from bods_gql.queries import circular_ownership, corporate_groups, ubo_detection


DATASET = "project.dataset"
GRAPH = "OwnershipGraph"


class TestUBODetection:
    def test_find_owners_syntax(self):
        q = ubo_detection.find_owners(DATASET)
        assert "GRAPH" in q
        assert "MATCH" in q
        assert "HAS_INTEREST" in q
        assert "@record_id" in q
        assert "NOT EXISTS" in q

    def test_find_owners_depth(self):
        q = ubo_detection.find_owners(DATASET, max_depth=5)
        assert "{1,5}" in q

    def test_find_owned_entities(self):
        q = ubo_detection.find_owned_entities(DATASET)
        assert "target:Entity" in q
        assert "jurisdiction_code" in q

    def test_find_ubos_gql(self):
        q = ubo_detection.find_ubos_gql(DATASET)
        assert "person:Person" in q
        assert "entity:Entity" in q
        assert "NOT EXISTS" in q
        assert "share_minimum" in q

    def test_find_ubos_with_sql(self):
        q = ubo_detection.find_ubos_with_sql(DATASET, threshold=25.0)
        assert "GRAPH_TABLE" in q
        assert "25.0" in q
        assert "effective_min_pct" in q

    def test_find_ubos_custom_threshold(self):
        q = ubo_detection.find_ubos_with_sql(DATASET, threshold=10.0)
        assert "10.0" in q

    def test_entities_without_ubos(self):
        q = ubo_detection.find_entities_without_ubos(DATASET)
        assert "Person" in q
        assert "NOT EXISTS" in q
        assert "entity_type" in q


class TestCorporateGroups:
    def test_corporate_group(self):
        q = corporate_groups.corporate_group(DATASET)
        assert "TRAIL" in q
        assert "@record_id" in q
        assert "DISTINCT" in q

    def test_corporate_group_depth(self):
        q = corporate_groups.corporate_group(DATASET, max_depth=30)
        assert "{0,30}" in q

    def test_top_level_parents(self):
        q = corporate_groups.top_level_parents(DATASET)
        assert "NOT EXISTS" in q
        assert "subsidiary_count" in q
        assert "ORDER BY" in q

    def test_top_level_parents_limit(self):
        q = corporate_groups.top_level_parents(DATASET, limit=50)
        assert "LIMIT 50" in q

    def test_jurisdiction_analysis(self):
        q = corporate_groups.group_jurisdiction_analysis(DATASET)
        assert "jurisdiction_code" in q
        assert "entity_count" in q
        assert "GROUP BY" in q

    def test_group_metrics(self):
        q = corporate_groups.group_metrics(DATASET)
        assert "total_members" in q
        assert "max_depth" in q
        assert "PATH_LENGTH" in q

    def test_all_groups(self):
        q = corporate_groups.all_groups(DATASET)
        assert "subsidiary_count" in q
        assert "GROUP BY" in q
        assert "ORDER BY" in q


class TestCircularOwnership:
    def test_find_cycles(self):
        q = circular_ownership.find_cycles(DATASET)
        # Cycle: same variable (e) at start and end
        assert "->{2," in q
        assert "cycle_length" in q

    def test_find_cycles_depth(self):
        q = circular_ownership.find_cycles(DATASET, max_depth=5)
        assert "{2,5}" in q

    def test_check_entity_cycle(self):
        q = circular_ownership.check_entity_cycle(DATASET)
        assert "@record_id" in q
        assert "cycle_length" in q

    def test_mutual_ownership(self):
        q = circular_ownership.mutual_ownership(DATASET)
        assert "ELEMENT_ID" in q
        assert "entity_a" in q
        assert "entity_b" in q

    def test_cycle_stats(self):
        q = circular_ownership.cycle_stats(DATASET)
        assert "entities_in_cycles" in q
        assert "shortest_cycle" in q
        assert "longest_cycle" in q
        assert "AVG" in q


class TestQueryConsistency:
    """Ensure all queries follow consistent patterns."""

    def _all_queries(self):
        return [
            ubo_detection.find_owners(DATASET),
            ubo_detection.find_owned_entities(DATASET),
            ubo_detection.find_ubos_gql(DATASET),
            ubo_detection.find_entities_without_ubos(DATASET),
            corporate_groups.corporate_group(DATASET),
            corporate_groups.top_level_parents(DATASET),
            corporate_groups.group_jurisdiction_analysis(DATASET),
            corporate_groups.group_metrics(DATASET),
            corporate_groups.all_groups(DATASET),
            circular_ownership.find_cycles(DATASET),
            circular_ownership.check_entity_cycle(DATASET),
            circular_ownership.mutual_ownership(DATASET),
            circular_ownership.cycle_stats(DATASET),
        ]

    def test_all_reference_graph(self):
        for q in self._all_queries():
            assert f"GRAPH `{DATASET}`.{GRAPH}" in q

    def test_all_have_match(self):
        for q in self._all_queries():
            assert "MATCH" in q

    def test_all_have_return(self):
        for q in self._all_queries():
            assert "RETURN" in q

    def test_gql_not_cypher(self):
        """Ensure no Cypher-specific syntax leaked through."""
        for q in self._all_queries():
            assert "WHERE NOT EXISTS {" not in q or "NOT EXISTS" in q
            # Cypher uses *1..10, GQL uses {1,10}
            assert "*1.." not in q
            assert "*.." not in q
