"""Tests for property graph DDL generation."""

from bods_gql.graph_schema.property_graph import (
    generate_create_graph_ddl,
    generate_create_graph_for_bodsdata,
    generate_drop_graph_ddl,
)


class TestCreateGraphDDL:
    def test_contains_create_statement(self):
        ddl = generate_create_graph_ddl("project.dataset")
        assert "CREATE OR REPLACE PROPERTY GRAPH" in ddl
        assert "`project.dataset`.OwnershipGraph" in ddl

    def test_node_tables(self):
        ddl = generate_create_graph_ddl("project.dataset")
        assert "NODE TABLES" in ddl
        assert "LABEL Entity" in ddl
        assert "LABEL Person" in ddl

    def test_edge_tables(self):
        ddl = generate_create_graph_ddl("project.dataset")
        assert "EDGE TABLES" in ddl
        assert "LABEL HAS_INTEREST" in ddl
        assert "SOURCE KEY (interested_party)" in ddl
        assert "DESTINATION KEY (subject)" in ddl

    def test_key_properties(self):
        ddl = generate_create_graph_ddl("project.dataset")
        assert "KEY (record_id)" in ddl
        assert "share_minimum" in ddl
        assert "share_maximum" in ddl
        assert "interest_types" in ddl

    def test_custom_names(self):
        ddl = generate_create_graph_ddl(
            "p.d",
            graph_name="MyGraph",
            entity_table="entities",
            person_table="persons",
            edge_table="edges",
        )
        assert "`p.d`.MyGraph" in ddl
        assert "`p.d`.entities" in ddl
        assert "`p.d`.persons" in ddl
        assert "`p.d`.edges" in ddl

    def test_person_to_entity_edge(self):
        ddl = generate_create_graph_ddl("project.dataset")
        assert "PersonOwnsEntity" in ddl
        assert "REFERENCES Person" in ddl
        assert "REFERENCES Entity" in ddl


class TestBodsdataGraphDDL:
    def test_gleif_dataset(self):
        ddl = generate_create_graph_for_bodsdata("gleif_version_0_4")
        assert "bodsdata.gleif_version_0_4" in ddl
        assert "entity_statement" in ddl
        assert "relationship_statement" in ddl

    def test_uk_dataset(self):
        ddl = generate_create_graph_for_bodsdata("uk_version_0_4")
        assert "bodsdata.uk_version_0_4" in ddl

    def test_contains_join_hint(self):
        ddl = generate_create_graph_for_bodsdata()
        assert "relationship_recorddetails_interests" in ddl


class TestDropGraphDDL:
    def test_drop_statement(self):
        ddl = generate_drop_graph_ddl("project.dataset")
        assert "DROP PROPERTY GRAPH IF EXISTS" in ddl
        assert "`project.dataset`.OwnershipGraph" in ddl

    def test_custom_name(self):
        ddl = generate_drop_graph_ddl("p.d", "CustomGraph")
        assert "CustomGraph" in ddl
