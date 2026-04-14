"""Tests for BODS-to-graph mapper."""

import json

from bods_gql.converter.mapper import (
    EntityNode,
    MappingResult,
    OwnershipEdge,
    PersonNode,
    map_entity,
    map_person,
    map_relationship,
    map_statements,
)


class TestMapEntity:
    def test_basic_entity(self, sample_entity_statement):
        node = map_entity(sample_entity_statement)
        assert node.record_id == "entity-001"
        assert node.name == "Acme Holdings Ltd"
        assert node.entity_type == "registeredEntity"
        assert node.jurisdiction_code == "GB"
        assert node.founding_date == "2010-03-25"
        assert node.node_label == "RegisteredEntity"

    def test_entity_identifier(self, sample_entity_statement):
        node = map_entity(sample_entity_statement)
        assert node.primary_identifier_id == "12345678"
        assert node.primary_identifier_scheme == "GB-COH"

    def test_entity_address(self, sample_entity_statement):
        node = map_entity(sample_entity_statement)
        assert node.registered_address == "123 Main Street, London"
        assert node.registered_country == "GB"

    def test_entity_publication(self, sample_entity_statement):
        node = map_entity(sample_entity_statement)
        assert node.publisher_name == "Companies House"
        assert node.bods_version == "0.4"

    def test_entity_json_fields(self, sample_entity_statement):
        node = map_entity(sample_entity_statement)
        identifiers = json.loads(node.identifiers_json)
        assert len(identifiers) == 1
        assert identifiers[0]["id"] == "12345678"

    def test_entity_trust_label(self):
        stmt = {
            "statementId": "stmt-trust-001-abcdef1234567890abcdef1234567890",
            "recordId": "trust-001",
            "recordType": "entity",
            "recordDetails": {
                "isComponent": False,
                "entityType": {"type": "arrangement", "subtype": "trust"},
                "name": "Family Trust",
            },
        }
        node = map_entity(stmt)
        assert node.node_label == "Trust"
        assert node.entity_type == "arrangement"
        assert node.entity_subtype == "trust"


class TestMapPerson:
    def test_basic_person(self, sample_person_statement):
        node = map_person(sample_person_statement)
        assert node.record_id == "person-001"
        assert node.name == "Jane Smith"
        assert node.family_name == "Smith"
        assert node.given_name == "Jane"
        assert node.person_type == "knownPerson"
        assert node.birth_date == "1980-06-15"

    def test_person_nationality(self, sample_person_statement):
        node = map_person(sample_person_statement)
        assert node.nationality_code == "GB"

    def test_person_name_fallback(self):
        """When fullName is missing, construct from parts."""
        stmt = {
            "statementId": "stmt-per-002-abcdef12345678901234567890abcdef",
            "recordId": "person-002",
            "recordType": "person",
            "recordDetails": {
                "isComponent": False,
                "personType": "knownPerson",
                "names": [{"givenName": "Alice", "familyName": "Jones"}],
            },
        }
        node = map_person(stmt)
        assert node.name == "Alice Jones"

    def test_anonymous_person(self):
        stmt = {
            "statementId": "stmt-per-003-abcdef12345678901234567890abcdef",
            "recordId": "person-003",
            "recordType": "person",
            "recordDetails": {
                "isComponent": False,
                "personType": "anonymousPerson",
            },
        }
        node = map_person(stmt)
        assert node.person_type == "anonymousPerson"
        assert node.name is None


class TestMapRelationship:
    def test_basic_relationship(self, sample_relationship_statement):
        edge = map_relationship(sample_relationship_statement)
        assert edge.record_id == "rel-001"
        assert edge.interested_party == "person-001"
        assert edge.subject == "entity-001"
        assert edge.share_exact == 51.0
        assert edge.share_minimum == 51.0
        assert edge.is_beneficial_ownership is True
        assert edge.direct_or_indirect == "direct"
        assert "shareholding" in edge.interest_types

    def test_relationship_dates(self, sample_relationship_statement):
        edge = map_relationship(sample_relationship_statement)
        assert edge.interest_start_date == "2015-01-01"

    def test_unspecified_interested_party(self):
        stmt = {
            "statementId": "stmt-rel-002-abcdef12345678901234567890abcdef",
            "recordId": "rel-002",
            "recordType": "relationship",
            "recordDetails": {
                "isComponent": False,
                "subject": "entity-001",
                "interestedParty": {
                    "reason": "unknown",
                    "description": "Not identified",
                },
                "interests": [],
            },
        }
        edge = map_relationship(stmt)
        assert edge.subject == "entity-001"
        assert edge.interested_party == "_unspecified_rel-002_ip"
        assert edge.interested_party_unspecified_json is not None

    def test_both_unspecified_returns_none(self):
        stmt = {
            "statementId": "stmt-rel-003-abcdef12345678901234567890abcdef",
            "recordId": "rel-003",
            "recordType": "relationship",
            "recordDetails": {
                "isComponent": False,
                "subject": {"reason": "unknown"},
                "interestedParty": {"reason": "unknown"},
                "interests": [],
            },
        }
        edge = map_relationship(stmt)
        assert edge is None

    def test_multiple_interests(self):
        stmt = {
            "statementId": "stmt-rel-004-abcdef12345678901234567890abcdef",
            "recordId": "rel-004",
            "recordType": "relationship",
            "recordDetails": {
                "isComponent": False,
                "subject": "entity-001",
                "interestedParty": "person-001",
                "interests": [
                    {"type": "shareholding", "share": {"minimum": 25.0, "maximum": 50.0}},
                    {"type": "votingRights", "beneficialOwnershipOrControl": True},
                ],
            },
        }
        edge = map_relationship(stmt)
        assert edge.interest_types == "shareholding,votingRights"
        assert edge.share_minimum == 25.0
        assert edge.share_maximum == 50.0
        assert edge.is_beneficial_ownership is True


class TestMapStatements:
    def test_full_chain(self, sample_ownership_chain):
        result = map_statements(iter(sample_ownership_chain))
        assert len(result.entity_nodes) == 2
        assert len(result.person_nodes) == 1
        assert len(result.ownership_edges) == 2
        assert len(result.errors) == 0

    def test_entity_names(self, sample_ownership_chain):
        result = map_statements(iter(sample_ownership_chain))
        names = {n.name for n in result.entity_nodes}
        assert names == {"Beta Corp", "Alpha Holdings"}

    def test_edge_connections(self, sample_ownership_chain):
        result = map_statements(iter(sample_ownership_chain))
        edges = {(e.interested_party, e.subject) for e in result.ownership_edges}
        assert ("person-1", "entity-a") in edges
        assert ("entity-a", "entity-b") in edges

    def test_share_values(self, sample_ownership_chain):
        result = map_statements(iter(sample_ownership_chain))
        person_edge = next(e for e in result.ownership_edges if e.interested_party == "person-1")
        assert person_edge.share_exact == 75.0

        entity_edge = next(e for e in result.ownership_edges if e.interested_party == "entity-a")
        assert entity_edge.share_minimum == 60.0
        assert entity_edge.share_maximum == 75.0

    def test_handles_invalid_record_type(self):
        stmts = [{"statementId": "bad-001", "recordType": "invalid"}]
        result = map_statements(iter(stmts))
        assert len(result.errors) == 1
        assert "Unknown recordType" in result.errors[0]["error"]

    def test_handles_missing_fields(self):
        stmts = [{"recordType": "entity"}]
        result = map_statements(iter(stmts))
        assert len(result.errors) == 1

    def test_to_dict_excludes_none(self, sample_entity_statement):
        node = map_entity(sample_entity_statement)
        d = node.to_dict()
        assert all(v is not None for v in d.values())
