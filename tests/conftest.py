"""Shared test fixtures for bods-gql tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest


@pytest.fixture
def sample_entity_statement() -> dict:
    """A minimal BODS v0.4 entity statement."""
    return {
        "statementId": "stmt-entity-001-abcdef1234567890abcdef1234567890",
        "statementDate": "2024-01-15",
        "recordId": "entity-001",
        "recordType": "entity",
        "recordStatus": "new",
        "declarationSubject": "entity-001",
        "recordDetails": {
            "isComponent": False,
            "entityType": {"type": "registeredEntity"},
            "name": "Acme Holdings Ltd",
            "jurisdiction": {"name": "United Kingdom", "code": "GB"},
            "identifiers": [
                {"id": "12345678", "scheme": "GB-COH", "schemeName": "Companies House"}
            ],
            "addresses": [
                {
                    "type": "registered",
                    "address": "123 Main Street, London",
                    "postCode": "EC1A 1BB",
                    "country": {"name": "United Kingdom", "code": "GB"},
                }
            ],
            "foundingDate": "2010-03-25",
        },
        "publicationDetails": {
            "publicationDate": "2024-01-15T10:00:00Z",
            "bodsVersion": "0.4",
            "publisher": {"name": "Companies House"},
        },
    }


@pytest.fixture
def sample_person_statement() -> dict:
    """A minimal BODS v0.4 person statement."""
    return {
        "statementId": "stmt-person-001-abcdef1234567890abcdef1234567890",
        "statementDate": "2024-01-15",
        "recordId": "person-001",
        "recordType": "person",
        "recordStatus": "new",
        "declarationSubject": "entity-001",
        "recordDetails": {
            "isComponent": False,
            "personType": "knownPerson",
            "names": [
                {
                    "type": "legal",
                    "fullName": "Jane Smith",
                    "familyName": "Smith",
                    "givenName": "Jane",
                }
            ],
            "nationalities": [{"name": "United Kingdom", "code": "GB"}],
            "birthDate": "1980-06-15",
        },
        "publicationDetails": {
            "publicationDate": "2024-01-15T10:00:00Z",
            "bodsVersion": "0.4",
            "publisher": {"name": "Companies House"},
        },
    }


@pytest.fixture
def sample_relationship_statement() -> dict:
    """A minimal BODS v0.4 relationship statement."""
    return {
        "statementId": "stmt-rel-001-abcdef12345678901234567890abcdef",
        "statementDate": "2024-01-15",
        "recordId": "rel-001",
        "recordType": "relationship",
        "recordStatus": "new",
        "declarationSubject": "entity-001",
        "recordDetails": {
            "isComponent": False,
            "subject": "entity-001",
            "interestedParty": "person-001",
            "interests": [
                {
                    "type": "shareholding",
                    "directOrIndirect": "direct",
                    "beneficialOwnershipOrControl": True,
                    "share": {"exact": 51.0, "minimum": 51.0, "maximum": 51.0},
                    "startDate": "2015-01-01",
                }
            ],
        },
        "publicationDetails": {
            "publicationDate": "2024-01-15T10:00:00Z",
            "bodsVersion": "0.4",
            "publisher": {"name": "Companies House"},
        },
    }


@pytest.fixture
def sample_ownership_chain() -> list[dict]:
    """A BODS ownership chain: Person -> Entity A -> Entity B.

    Person-001 owns 75% of Entity-A, Entity-A owns 60% of Entity-B.
    Effective ownership: 75% * 60% = 45%.
    """
    return [
        {
            "statementId": "stmt-entb-001-abcdef1234567890abcdef1234567890",
            "statementDate": "2024-01-15",
            "recordId": "entity-b",
            "recordType": "entity",
            "recordStatus": "new",
            "declarationSubject": "entity-b",
            "recordDetails": {
                "isComponent": False,
                "entityType": {"type": "registeredEntity"},
                "name": "Beta Corp",
                "jurisdiction": {"name": "Germany", "code": "DE"},
            },
            "publicationDetails": {
                "publicationDate": "2024-01-15T10:00:00Z",
                "bodsVersion": "0.4",
                "publisher": {"name": "Test Publisher"},
            },
        },
        {
            "statementId": "stmt-enta-001-abcdef1234567890abcdef1234567890",
            "statementDate": "2024-01-15",
            "recordId": "entity-a",
            "recordType": "entity",
            "recordStatus": "new",
            "declarationSubject": "entity-b",
            "recordDetails": {
                "isComponent": False,
                "entityType": {"type": "registeredEntity"},
                "name": "Alpha Holdings",
                "jurisdiction": {"name": "United Kingdom", "code": "GB"},
            },
            "publicationDetails": {
                "publicationDate": "2024-01-15T10:00:00Z",
                "bodsVersion": "0.4",
                "publisher": {"name": "Test Publisher"},
            },
        },
        {
            "statementId": "stmt-per1-001-abcdef1234567890abcdef1234567890",
            "statementDate": "2024-01-15",
            "recordId": "person-1",
            "recordType": "person",
            "recordStatus": "new",
            "declarationSubject": "entity-b",
            "recordDetails": {
                "isComponent": False,
                "personType": "knownPerson",
                "names": [{"fullName": "John Doe"}],
            },
            "publicationDetails": {
                "publicationDate": "2024-01-15T10:00:00Z",
                "bodsVersion": "0.4",
                "publisher": {"name": "Test Publisher"},
            },
        },
        {
            "statementId": "stmt-rel1-001-abcdef1234567890abcdef1234567890",
            "statementDate": "2024-01-15",
            "recordId": "rel-person-a",
            "recordType": "relationship",
            "recordStatus": "new",
            "declarationSubject": "entity-b",
            "recordDetails": {
                "isComponent": False,
                "subject": "entity-a",
                "interestedParty": "person-1",
                "interests": [
                    {
                        "type": "shareholding",
                        "beneficialOwnershipOrControl": True,
                        "share": {"exact": 75.0},
                    }
                ],
            },
            "publicationDetails": {
                "publicationDate": "2024-01-15T10:00:00Z",
                "bodsVersion": "0.4",
                "publisher": {"name": "Test Publisher"},
            },
        },
        {
            "statementId": "stmt-rel2-001-abcdef1234567890abcdef1234567890",
            "statementDate": "2024-01-15",
            "recordId": "rel-a-b",
            "recordType": "relationship",
            "recordStatus": "new",
            "declarationSubject": "entity-b",
            "recordDetails": {
                "isComponent": False,
                "subject": "entity-b",
                "interestedParty": "entity-a",
                "interests": [
                    {
                        "type": "shareholding",
                        "share": {"minimum": 60.0, "maximum": 75.0},
                    }
                ],
            },
            "publicationDetails": {
                "publicationDate": "2024-01-15T10:00:00Z",
                "bodsVersion": "0.4",
                "publisher": {"name": "Test Publisher"},
            },
        },
    ]


@pytest.fixture
def sample_bods_jsonl(tmp_path, sample_ownership_chain) -> Path:
    """Write the sample ownership chain to a JSONL file."""
    path = tmp_path / "sample.jsonl"
    with open(path, "w") as f:
        for stmt in sample_ownership_chain:
            f.write(json.dumps(stmt) + "\n")
    return path


@pytest.fixture
def sample_bods_json(tmp_path, sample_ownership_chain) -> Path:
    """Write the sample ownership chain to a JSON array file."""
    path = tmp_path / "sample.json"
    with open(path, "w") as f:
        json.dump(sample_ownership_chain, f)
    return path
