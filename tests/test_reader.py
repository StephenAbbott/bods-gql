"""Tests for BODS v0.4 file reader."""

from bods_gql.converter.reader import read_statements


def test_read_jsonl(sample_bods_jsonl):
    statements = list(read_statements(sample_bods_jsonl))
    assert len(statements) == 5
    assert statements[0]["recordType"] == "entity"
    assert statements[2]["recordType"] == "person"
    assert statements[3]["recordType"] == "relationship"


def test_read_json_array(sample_bods_json):
    statements = list(read_statements(sample_bods_json))
    assert len(statements) == 5
    assert statements[0]["recordId"] == "entity-b"


def test_read_preserves_statement_ids(sample_bods_jsonl):
    statements = list(read_statements(sample_bods_jsonl))
    ids = [s["statementId"] for s in statements]
    assert len(ids) == 5
    assert all(ids)
