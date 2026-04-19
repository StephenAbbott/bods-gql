"""Conformance tests against the shared bods-fixtures pack.

The pack (https://github.com/StephenAbbott/bods-fixtures) is the canonical
source of truth for BODS v0.4 shape across the adapter ecosystem. Passing
these tests means bods-gql's mapper agrees with the canonical envelope
that other adapters also target. Failures here indicate either genuine
bugs in mapping or a fixture-pack bug worth reporting upstream.

Graph-specific concerns tested here:
- the mapper's error list must stay empty across every canonical case —
  GQL load time is a bad place to discover shape divergence.
- circular ownership must produce two distinct HAS_INTEREST edges.
- declared-unknown UBOs (inline ``unspecifiedReason``) must not be
  silently dropped — they're a FATF risk signal in their own right.

The ``bods_fixture`` parameter is auto-parametrized by the
pytest-bods-fixtures plugin over every case in the pack. Tests that need
a specific case use ``load(name)`` directly.
"""

from __future__ import annotations

from bods_fixtures import Fixture, load

from bods_gql.converter.mapper import map_statements


def test_mapper_emits_no_errors_on_canonical_fixtures(bods_fixture: Fixture) -> None:
    """Every statement in every canonical fixture must map cleanly.
    Anything in ``result.errors`` here means the mapper failed to handle
    a shape the spec says is legal."""
    result = map_statements(iter(bods_fixture.statements))
    assert not result.errors, (
        f"{bods_fixture.name}: mapper emitted errors on a canonical fixture: "
        f"{result.errors}"
    )


def test_node_and_edge_counts_match_fixture(bods_fixture: Fixture) -> None:
    """Entity → entity_nodes, person → person_nodes, relationship →
    ownership_edges. A count mismatch means something is being silently
    dropped or misrouted."""
    result = map_statements(iter(bods_fixture.statements))
    assert len(result.entity_nodes) == len(bods_fixture.by_record_type("entity")), (
        f"{bods_fixture.name}: entity node count mismatch"
    )
    assert len(result.person_nodes) == len(bods_fixture.by_record_type("person")), (
        f"{bods_fixture.name}: person node count mismatch"
    )
    assert len(result.ownership_edges) == len(
        bods_fixture.by_record_type("relationship")
    ), f"{bods_fixture.name}: ownership edge count mismatch"


def test_direct_ownership_wires_edge_correctly() -> None:
    """The baseline fixture's edge must point from the person's record_id
    (interestedParty) to the entity's record_id (subject)."""
    fixture = load("core/01-direct-ownership")
    result = map_statements(iter(fixture.statements))

    assert len(result.entity_nodes) == 1
    assert len(result.person_nodes) == 1
    assert len(result.ownership_edges) == 1

    entity = result.entity_nodes[0]
    person = result.person_nodes[0]
    edge = result.ownership_edges[0]

    assert edge.interested_party == person.record_id
    assert edge.subject == entity.record_id


def test_circular_ownership_produces_two_distinct_edges() -> None:
    """A↔B cycle must emit two mirrored HAS_INTEREST edges. Deduplicating
    to one would hide half the cycle in the graph."""
    fixture = load("edge-cases/10-circular-ownership")
    result = map_statements(iter(fixture.statements))
    assert len(result.ownership_edges) == 2

    pairs = {(e.interested_party, e.subject) for e in result.ownership_edges}
    assert len(pairs) == 2, f"expected 2 distinct edges, got {pairs}"

    # The two edges must be mirror images of each other.
    sources = {e.interested_party for e in result.ownership_edges}
    targets = {e.subject for e in result.ownership_edges}
    assert sources == targets, (
        f"cycle edges aren't mirrored: sources={sources}, targets={targets}"
    )


def test_anonymous_interested_party_is_preserved() -> None:
    """Declared-unknown UBO (inline ``unspecifiedReason`` as
    ``interestedParty``) must land in ``interested_party_unspecified_json``
    — not be silently dropped. FATF treats declared-unknown UBOs as a
    risk flag; erasing them from the graph would understate opacity."""
    fixture = load("edge-cases/11-anonymous-person")
    result = map_statements(iter(fixture.statements))

    # No errors — mapper must handle the inline-object shape cleanly.
    assert not result.errors, f"unexpected errors: {result.errors}"

    # The known subject entity must still be a node.
    assert result.entity_nodes, "known subject entity was dropped"

    # The relationship edge must still exist and carry the unspecified
    # reason payload.
    assert result.ownership_edges, "relationship edge dropped"
    edge = result.ownership_edges[0]
    assert edge.interested_party_unspecified_json is not None, (
        "declared-unknown UBO reason was silently dropped from the edge"
    )
    assert "subjectUnableToConfirmOrIdentifyBeneficialOwner" in (
        edge.interested_party_unspecified_json
    ), (
        "unspecifiedReason code did not survive into the edge payload: "
        f"{edge.interested_party_unspecified_json}"
    )
