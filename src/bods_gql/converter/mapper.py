"""Map BODS v0.4 statements to property graph node and edge table rows.

Produces three table types for BigQuery property graph:
- entity_nodes: from entity statements
- person_nodes: from person statements
- ownership_edges: from relationship statements (HAS_INTEREST)
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Iterator

from bods_gql.utils.bods_schema import ENTITY_TYPE_TO_LABEL, ENTITY_SUBTYPE_TO_LABEL


@dataclass
class EntityNode:
    """A node representing a BODS entity statement."""

    record_id: str
    statement_id: str
    statement_date: str | None = None
    record_status: str | None = None
    declaration_subject: str | None = None
    name: str | None = None
    entity_type: str | None = None
    entity_subtype: str | None = None
    entity_type_details: str | None = None
    is_component: bool = False
    jurisdiction_name: str | None = None
    jurisdiction_code: str | None = None
    founding_date: str | None = None
    dissolution_date: str | None = None
    uri: str | None = None
    primary_identifier_id: str | None = None
    primary_identifier_scheme: str | None = None
    registered_address: str | None = None
    registered_country: str | None = None
    publisher_name: str | None = None
    publication_date: str | None = None
    bods_version: str | None = None
    # JSON-serialised complex fields for round-trip fidelity
    identifiers_json: str | None = None
    addresses_json: str | None = None
    alternate_names_json: str | None = None
    source_json: str | None = None
    annotations_json: str | None = None
    # Computed label for GQL
    node_label: str = "Entity"

    def to_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if v is not None}


@dataclass
class PersonNode:
    """A node representing a BODS person statement."""

    record_id: str
    statement_id: str
    statement_date: str | None = None
    record_status: str | None = None
    declaration_subject: str | None = None
    name: str | None = None
    family_name: str | None = None
    given_name: str | None = None
    person_type: str | None = None
    is_component: bool = False
    birth_date: str | None = None
    death_date: str | None = None
    nationality_code: str | None = None
    pep_status: str | None = None
    publisher_name: str | None = None
    publication_date: str | None = None
    bods_version: str | None = None
    # JSON-serialised complex fields
    names_json: str | None = None
    identifiers_json: str | None = None
    addresses_json: str | None = None
    nationalities_json: str | None = None
    political_exposure_json: str | None = None
    source_json: str | None = None
    annotations_json: str | None = None
    node_label: str = "Person"

    def to_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if v is not None}


@dataclass
class OwnershipEdge:
    """An edge representing a BODS relationship statement (HAS_INTEREST)."""

    record_id: str
    statement_id: str
    interested_party: str  # source node record_id
    subject: str  # destination node record_id
    statement_date: str | None = None
    record_status: str | None = None
    declaration_subject: str | None = None
    is_component: bool = False
    interest_types: str | None = None  # comma-separated
    is_beneficial_ownership: bool | None = None
    direct_or_indirect: str | None = None
    share_exact: float | None = None
    share_minimum: float | None = None
    share_maximum: float | None = None
    share_exclusive_minimum: float | None = None
    share_exclusive_maximum: float | None = None
    interest_start_date: str | None = None
    interest_end_date: str | None = None
    publisher_name: str | None = None
    publication_date: str | None = None
    bods_version: str | None = None
    # JSON-serialised complex fields
    interests_json: str | None = None
    component_records_json: str | None = None
    source_json: str | None = None
    annotations_json: str | None = None
    # Unspecified party info
    subject_unspecified_json: str | None = None
    interested_party_unspecified_json: str | None = None
    edge_label: str = "HAS_INTEREST"

    def to_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if v is not None}


@dataclass
class MappingResult:
    """Accumulated mapping results from processing BODS statements."""

    entity_nodes: list[EntityNode] = field(default_factory=list)
    person_nodes: list[PersonNode] = field(default_factory=list)
    ownership_edges: list[OwnershipEdge] = field(default_factory=list)
    errors: list[dict] = field(default_factory=list)


def _extract_publication(stmt: dict) -> tuple[str | None, str | None, str | None]:
    pub = stmt.get("publicationDetails", {})
    publisher_name = pub.get("publisher", {}).get("name")
    publication_date = pub.get("publicationDate")
    bods_version = pub.get("bodsVersion")
    return publisher_name, publication_date, bods_version


def _json_or_none(value) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False)


def _compute_entity_label(entity_type: str | None, entity_subtype: str | None) -> str:
    """Compute the most specific GQL node label for an entity."""
    if entity_subtype and entity_subtype in ENTITY_SUBTYPE_TO_LABEL:
        return ENTITY_SUBTYPE_TO_LABEL[entity_subtype]
    if entity_type and entity_type in ENTITY_TYPE_TO_LABEL:
        return ENTITY_TYPE_TO_LABEL[entity_type]
    return "Entity"


def map_entity(stmt: dict) -> EntityNode:
    """Map a BODS entity statement to an EntityNode."""
    details = stmt.get("recordDetails", {})
    entity_type_obj = details.get("entityType", {})
    jurisdiction = details.get("jurisdiction", {})
    identifiers = details.get("identifiers", [])
    addresses = details.get("addresses", [])

    # Extract first identifier
    primary_id = identifiers[0].get("id") if identifiers else None
    primary_scheme = identifiers[0].get("scheme") if identifiers else None

    # Extract first registered address
    registered = next(
        (a for a in addresses if a.get("type") == "registered"),
        addresses[0] if addresses else None,
    )
    registered_address = registered.get("address") if registered else None
    registered_country = registered.get("country", {}).get("code") if registered else None

    publisher_name, publication_date, bods_version = _extract_publication(stmt)
    entity_type = entity_type_obj.get("type")
    entity_subtype = entity_type_obj.get("subtype")

    return EntityNode(
        record_id=stmt["recordId"],
        statement_id=stmt["statementId"],
        statement_date=stmt.get("statementDate"),
        record_status=stmt.get("recordStatus"),
        declaration_subject=stmt.get("declarationSubject"),
        name=details.get("name"),
        entity_type=entity_type,
        entity_subtype=entity_subtype,
        entity_type_details=entity_type_obj.get("details"),
        is_component=details.get("isComponent", False),
        jurisdiction_name=jurisdiction.get("name"),
        jurisdiction_code=jurisdiction.get("code"),
        founding_date=details.get("foundingDate"),
        dissolution_date=details.get("dissolutionDate"),
        uri=details.get("uri"),
        primary_identifier_id=primary_id,
        primary_identifier_scheme=primary_scheme,
        registered_address=registered_address,
        registered_country=registered_country,
        publisher_name=publisher_name,
        publication_date=publication_date,
        bods_version=bods_version,
        identifiers_json=_json_or_none(identifiers) if identifiers else None,
        addresses_json=_json_or_none(addresses) if addresses else None,
        alternate_names_json=_json_or_none(details.get("alternateNames")),
        source_json=_json_or_none(stmt.get("source")),
        annotations_json=_json_or_none(stmt.get("annotations")),
        node_label=_compute_entity_label(entity_type, entity_subtype),
    )


def map_person(stmt: dict) -> PersonNode:
    """Map a BODS person statement to a PersonNode."""
    details = stmt.get("recordDetails", {})
    names = details.get("names", [])
    nationalities = details.get("nationalities", [])
    political_exposure = details.get("politicalExposure", {})

    # Compute primary name
    name = None
    family_name = None
    given_name = None
    if names:
        first_name = names[0]
        name = first_name.get("fullName")
        family_name = first_name.get("familyName")
        given_name = first_name.get("givenName")
        if not name and (given_name or family_name):
            parts = [p for p in [given_name, family_name] if p]
            name = " ".join(parts)

    nationality_code = nationalities[0].get("code") if nationalities else None
    pep_status = political_exposure.get("status") if political_exposure else None
    publisher_name, publication_date, bods_version = _extract_publication(stmt)

    return PersonNode(
        record_id=stmt["recordId"],
        statement_id=stmt["statementId"],
        statement_date=stmt.get("statementDate"),
        record_status=stmt.get("recordStatus"),
        declaration_subject=stmt.get("declarationSubject"),
        name=name,
        family_name=family_name,
        given_name=given_name,
        person_type=details.get("personType"),
        is_component=details.get("isComponent", False),
        birth_date=details.get("birthDate"),
        death_date=details.get("deathDate"),
        nationality_code=nationality_code,
        pep_status=pep_status,
        publisher_name=publisher_name,
        publication_date=publication_date,
        bods_version=bods_version,
        names_json=_json_or_none(names) if names else None,
        identifiers_json=_json_or_none(details.get("identifiers")),
        addresses_json=_json_or_none(details.get("addresses")),
        nationalities_json=_json_or_none(nationalities) if nationalities else None,
        political_exposure_json=_json_or_none(political_exposure) if political_exposure else None,
        source_json=_json_or_none(stmt.get("source")),
        annotations_json=_json_or_none(stmt.get("annotations")),
    )


def _extract_interest_summary(interests: list[dict]) -> dict:
    """Extract queryable scalar values from interest array."""
    result: dict = {}
    if not interests:
        return result

    # Collect all interest types
    types = []
    for interest in interests:
        if interest.get("type"):
            types.append(interest["type"])
    if types:
        result["interest_types"] = ",".join(types)

    # Beneficial ownership flag: true if any interest has it
    bo_flags = [i.get("beneficialOwnershipOrControl") for i in interests]
    if any(bo_flags):
        result["is_beneficial_ownership"] = True
    elif any(f is False for f in bo_flags):
        result["is_beneficial_ownership"] = False

    # Direct or indirect from first interest that has it
    for interest in interests:
        if interest.get("directOrIndirect"):
            result["direct_or_indirect"] = interest["directOrIndirect"]
            break

    # Share values from first interest with share data
    for interest in interests:
        share = interest.get("share", {})
        if share:
            if share.get("exact") is not None:
                result["share_exact"] = float(share["exact"])
            if share.get("minimum") is not None:
                result["share_minimum"] = float(share["minimum"])
            if share.get("maximum") is not None:
                result["share_maximum"] = float(share["maximum"])
            if share.get("exclusiveMinimum") is not None:
                result["share_exclusive_minimum"] = float(share["exclusiveMinimum"])
            if share.get("exclusiveMaximum") is not None:
                result["share_exclusive_maximum"] = float(share["exclusiveMaximum"])
            break

    # Date range across all interests
    start_dates = [i["startDate"] for i in interests if i.get("startDate")]
    end_dates = [i["endDate"] for i in interests if i.get("endDate")]
    if start_dates:
        result["interest_start_date"] = min(start_dates)
    if end_dates:
        result["interest_end_date"] = max(end_dates)

    return result


def map_relationship(stmt: dict) -> OwnershipEdge | None:
    """Map a BODS relationship statement to an OwnershipEdge.

    Returns None if the relationship cannot be mapped (missing party references).
    """
    details = stmt.get("recordDetails", {})
    interested_party = details.get("interestedParty")
    subject = details.get("subject")

    # interestedParty and subject can be strings (recordIds) or objects (unspecified)
    interested_party_id = None
    interested_party_unspecified = None
    if isinstance(interested_party, str):
        interested_party_id = interested_party
    elif isinstance(interested_party, dict):
        interested_party_unspecified = interested_party

    subject_id = None
    subject_unspecified = None
    if isinstance(subject, str):
        subject_id = subject
    elif isinstance(subject, dict):
        subject_unspecified = subject

    # Both parties must resolve to record IDs for a valid edge
    if not interested_party_id or not subject_id:
        # Store as edge with placeholder references for unspecified parties
        if not interested_party_id and not subject_id:
            return None

    interests = details.get("interests", [])
    interest_summary = _extract_interest_summary(interests)
    publisher_name, publication_date, bods_version = _extract_publication(stmt)

    return OwnershipEdge(
        record_id=stmt["recordId"],
        statement_id=stmt["statementId"],
        interested_party=interested_party_id or f"_unspecified_{stmt['recordId']}_ip",
        subject=subject_id or f"_unspecified_{stmt['recordId']}_subj",
        statement_date=stmt.get("statementDate"),
        record_status=stmt.get("recordStatus"),
        declaration_subject=stmt.get("declarationSubject"),
        is_component=details.get("isComponent", False),
        interest_types=interest_summary.get("interest_types"),
        is_beneficial_ownership=interest_summary.get("is_beneficial_ownership"),
        direct_or_indirect=interest_summary.get("direct_or_indirect"),
        share_exact=interest_summary.get("share_exact"),
        share_minimum=interest_summary.get("share_minimum"),
        share_maximum=interest_summary.get("share_maximum"),
        share_exclusive_minimum=interest_summary.get("share_exclusive_minimum"),
        share_exclusive_maximum=interest_summary.get("share_exclusive_maximum"),
        interest_start_date=interest_summary.get("interest_start_date"),
        interest_end_date=interest_summary.get("interest_end_date"),
        publisher_name=publisher_name,
        publication_date=publication_date,
        bods_version=bods_version,
        interests_json=_json_or_none(interests) if interests else None,
        component_records_json=_json_or_none(details.get("componentRecords")),
        source_json=_json_or_none(stmt.get("source")),
        annotations_json=_json_or_none(stmt.get("annotations")),
        subject_unspecified_json=_json_or_none(subject_unspecified),
        interested_party_unspecified_json=_json_or_none(interested_party_unspecified),
    )


def map_statements(statements: Iterator[dict]) -> MappingResult:
    """Map a stream of BODS v0.4 statements to graph node and edge tables."""
    result = MappingResult()

    for stmt in statements:
        try:
            record_type = stmt.get("recordType")
            if record_type == "entity":
                result.entity_nodes.append(map_entity(stmt))
            elif record_type == "person":
                result.person_nodes.append(map_person(stmt))
            elif record_type == "relationship":
                edge = map_relationship(stmt)
                if edge:
                    result.ownership_edges.append(edge)
            else:
                result.errors.append({
                    "statementId": stmt.get("statementId"),
                    "error": f"Unknown recordType: {record_type}",
                })
        except (KeyError, TypeError, ValueError) as e:
            result.errors.append({
                "statementId": stmt.get("statementId"),
                "error": str(e),
            })

    return result
