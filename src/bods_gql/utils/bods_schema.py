"""BODS v0.4 schema constants and helpers."""

BODS_VERSION = "0.4"

RECORD_TYPES = ("entity", "person", "relationship")

ENTITY_TYPES = (
    "registeredEntity",
    "legalEntity",
    "arrangement",
    "anonymousEntity",
    "unknownEntity",
    "state",
    "stateBody",
)

ENTITY_SUBTYPES = (
    "governmentDepartment",
    "stateAgency",
    "trust",
    "nomination",
    "other",
)

PERSON_TYPES = ("knownPerson", "anonymousPerson", "unknownPerson")

INTEREST_TYPES = (
    "shareholding",
    "votingRights",
    "appointmentOfBoard",
    "otherInfluenceOrControl",
    "seniorManagingOfficial",
    "settlor",
    "trustee",
    "protector",
    "beneficiaryOfLegalArrangement",
    "rightsToSurplusAssetsOnDissolution",
    "rightsToProfitOrIncome",
    "rightsGrantedByContract",
    "conditionalRightsGrantedByContract",
    "controlViaCompanyRulesOrArticles",
    "controlByLegalFramework",
    "boardMember",
    "boardChair",
    "unknownInterest",
    "unpublishedInterest",
    "enjoymentAndUseOfAssets",
    "rightToProfitOrIncomeFromAssets",
    "nominee",
    "nominator",
)

RECORD_STATUSES = ("new", "updated", "closed")

DIRECT_OR_INDIRECT = ("direct", "indirect", "unknown")

# GQL node labels derived from BODS entity types
ENTITY_TYPE_TO_LABEL = {
    "registeredEntity": "RegisteredEntity",
    "legalEntity": "LegalEntity",
    "arrangement": "Arrangement",
    "anonymousEntity": "AnonymousEntity",
    "unknownEntity": "UnknownEntity",
    "state": "State",
    "stateBody": "StateBody",
}

ENTITY_SUBTYPE_TO_LABEL = {
    "governmentDepartment": "GovernmentDepartment",
    "stateAgency": "StateAgency",
    "trust": "Trust",
    "nomination": "Nomination",
}
