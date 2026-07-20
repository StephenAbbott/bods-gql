"""``bods-gql to-csv`` must produce a stable column schema.

Regression for a bug hit in production by OpenCheck's embedding of the mapper:
``to_dict()`` drops ``None`` values, so mapped row key sets vary with the
data. With fieldnames taken from the first row (the old behaviour), a later
row carrying a column the first row lacked made ``csv.DictWriter`` raise
``dict contains fields not in fieldnames`` — first seen live when BP's first
mapped entity had no jurisdiction and a later one did.
"""

from __future__ import annotations

import csv
import dataclasses
import json

from click.testing import CliRunner

from bods_gql.cli import main
from bods_gql.converter.mapper import EntityNode

# Entity 1 has no jurisdiction; entity 2 does — differing to_dict() key sets.
VARYING_SHAPE_BODS = [
    {"statementId": "e1", "recordId": "e1", "recordType": "entity",
     "recordStatus": "new", "statementDate": "2026-01-01",
     "recordDetails": {"entityType": {"type": "registeredEntity"},
                       "name": "No Jurisdiction Ltd", "isComponent": False}},
    {"statementId": "e2", "recordId": "e2", "recordType": "entity",
     "recordStatus": "new", "statementDate": "2026-01-01",
     "recordDetails": {"entityType": {"type": "registeredEntity"},
                       "name": "Mit Sitz GmbH", "isComponent": False,
                       "jurisdiction": {"name": "Germany", "code": "DE"}}},
]


def test_to_csv_handles_varying_row_shapes(tmp_path):
    bods_file = tmp_path / "bods.json"
    bods_file.write_text(json.dumps(VARYING_SHAPE_BODS))

    result = CliRunner().invoke(
        main, ["to-csv", str(bods_file), "--output-dir", str(tmp_path)]
    )
    assert result.exit_code == 0, result.output

    with open(tmp_path / "entity_nodes.csv", newline="") as f:
        rows = list(csv.DictReader(f))

    assert len(rows) == 2
    # The header is the full dataclass schema, in field order.
    expected = [f.name for f in dataclasses.fields(EntityNode)]
    assert list(rows[0].keys()) == expected
    # The sparse row gets blanks; the richer row keeps its values.
    assert rows[0]["jurisdiction_code"] == ""
    assert rows[1]["jurisdiction_code"] == "DE"
    assert rows[1]["jurisdiction_name"] == "Germany"


def test_to_csv_column_count_is_data_independent(tmp_path):
    """One entity alone produces the same header as the varying-shape pair."""
    solo = tmp_path / "solo.json"
    solo.write_text(json.dumps([VARYING_SHAPE_BODS[0]]))
    out_solo = tmp_path / "solo-out"

    result = CliRunner().invoke(main, ["to-csv", str(solo), "--output-dir", str(out_solo)])
    assert result.exit_code == 0, result.output

    header = (out_solo / "entity_nodes.csv").read_text().splitlines()[0]
    assert header.split(",") == [f.name for f in dataclasses.fields(EntityNode)]
