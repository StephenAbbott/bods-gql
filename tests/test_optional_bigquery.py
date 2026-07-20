"""The Google Cloud stack is an optional extra — core imports must not touch it.

Two guarantees, each checked in a subprocess for a genuinely fresh import
state:

1. Importing the core surface (CLI, mapper, reader, property-graph DDL, query
   generators) never imports ``google.*`` — so ``pip install bods-gql``
   without the ``[bigquery]`` extra works for everything except ``load``.
2. When the Google dependencies are absent, importing the loader (or running
   ``bods-gql load``) fails with a message that points at
   ``pip install 'bods-gql[bigquery]'`` rather than a bare ImportError.
   Simulated with a meta-path finder that blocks ``google`` imports, so the
   test holds whether or not the extra is installed in the dev environment.
"""

from __future__ import annotations

import subprocess
import sys

CORE_IMPORT_CHECK = """
import sys
import bods_gql
import bods_gql.cli
from bods_gql.converter.mapper import map_statements
from bods_gql.converter.reader import read_statements
from bods_gql.graph_schema.property_graph import generate_create_graph_ddl
from bods_gql.queries import circular_ownership, corporate_groups, ubo_detection

leaked = sorted(m for m in sys.modules if m == "google" or m.startswith("google."))
assert not leaked, f"core import pulled in Google modules: {leaked}"
print("OK")
"""

BLOCKED_IMPORT_CHECK = """
import importlib.abc
import sys


class _BlockGoogle(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "google" or fullname.startswith("google."):
            raise ImportError(f"blocked for test: {fullname}")
        return None


sys.meta_path.insert(0, _BlockGoogle())

try:
    import bods_gql.converter.bigquery_loader  # noqa: F401
except ImportError as exc:
    assert "bods-gql[bigquery]" in str(exc), f"unhelpful message: {exc}"
    print("OK")
else:
    raise AssertionError("bigquery_loader imported despite google being blocked")
"""


def _run(code: str) -> str:
    proc = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=120
    )
    assert proc.returncode == 0, proc.stderr
    return proc.stdout


def test_core_imports_do_not_require_google():
    assert "OK" in _run(CORE_IMPORT_CHECK)


def test_loader_import_error_points_at_the_extra():
    assert "OK" in _run(BLOCKED_IMPORT_CHECK)
