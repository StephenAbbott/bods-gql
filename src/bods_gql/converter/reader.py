"""Read BODS v0.4 JSON and JSONL files into statement dicts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterator


def read_statements(path: str | Path) -> Iterator[dict]:
    """Yield BODS v0.4 statement dicts from a JSON or JSONL file.

    Supports:
    - JSON array of statements (standard BODS package)
    - JSONL with one statement per line
    """
    path = Path(path)
    with open(path, encoding="utf-8") as f:
        first_char = f.read(1)
        f.seek(0)

        if first_char == "[":
            # JSON array
            statements = json.load(f)
            yield from statements
        else:
            # JSONL: one statement per line
            for line in f:
                line = line.strip()
                if line:
                    yield json.loads(line)


def read_multiple(paths: list[str | Path]) -> Iterator[dict]:
    """Yield statements from multiple BODS files."""
    for path in paths:
        yield from read_statements(path)
