"""Format CUSP source citations from the release bibliography."""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path

import pandas as pd

CONTAINER_FIELDS = (
    "journal",
    "booktitle",
    "publisher",
    "institution",
    "howpublished",
)


def _unwrap_bibtex_value(value: str) -> str:
    """Remove matching outer brace or quote pairs from a field value."""
    value = value.strip()

    while len(value) >= 2 and (
        (value[0] == "{" and value[-1] == "}") or (value[0] == '"' and value[-1] == '"')
    ):
        value = value[1:-1].strip()

    return value


def _split_bibtex_fields(body: str) -> list[str]:
    """Split a BibTeX entry body at top-level commas."""
    fields: list[str] = []
    current: list[str] = []
    brace_depth = 0
    in_quotes = False
    escaped = False

    for char in body:
        if char == '"' and brace_depth == 0 and not escaped:
            in_quotes = not in_quotes
        elif not in_quotes and not escaped:
            if char == "{":
                brace_depth += 1
            elif char == "}":
                brace_depth -= 1

        if char == "," and brace_depth == 0 and not in_quotes:
            if "".join(current).strip():
                fields.append("".join(current).strip())
            current = []
        else:
            current.append(char)

        escaped = char == "\\" and not escaped
        if char != "\\":
            escaped = False

    if "".join(current).strip():
        fields.append("".join(current).strip())

    return fields


def _iter_bibtex_entries(path: Path) -> list[tuple[str, str]]:
    """Return BibTeX keys and full entry text in source-file order."""
    text = path.read_text(encoding="utf-8")
    entries: list[tuple[str, str]] = []
    current_lines: list[str] = []
    current_key: str | None = None
    brace_balance = 0

    for line in text.splitlines(keepends=True):
        stripped = line.lstrip()

        if current_key is None:
            if not stripped.startswith("@"):
                continue

            current_lines = [line]
            brace_balance = line.count("{") - line.count("}")
            header = stripped.split("{", 1)

            if len(header) != 2 or "," not in header[1]:
                raise ValueError(f"Could not parse BibTeX entry header: {line.strip()}")

            current_key = header[1].split(",", 1)[0].strip()

            if brace_balance == 0:
                entries.append((current_key, "".join(current_lines).strip() + "\n"))
                current_lines = []
                current_key = None
        else:
            current_lines.append(line)
            brace_balance += line.count("{") - line.count("}")

            if brace_balance == 0:
                entries.append((current_key, "".join(current_lines).strip() + "\n"))
                current_lines = []
                current_key = None

    if current_key is not None:
        raise ValueError(f"Unterminated BibTeX entry for key: {current_key}")

    return entries


def parse_bibtex_entries(path: Path) -> dict[str, list[dict[str, str]]]:
    """Parse a BibTeX file, keeping every entry for a repeated cite-key."""
    records: dict[str, list[dict[str, str]]] = defaultdict(list)

    for source, entry_text in _iter_bibtex_entries(path):
        header = re.match(
            r"\s*@(?P<entrytype>[^\s{]+)\s*\{\s*[^,]+,",
            entry_text,
            flags=re.DOTALL,
        )

        if header is None:
            raise ValueError(f"Could not parse BibTeX entry header for key: {source}")

        body = entry_text[header.end() :].rstrip()

        if not body.endswith("}"):
            raise ValueError(f"Could not parse BibTeX entry body for key: {source}")

        fields = {"entrytype": header.group("entrytype").lower()}

        for field in _split_bibtex_fields(body[:-1]):
            if "=" not in field:
                continue

            name, value = field.split("=", 1)
            fields[name.strip().lower()] = _unwrap_bibtex_value(value)

        records[source].append(fields)

    return dict(records)


def format_citation(fields: dict[str, str]) -> str:
    """Build a single-line citation from parsed BibTeX fields.

    Empty pieces are omitted. The container is the first of journal,
    booktitle, publisher, institution, or howpublished. A DOI is preferred
    over a URL and is written as ``https://doi.org/{doi}`` unless it is
    already a URL.
    """
    author = _unwrap_bibtex_value(fields.get("author") or "")
    year = (fields.get("year") or "").strip()
    title = _unwrap_bibtex_value(fields.get("title") or "")
    container = next(
        (
            _unwrap_bibtex_value(fields[name])
            for name in CONTAINER_FIELDS
            if (fields.get(name) or "").strip()
        ),
        "",
    )

    if author and year:
        head = f"{author} ({year})"
    else:
        head = author or (f"({year})" if year else "")

    doi = (fields.get("doi") or "").strip()
    url = (fields.get("url") or "").strip()

    if doi:
        link = doi if doi.startswith("http") else f"https://doi.org/{doi}"
    else:
        link = url

    return ". ".join(piece for piece in (head, title, container, link) if piece)


def citations_by_source(bib_path: Path) -> dict[str, str]:
    """Map each cite-key to a formatted citation.

    Duplicate keys are joined with `` | `` so every matching entry is kept.
    """
    formatted: dict[str, str] = {}

    for source, entries in parse_bibtex_entries(bib_path).items():
        formatted[source] = " | ".join(format_citation(entry) for entry in entries)

    return formatted


def lookup_citations(sources: pd.Series, bib_path: Path) -> pd.Series:
    """Resolve each source key to its formatted citation.

    Raises:
        ValueError: If a source has no BibTeX entry, or a matched entry
            formats to an empty string.
    """
    by_source = citations_by_source(bib_path)
    unique = [
        value for value in sources.dropna().astype(str).str.strip().unique() if value
    ]
    missing = [source for source in unique if source not in by_source]

    if missing:
        raise ValueError(
            f"Missing BibTeX entries for source keys: {', '.join(missing)}"
        )

    empty = [source for source in unique if not by_source[source].strip()]

    if empty:
        raise ValueError(
            f"BibTeX entry produced an empty citation for source keys: {', '.join(empty)}"
        )

    return sources.map(
        lambda value: by_source[str(value).strip()] if pd.notna(value) else pd.NA
    )
