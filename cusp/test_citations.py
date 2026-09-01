from pathlib import Path

import pandas as pd
import pytest

from cusp.citations import format_citation, lookup_citations


def test_format_citation_assembles_author_year_title_container_and_doi():
    citation = format_citation(
        {
            "author": "Brown, J. and Nelson, F.",
            "year": "1998",
            "title": "Active layer and permafrost properties",
            "publisher": "National Snow and Ice Data Center",
            "doi": "10.7265/w3wq-st79",
        }
    )

    assert citation == (
        "Brown, J. and Nelson, F. (1998). "
        "Active layer and permafrost properties. "
        "National Snow and Ice Data Center. "
        "https://doi.org/10.7265/w3wq-st79"
    )


def test_format_citation_skips_empty_fields():
    citation = format_citation(
        {
            "author": "Schwenk, Jon",
            "year": "2024",
            "title": "Poker Flats Research Range observations, unpublished",
        }
    )

    assert citation == (
        "Schwenk, Jon (2024). Poker Flats Research Range observations, unpublished"
    )


def test_format_citation_prefers_journal_then_uses_doi_over_url():
    citation = format_citation(
        {
            "author": "Bonnaventure, Philip P.",
            "year": "2026",
            "title": "Modeling Permafrost Distribution",
            "journal": "Permafrost and Periglacial Processes",
            "publisher": "Should not appear",
            "doi": "10.1002/ppp.70037",
            "url": "https://example.invalid/ignored",
        }
    )

    assert citation == (
        "Bonnaventure, Philip P. (2026). "
        "Modeling Permafrost Distribution. "
        "Permafrost and Periglacial Processes. "
        "https://doi.org/10.1002/ppp.70037"
    )


def test_format_citation_falls_back_to_url_when_doi_is_absent():
    citation = format_citation(
        {
            "author": "{USDA Natural Resources Conservation Service}",
            "year": "2026",
            "title": "NCSS Lab Data Mart",
            "publisher": "USDA Natural Resources Conservation Service",
            "url": "https://ncsslabdatamart.sc.egov.usda.gov/",
        }
    )

    assert citation == (
        "USDA Natural Resources Conservation Service (2026). "
        "NCSS Lab Data Mart. "
        "USDA Natural Resources Conservation Service. "
        "https://ncsslabdatamart.sc.egov.usda.gov/"
    )


def test_lookup_joins_duplicate_cite_keys_with_a_pipe(tmp_path):
    bib = tmp_path / "cusp_sources.bib"
    bib.write_text(
        """
@dataset{Chapin_2025,
 author = {Chapin, F.S.},
 year = {2002},
 title = {Survey Line Fire},
 publisher = {Environmental Data Initiative},
}

@dataset{Chapin_2025,
 author = {Chapin, F.S.},
 year = {2004},
 title = {Boundary Fire},
 publisher = {Environmental Data Initiative},
}
""",
        encoding="utf-8",
    )

    citations = lookup_citations(pd.Series(["Chapin_2025"]), bib)

    assert citations.tolist() == [
        "Chapin, F.S. (2002). Survey Line Fire. Environmental Data Initiative"
        " | "
        "Chapin, F.S. (2004). Boundary Fire. Environmental Data Initiative"
    ]


def test_lookup_fails_when_a_source_has_no_bib_entry(tmp_path):
    bib = tmp_path / "cusp_sources.bib"
    bib.write_text(
        """
@dataset{CALM,
 author = {Streletskiy, Dmitry A},
 year = {2025},
 title = {GTN-P CALM},
}
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Missing BibTeX entries"):
        lookup_citations(pd.Series(["CALM", "NCSS"]), bib)


def test_lookup_fails_when_a_matched_entry_formats_empty(tmp_path):
    bib = tmp_path / "cusp_sources.bib"
    bib.write_text(
        """
@dataset{EmptySource,
}
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="empty citation"):
        lookup_citations(pd.Series(["EmptySource"]), bib)
