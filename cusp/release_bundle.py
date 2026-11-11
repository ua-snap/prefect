"""Build and inspect the exact CUSP archive published on Zenodo."""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo


@dataclass(frozen=True)
class BundleFiles:
    """The three validated files inside a versioned CUSP archive."""

    release_info: Path
    bibliography: Path
    observations: Path


def expected_archive_names(version: str) -> dict[str, str]:
    """Name the only three files allowed inside a release archive."""
    root = f"v{version}"
    return {
        "release_info": f"{root}/RELEASE_INFO.md",
        "bibliography": f"{root}/cusp_sources_v{version}.bib",
        "observations": f"{root}/cusp_v{version}.csv",
    }


def write_release_archive(
    *,
    version: str,
    files: BundleFiles,
    destination: Path,
) -> Path:
    """Write a reproducible ZIP with exactly the three approved files."""
    names = expected_archive_names(version)
    sources = {
        "release_info": files.release_info,
        "bibliography": files.bibliography,
        "observations": files.observations,
    }
    for kind, source in sources.items():
        if source.name != Path(names[kind]).name or not source.is_file():
            raise ValueError(f"Missing or incorrectly named {kind} asset: {source}")

    with ZipFile(destination, "w") as archive:
        for kind in ("release_info", "bibliography", "observations"):
            info = ZipInfo(names[kind], date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            with sources[kind].open("rb") as source, archive.open(info, "w") as target:
                shutil.copyfileobj(source, target)

    return destination


def extract_release_archive(
    *,
    archive_path: Path,
    version: str,
    destination_dir: Path,
) -> BundleFiles:
    """Validate the archive layout and copy its approved entries to disk."""
    names = expected_archive_names(version)
    expected = set(names.values())
    with ZipFile(archive_path) as archive:
        members = [member for member in archive.infolist() if not member.is_dir()]
        actual = [member.filename for member in members]
        directories = {
            member.filename for member in archive.infolist() if member.is_dir()
        }
        if len(actual) != len(expected) or set(actual) != expected:
            raise ValueError(
                f"Zenodo archive must contain exactly {sorted(expected)}; "
                f"found {actual}"
            )
        if directories - {f"v{version}/"}:
            raise ValueError(f"Unexpected archive directories: {sorted(directories)}")

        destination_dir.mkdir(parents=True, exist_ok=True)
        output = {}
        for kind, name in names.items():
            path = destination_dir / Path(name).name
            with archive.open(name) as source, path.open("wb") as target:
                shutil.copyfileobj(source, target)
            output[kind] = path

    return BundleFiles(
        release_info=output["release_info"],
        bibliography=output["bibliography"],
        observations=output["observations"],
    )
