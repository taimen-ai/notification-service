"""White-label guard: the product codename must not creep into the service.

The service is product-neutral (README, AGENTS.md): code, tests, migrations and
current docs carry no brand. The codename survives ONLY in the files listed in
``ALLOWED``, each for a documented reason, and in this guard. Anything else is
a regression. The same guard lives in the core (``tests/unit/test_branding.py``
of control-plane).
"""

import re
import tomllib
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
CODENAME = re.compile(r"taimen", re.IGNORECASE)

# Files allowed to mention the codename — each for a documented reason.
ALLOWED = {
    # Historical decision record (immutable): its rule examples carry the
    # catalog name of the platform package in `apiVersion` (TAI-ADR-0044).
    "docs/adr/0005-notification-rules-as-data.md",
    # Project-level documents of the open-source release name the platform and
    # the umbrella repository they belong to; they are not service contracts.
    "README.md",
    "README.ru.md",
    "CONTRIBUTING.md",
    "SECURITY.md",
    "TRADEMARK.md",
    # This guard itself.
    "tests/test_branding.py",
}

SCAN_GLOBS = [
    "src/**/*.py",
    "src/**/*.json",
    "docs/**/*.md",
    "*.md",
    "pyproject.toml",
    "alembic.ini",
    "migrations/**/*.py",
    "tests/**/*.py",
    "Makefile",
    "Dockerfile",
    ".agents/*.yaml",
]


def test_codename_absent_outside_allowlist() -> None:
    offenders: list[str] = []
    for pattern in SCAN_GLOBS:
        for path in REPO.glob(pattern):
            rel = path.relative_to(REPO).as_posix()
            if rel in ALLOWED or not path.is_file():
                continue
            for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                if CODENAME.search(line):
                    offenders.append(f"{rel}:{lineno}: {line.strip()[:80]}")
    assert not offenders, "codename found outside the allowlist:\n" + "\n".join(offenders)


def test_allowlisted_anchors_still_exist() -> None:
    """When an allowlisted file goes away, drop it from ALLOWED too."""
    for rel in sorted(ALLOWED):
        assert (REPO / rel).is_file(), f"allowlisted file vanished: {rel}"


def test_scan_globs_match_files() -> None:
    """A glob that matches nothing guards nothing: a renamed tree must be followed."""
    empty = [pattern for pattern in SCAN_GLOBS if not any(REPO.glob(pattern))]
    assert not empty, empty


def test_codename_matches_any_case() -> None:
    for sample in ("taimen", "Taimen", "TAIMEN", "x_taimen_bot", "taimen.ai/v1"):
        assert CODENAME.search(sample), sample
    for sample in ("", "notification-service", "example_bot", "tai men"):
        assert not CODENAME.search(sample), sample


def test_entry_points_carry_no_codename() -> None:
    manifest = tomllib.loads((REPO / "pyproject.toml").read_text(encoding="utf-8"))
    assert not CODENAME.search(manifest["project"]["name"])
    scripts = manifest["project"]["scripts"]
    assert not [name for name in scripts if CODENAME.search(name)], scripts
    packages = manifest["tool"]["hatch"]["build"]["targets"]["wheel"]["packages"]
    assert not [name for name in packages if CODENAME.search(name)], packages
