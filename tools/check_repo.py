"""Repository hygiene checks. Used by CI (files gate) and by pre-commit.

What it checks:
* no files that must never be committed (.env, task PDFs, planning notes, keys);
* every YAML / TOML / JSON file parses;
* no CRLF line endings and no files over 1 MiB;
* no Windows-specific temporary files.

Exit code 1 and a readable list of problems when something is wrong.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tomllib
from fnmatch import fnmatch
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
MAX_FILE_BYTES = 1024 * 1024

FORBIDDEN = [
    ".env",
    ".env.*",
    "!.env.example",
    "*.pdf",
    "*.pem",
    "*.key",
    "*.p12",
    "id_rsa*",
    "python-payment-service-blueprint.md",
    "Thumbs.db",
    ".DS_Store",
    "*.pyc",
]


def tracked_files() -> list[Path]:
    git = shutil.which("git") or "git"
    out = subprocess.run(  # noqa: S603 - fixed argument list, no user input
        [git, "ls-files", "-z"], cwd=ROOT, check=True, capture_output=True, text=True
    ).stdout
    return [ROOT / p for p in out.split("\0") if p]


def forbidden(path: Path) -> bool:
    name = path.name
    allowed = any(fnmatch(name, p[1:]) for p in FORBIDDEN if p.startswith("!"))
    if allowed:
        return False
    return any(fnmatch(name, p) for p in FORBIDDEN if not p.startswith("!"))


def check_parses(path: Path) -> str | None:
    try:
        text = path.read_text(encoding="utf-8")
        if path.suffix in {".yml", ".yaml"}:
            yaml.safe_load(text)
        elif path.suffix == ".toml":
            tomllib.loads(text)
        elif path.suffix == ".json":
            json.loads(text)
    except Exception as exc:  # noqa: BLE001 - we only report the problem
        return f"{path.relative_to(ROOT)}: does not parse ({exc})"
    return None


def main() -> int:
    problems: list[str] = []
    for path in tracked_files():
        rel = path.relative_to(ROOT)
        if forbidden(path):
            problems.append(f"{rel}: must not be committed")
            continue
        if not path.exists():
            continue
        if path.stat().st_size > MAX_FILE_BYTES:
            problems.append(f"{rel}: larger than 1 MiB")
        problem = check_parses(path) if path.suffix in {".yml", ".yaml", ".toml", ".json"} else None
        if problem is not None:
            problems.append(problem)
        is_text = path.suffix in {".py", ".md", ".yml", ".yaml", ".toml", ".ini", ".txt", ".cfg"}
        if is_text and b"\r\n" in path.read_bytes():
            problems.append(f"{rel}: has CRLF line endings")

    for problem in problems:
        print(f"ERROR: {problem}")
    if not problems:
        print("repository hygiene: ok")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
