#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Escaneja `.codex` per detectar corrupció d'encoding i inconsistències bàsiques.

Pensat per aquest layout:
  .codex/progress/ROADMAP_STATUS.md
  .codex/progress/IMPLEMENTATION_LOG.md
  .codex/prompts/roadmap/*.md

Ús:
  python3 tools/check_codex_integrity.py .codex
  python3 tools/check_codex_integrity.py .codex --strict

Per defecte:
  - detecta mojibake/UTF-8 trencat a tot `.codex`
  - valida que les files de ROADMAP_STATUS.md tinguin fitxer acabat en `.md`
  - comprova que els fitxers citats al roadmap existeixin dins `.codex/prompts/roadmap`, recursivament
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path


MOJIBAKE_RE = re.compile(r"Ã|Â|â€|â€™|â€œ|â€�|â€“|â€”|â€¦|�")
ROADMAP_ROW_RE = re.compile(r"^\|\s*(F[0-9A-Za-z_.-]+)\s*\|\s*`([^`]+)`\s*\|")
MARKDOWN_LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+\.md)(?:#[^)]+)?\)")
BACKTICK_MD_RE = re.compile(r"`([^`]+\.md)`")

TEXT_EXTS = {
    ".md", ".txt", ".json", ".yaml", ".yml", ".toml", ".cfg", ".ini",
    ".go", ".js", ".ts", ".html", ".css", ".sql", ".sh", ".py",
}


def is_probably_binary(data: bytes) -> bool:
    if b"\x00" in data:
        return True
    if not data:
        return False
    sample = data[:4096]
    control = sum(1 for b in sample if b < 9 or (13 < b < 32))
    return control / max(len(sample), 1) > 0.08


def should_scan(path: Path, data: bytes) -> bool:
    if path.suffix.lower() in TEXT_EXTS:
        return True
    if is_probably_binary(data):
        return False
    return path.suffix == ""


def iter_files(root: Path):
    skip_dirs = {".git", "node_modules", "vendor", ".cache", "__pycache__"}
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in skip_dirs]
        for filename in filenames:
            yield Path(dirpath) / filename


def check_utf8_and_mojibake(root: Path, max_examples: int = 5):
    problems = []

    for path in iter_files(root):
        try:
            data = path.read_bytes()
        except OSError as exc:
            problems.append(("read_error", path, str(exc), []))
            continue

        if not should_scan(path, data):
            continue

        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError as exc:
            problems.append(("invalid_utf8", path, str(exc), []))
            continue

        examples = []
        total = 0
        for lineno, line in enumerate(text.splitlines(), start=1):
            if MOJIBAKE_RE.search(line):
                total += 1
                if len(examples) < max_examples:
                    examples.append((lineno, line))

        if total:
            problems.append(("mojibake", path, f"{total} línies afectades; {len(examples)} exemples mostrats", examples))

    return problems


def build_filename_index(target_dir: Path) -> dict[str, list[Path]]:
    index: dict[str, list[Path]] = {}
    if not target_dir.exists():
        return index

    for path in target_dir.rglob("*.md"):
        if path.is_file():
            index.setdefault(path.name, []).append(path)

    return index


def check_roadmap(root: Path, roadmap_target_dir: Path, check_targets: bool):
    problems = []
    roadmap = root / "progress" / "ROADMAP_STATUS.md"

    if not roadmap.exists():
        problems.append(("missing_roadmap", roadmap, "No existeix", []))
        return problems

    text = roadmap.read_text(encoding="utf-8", errors="replace")
    bad_md = []
    missing_files = []
    duplicated_targets = []

    filename_index = build_filename_index(roadmap_target_dir) if check_targets else {}

    for lineno, line in enumerate(text.splitlines(), start=1):
        m = ROADMAP_ROW_RE.match(line)
        if not m:
            continue

        phase = m.group(1).strip()
        filename = m.group(2).strip()

        if not filename.endswith(".md"):
            bad_md.append((lineno, f"{phase}: `{filename}` no acaba en .md"))
            continue

        if check_targets:
            matches = filename_index.get(filename, [])
            if not matches:
                missing_files.append((lineno, f"{phase}: falta `{filename}` dins {roadmap_target_dir}"))
            elif len(matches) > 1:
                paths = ", ".join(str(p) for p in matches[:5])
                duplicated_targets.append((lineno, f"{phase}: `{filename}` existeix més d'un cop: {paths}"))

    if bad_md:
        problems.append(("roadmap_bad_md", roadmap, f"{len(bad_md)} files sense .md", bad_md[:80]))

    if check_targets:
        if not roadmap_target_dir.exists():
            problems.append(("roadmap_target_dir_missing", roadmap_target_dir, "No existeix el directori de prompts roadmap", []))

        if missing_files:
            problems.append((
                "roadmap_missing_files",
                roadmap,
                f"{len(missing_files)} fitxers citats no existeixen dins {roadmap_target_dir}",
                missing_files[:80],
            ))

        if duplicated_targets:
            problems.append((
                "roadmap_duplicate_files",
                roadmap,
                f"{len(duplicated_targets)} fitxers citats existeixen duplicats dins {roadmap_target_dir}",
                duplicated_targets[:80],
            ))

    return problems


def check_internal_md_links(root: Path):
    problems = []

    for path in iter_files(root):
        if path.suffix.lower() != ".md":
            continue

        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeError:
            continue

        broken = []

        for lineno, line in enumerate(text.splitlines(), start=1):
            candidates = []

            for m in MARKDOWN_LINK_RE.finditer(line):
                href = m.group(2).strip()
                if href.startswith(("http://", "https://", "#", "mailto:")):
                    continue
                candidates.append(href.split("#", 1)[0])

            for m in BACKTICK_MD_RE.finditer(line):
                href = m.group(1).strip()
                # Evita tractar les files del ROADMAP_STATUS com a links relatius.
                if path.name == "ROADMAP_STATUS.md" and ROADMAP_ROW_RE.search(line):
                    continue
                candidates.append(href)

            for href in candidates:
                if not href or href.startswith("/"):
                    continue

                target = (path.parent / href).resolve()
                if not target.exists():
                    broken.append((lineno, href))

        if broken:
            problems.append(("broken_md_ref", path, f"{len(broken)} referències .md no trobades", broken[:30]))

    return problems


def print_problem(kind: str, path: Path, summary: str, examples):
    print(f"[FAIL] {kind}: {path} — {summary}")

    for item in examples:
        if isinstance(item, tuple) and len(item) >= 2:
            lineno, value = item[0], item[1]
            print(f"  L{lineno}: {value}")
        else:
            print(f"  {item}")

    print("")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", nargs="?", default=".codex")
    parser.add_argument("--strict", action="store_true", help="Inclou comprovació de links Markdown interns.")
    parser.add_argument("--no-check-roadmap-targets", action="store_true", help="No comprova que els fitxers del roadmap existeixin.")
    parser.add_argument("--roadmap-target-dir", default=None, help="Directori on buscar els fitxers citats pel roadmap.")
    args = parser.parse_args()

    root = Path(args.root)
    if not root.exists():
        print(f"[ERROR] No existeix: {root}", file=sys.stderr)
        return 2

    roadmap_target_dir = Path(args.roadmap_target_dir) if args.roadmap_target_dir else (root / "prompts" / "roadmap")

    all_problems = []
    all_problems.extend(check_utf8_and_mojibake(root))
    all_problems.extend(check_roadmap(root, roadmap_target_dir, check_targets=not args.no_check_roadmap_targets))

    if args.strict:
        all_problems.extend(check_internal_md_links(root))

    if not all_problems:
        print(f"[OK] {root} no té mojibake detectable ni inconsistències bàsiques.")
        print(f"[OK] ROADMAP_STATUS.md referencia fitxers existents dins {roadmap_target_dir}.")
        return 0

    for problem in all_problems:
        print_problem(*problem)

    print(f"[ERROR] Problemes detectats: {len(all_problems)}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
