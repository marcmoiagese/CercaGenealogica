#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Repair and validate .codex/progress/ROADMAP_STATUS.md after repeated UTF-8/Windows-1252 mojibake.

Usage:
  python3 tools/repair_roadmap_status_encoding.py .codex/progress/ROADMAP_STATUS.md
  python3 tools/repair_roadmap_status_encoding.py .codex/progress/ROADMAP_STATUS.md --apply
  python3 tools/repair_roadmap_status_encoding.py .codex/progress/ROADMAP_STATUS.md --check-only
  python3 tools/repair_roadmap_status_encoding.py .codex/progress/ROADMAP_STATUS.md --apply --fix-md-names
"""

from __future__ import annotations

import argparse
import difflib
import re
import sys
from datetime import datetime
from pathlib import Path


MOJIBAKE_RE = re.compile(r"Ã|Â|â€|â€™|â€œ|â€�|â€“|â€”|â€¦|�")
# Runs that look like mojibake bytes decoded as Windows-1252.
# We only try to repair a run when it contains one of the mojibake markers.
MOJIBAKE_RUN_RE = re.compile(
    r"[ÃÂâƒÆÐÑÒÓÔÕÖØÙÚÛÜÝÞßàáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿ"
    r"€‚ƒ„…†‡ˆ‰Š‹ŒŽ‘’“”•–—˜™š›œžŸ]+"
)

ROADMAP_ROW_RE = re.compile(r"^\|\s*F\d+(?:[-.]\d+)?\s*\|\s*`([^`]+)`\s*\|")


def mojibake_score(text: str) -> int:
    patterns = [
        "Ã", "Â", "â€", "â€™", "â€œ", "â€�", "â€“", "â€”", "â€¦", "�",
        "Ãƒ", "Ã¢", "Ã‚", "Ã†", "â‚¬", "â„¢",
    ]
    return sum(text.count(p) for p in patterns)


def cp1252_to_utf8_once(text: str) -> str | None:
    try:
        return text.encode("cp1252").decode("utf-8")
    except UnicodeError:
        return None


def repair_whole_text(text: str, max_rounds: int = 12) -> str:
    """Fast path: works when the whole file was corrupted uniformly."""
    current = text
    current_score = mojibake_score(current)

    for _ in range(max_rounds):
        candidate = cp1252_to_utf8_once(current)
        if candidate is None:
            break

        candidate_score = mojibake_score(candidate)
        if candidate_score >= current_score:
            break

        current = candidate
        current_score = candidate_score
        if current_score == 0:
            break

    return current


def repair_mojibake_runs(text: str, max_rounds: int = 12) -> str:
    """Safer fallback: repairs only suspicious mojibake-looking runs."""
    current = text

    for _ in range(max_rounds):
        changed = False

        def repl(match: re.Match[str]) -> str:
            nonlocal changed
            chunk = match.group(0)
            if not MOJIBAKE_RE.search(chunk):
                return chunk

            before_score = mojibake_score(chunk)
            candidate = cp1252_to_utf8_once(chunk)
            if candidate is None:
                return chunk

            after_score = mojibake_score(candidate)
            if after_score < before_score:
                changed = True
                return candidate

            return chunk

        updated = MOJIBAKE_RUN_RE.sub(repl, current)
        current = updated

        if not changed:
            break

    return current


def repair_text(text: str) -> str:
    repaired = repair_whole_text(text)

    # If whole-file decoding did not fully solve it, try targeted chunks.
    if mojibake_score(repaired) > 0:
        repaired = repair_mojibake_runs(repaired)

    return repaired


def find_bad_md_rows(text: str) -> list[tuple[int, str, str]]:
    bad: list[tuple[int, str, str]] = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        m = ROADMAP_ROW_RE.match(line)
        if not m:
            continue
        filename = m.group(1)
        if not filename.endswith(".md"):
            bad.append((lineno, filename, line))
    return bad


def fix_md_names(text: str) -> str:
    lines = text.splitlines(keepends=True)
    out: list[str] = []

    for line in lines:
        m = ROADMAP_ROW_RE.match(line.rstrip("\n"))
        if not m:
            out.append(line)
            continue

        filename = m.group(1)
        if filename.endswith(".md"):
            out.append(line)
            continue

        fixed_line = line.replace(f"`{filename}`", f"`{filename}.md`", 1)
        out.append(fixed_line)

    return "".join(out)


def print_validation(text: str) -> int:
    errors = 0

    bad_mojibake = [
        (lineno, line)
        for lineno, line in enumerate(text.splitlines(), start=1)
        if MOJIBAKE_RE.search(line)
    ]

    bad_md_rows = find_bad_md_rows(text)

    if bad_mojibake:
        errors += 1
        print(f"[FAIL] Mojibake detectat en {len(bad_mojibake)} línies. Primeres 20:")
        for lineno, line in bad_mojibake[:20]:
            print(f"{lineno}:{line}")
    else:
        print("[OK] No s'han detectat patrons típics de mojibake.")

    if bad_md_rows:
        errors += 1
        print(f"[FAIL] Files de roadmap amb nom de fitxer sense .md: {len(bad_md_rows)}. Primeres 50:")
        for lineno, filename, line in bad_md_rows[:50]:
            print(f"{lineno}: {filename} -> {line}")
    else:
        print("[OK] Totes les files de roadmap detectades tenen fitxer acabat en .md.")

    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("path", nargs="?", default=".codex/progress/ROADMAP_STATUS.md")
    parser.add_argument("--apply", action="store_true", help="Escriu el fitxer reparat.")
    parser.add_argument("--check-only", action="store_true", help="Només valida; no intenta reparar.")
    parser.add_argument("--fix-md-names", action="store_true", help="Afegeix .md als noms de fitxer del roadmap que no en tenen.")
    parser.add_argument("--max-diff-lines", type=int, default=240)
    args = parser.parse_args()

    path = Path(args.path)
    if not path.exists():
        print(f"[ERROR] No existeix: {path}", file=sys.stderr)
        return 2

    original = path.read_text(encoding="utf-8", errors="surrogateescape")

    if args.check_only:
        return print_validation(original)

    repaired = repair_text(original)

    if args.fix_md_names:
        repaired = fix_md_names(repaired)

    print("=== Resum ===")
    print(f"Fitxer: {path}")
    print(f"Mojibake score abans:  {mojibake_score(original)}")
    print(f"Mojibake score després: {mojibake_score(repaired)}")
    print("")

    validation_errors = print_validation(repaired)
    print("")

    if repaired == original:
        print("[INFO] No hi ha canvis a aplicar.")
        return validation_errors

    diff = list(
        difflib.unified_diff(
            original.splitlines(),
            repaired.splitlines(),
            fromfile=str(path) + " (abans)",
            tofile=str(path) + " (després)",
            lineterm="",
        )
    )

    print("=== Diff preview ===")
    max_lines = max(args.max_diff_lines, 0)
    if max_lines and len(diff) > max_lines:
        print("\n".join(diff[:max_lines]))
        print(f"... diff truncat: {len(diff) - max_lines} línies més ...")
    else:
        print("\n".join(diff))

    if not args.apply:
        print("")
        print("[DRY-RUN] No s'ha escrit cap canvi. Torna-ho a executar amb --apply per aplicar-ho.")
        return validation_errors

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = path.with_name(path.name + f".bak.{timestamp}")
    backup.write_text(original, encoding="utf-8", errors="surrogateescape")
    path.write_text(repaired, encoding="utf-8", newline="\n")

    print("")
    print(f"[OK] Backup creat: {backup}")
    print(f"[OK] Fitxer reparat: {path}")
    return validation_errors


if __name__ == "__main__":
    raise SystemExit(main())
