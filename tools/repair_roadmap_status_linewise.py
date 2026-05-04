#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import difflib
import re
import sys
from datetime import datetime
from pathlib import Path

MOJIBAKE_PATTERNS = [
    "Ã", "Â", "â€", "â€™", "â€œ", "â€�", "â€“", "â€”", "â€¦", "�",
    "ƒ", "Æ", "¢", "€š", "Å", "â‚¬", "â„¢",
]
MOJIBAKE_RE = re.compile(r"Ã|Â|â€|â€™|â€œ|â€�|â€“|â€”|â€¦|�")
ROADMAP_ROW_RE = re.compile(r"^(\|\s*F\d+(?:[-.][A-Za-z0-9]+)?\s*\|\s*`)([^`]+)(`\s*\|.*)$")

def mojibake_score(text: str) -> int:
    return sum(text.count(p) for p in MOJIBAKE_PATTERNS)

def sloppy_cp1252_encode(text: str) -> bytes | None:
    out = bytearray()
    for ch in text:
        try:
            out += ch.encode("cp1252")
        except UnicodeEncodeError:
            code = ord(ch)
            if 0x80 <= code <= 0x9F:
                out.append(code)
            else:
                return None
    return bytes(out)

def decode_once(text: str) -> str | None:
    raw = sloppy_cp1252_encode(text)
    if raw is None:
        return None
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("utf-8", errors="replace")

def repair_line_best(line: str, max_iter: int = 10) -> str:
    current = line
    best = line
    best_score = mojibake_score(line)

    for _ in range(max_iter):
        decoded = decode_once(current)
        if decoded is None:
            break

        decoded_score = mojibake_score(decoded)
        if decoded_score < best_score:
            best = decoded
            best_score = decoded_score

        current = decoded

    return best

def repair_text(text: str) -> str:
    repaired = "\n".join(repair_line_best(line) for line in text.splitlines())
    if text.endswith("\n"):
        repaired += "\n"
    return repaired

def fix_missing_md(text: str) -> tuple[str, list[tuple[int, str, str]]]:
    lines = []
    changed = []

    for lineno, line in enumerate(text.splitlines(), start=1):
        m = ROADMAP_ROW_RE.match(line)
        if m and not m.group(2).endswith(".md"):
            old_name = m.group(2)
            new_name = old_name + ".md"
            line = m.group(1) + new_name + m.group(3)
            changed.append((lineno, old_name, new_name))
        lines.append(line)

    return "\n".join(lines) + ("\n" if text.endswith("\n") else ""), changed

def validate(text: str) -> int:
    errors = 0

    mojibake_lines = [
        (lineno, line)
        for lineno, line in enumerate(text.splitlines(), start=1)
        if MOJIBAKE_RE.search(line)
    ]

    bad_md_lines = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        m = ROADMAP_ROW_RE.match(line)
        if m and not m.group(2).endswith(".md"):
            bad_md_lines.append((lineno, m.group(2), line))

    if mojibake_lines:
        errors += 1
        print(f"[FAIL] Mojibake detectat en {len(mojibake_lines)} línies. Primeres 20:")
        for lineno, line in mojibake_lines[:20]:
            print(f"{lineno}:{line}")
    else:
        print("[OK] Sense patrons típics de mojibake.")

    if bad_md_lines:
        errors += 1
        print(f"[FAIL] Files amb fitxer sense .md: {len(bad_md_lines)}")
        for lineno, filename, line in bad_md_lines[:50]:
            print(f"{lineno}: {filename} -> {line}")
    else:
        print("[OK] Totes les files de roadmap detectades acaben en .md.")

    return errors

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("path", nargs="?", default=".codex/progress/ROADMAP_STATUS.md")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--check-only", action="store_true")
    parser.add_argument("--no-fix-md", action="store_true")
    parser.add_argument("--max-diff-lines", type=int, default=240)
    args = parser.parse_args()

    path = Path(args.path)
    original = path.read_text(encoding="utf-8", errors="replace")

    if args.check_only:
        return validate(original)

    repaired = repair_text(original)
    md_fixes = []
    if not args.no_fix_md:
        repaired, md_fixes = fix_missing_md(repaired)

    print("=== Resum ===")
    print(f"Fitxer: {path}")
    print(f"Mojibake score abans:  {mojibake_score(original)}")
    print(f"Mojibake score després: {mojibake_score(repaired)}")
    print(f"Correccions .md: {len(md_fixes)}")
    for lineno, old_name, new_name in md_fixes:
        print(f"  {lineno}: `{old_name}` -> `{new_name}`")
    print("")

    validation_errors = validate(repaired)
    print("")

    if repaired == original:
        print("[INFO] No hi ha canvis a aplicar.")
        return validation_errors

    diff = list(difflib.unified_diff(
        original.splitlines(),
        repaired.splitlines(),
        fromfile=str(path) + " (abans)",
        tofile=str(path) + " (després)",
        lineterm="",
    ))

    print("=== Diff preview ===")
    if args.max_diff_lines and len(diff) > args.max_diff_lines:
        print("\n".join(diff[:args.max_diff_lines]))
        print(f"... diff truncat: {len(diff) - args.max_diff_lines} línies més ...")
    else:
        print("\n".join(diff))

    if not args.apply:
        print("")
        print("[DRY-RUN] No s'ha escrit cap canvi. Reexecuta amb --apply per aplicar.")
        return validation_errors

    backup = path.with_name(path.name + ".bak." + datetime.now().strftime("%Y%m%d_%H%M%S"))
    backup.write_text(original, encoding="utf-8", newline="\n")
    path.write_text(repaired, encoding="utf-8", newline="\n")
    print("")
    print(f"[OK] Backup creat: {backup}")
    print(f"[OK] Fitxer reparat: {path}")
    return validation_errors

if __name__ == "__main__":
    raise SystemExit(main())
