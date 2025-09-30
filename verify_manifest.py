#!/usr/bin/env python3
"""
verify_manifest.py

Batch and single-file verification for EmuleToPlex manifests.

What it does
------------
- Compares each actual manifest (_manifest.json or _manifest_v_XXX.json)
  against the golden manifest (default: .\\prueba\\PlexLibraryObjective\\_manifest_objective.json).
- Produces per-placeholder details and totals by error type.
- Writes results to _results.json (single file) or _results_v_XXX.json (batch),
  placed in the same folder as the input manifest.

Error types reported:
- wrong_type       : decision/classification mismatch (movie/tv/movie_kids/unclassified)
- name_problem     : normalized title or final dest basename mismatch
- wrong_year       : year mismatch (from explicit field or parsed from dest basename)
- unclassified     : actual decided "unclassified" while golden is a valid class
- missing_actual   : placeholder exists in golden but not in actual manifest
- unexpected_actual: placeholder exists in actual but not in golden

A record is counted "healthy" when it has zero errors.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ---------- Utilities ----------

PLACEHOLDER_KEYS = [
    "placeholder", "source", "src", "input", "source_path", "src_path", "name"
]

CLASS_KEYS = ["decision", "type"]

_TITLE_TOKEN_RE = re.compile(r"[A-Za-z0-9]+")
YEAR_RE = re.compile(r"\((19|20)\d{2}\)")

MANIFEST_VERSIONED_RE = re.compile(
    r"^_manifest(?:_v_(?P<ver>[^.]+))?\.json$", re.IGNORECASE
)


def load_json(path: Path) -> Dict[str, Any]:
    """Load JSON from file."""
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_json(obj: Dict[str, Any], path: Path) -> None:
    """Atomically save JSON to file with indentation and UTF-8."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def extract_placeholder(rec: Dict[str, Any]) -> str:
    """
    Derive a stable placeholder identifier for a record.
    Tries several keys, then falls back to the basename of 'dest'.
    """
    for k in PLACEHOLDER_KEYS:
        v = rec.get(k)
        if isinstance(v, str) and v.strip():
            return normalize_path_like(v)
    dest = rec.get("dest") or rec.get("destination")
    if isinstance(dest, str) and dest.strip():
        base = Path(dest).name
        return base
    raise ValueError("Cannot derive placeholder identifier for record")


def normalize_path_like(s: str) -> str:
    """Normalize slashes and collapse repeated separators; keep filename."""
    s = s.strip().replace("\\", "/")
    parts = [p for p in s.split("/") if p]
    return "/".join(parts)


def get_classification(rec: Dict[str, Any]) -> Optional[str]:
    """Return classification/decision as lowercased string if present."""
    for k in CLASS_KEYS:
        v = rec.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip().lower()
    return None


def normalize_title(s: Optional[str]) -> Optional[str]:
    """Normalize title for comparison by lowercasing and tokenizing alphanumerics."""
    if not isinstance(s, str):
        return None
    tokens = _TITLE_TOKEN_RE.findall(s.lower())
    return " ".join(tokens) if tokens else ""


def extract_year_field(rec: Dict[str, Any]) -> Optional[int]:
    """
    Extract year using explicit numeric fields, then from dest basename, then title.
    Accepts (YYYY) patterns in names.
    """
    for key in ("year", "release_year"):
        y = rec.get(key)
        if isinstance(y, int):
            return y
        if isinstance(y, str) and y.isdigit():
            return int(y)
    dest = rec.get("dest") or rec.get("destination")
    if isinstance(dest, str):
        m = YEAR_RE.search(Path(dest).stem)
        if m:
            return int(m.group(0).strip("()"))
    title = rec.get("title")
    if isinstance(title, str):
        m = YEAR_RE.search(title)
        if m:
            return int(m.group(0).strip("()"))
    return None


def basename_no_ext(path_like: Optional[str]) -> Optional[str]:
    """Return the stem (basename without extension) of a path-like string."""
    if not isinstance(path_like, str):
        return None
    return Path(path_like).stem


def titles_match(actual: Dict[str, Any], golden: Dict[str, Any]) -> bool:
    """
    Compare normalized title OR dest basename if title missing.
    Includes fallback that strips (YYYY) segments.
    """
    a_title = normalize_title(actual.get("title"))
    g_title = normalize_title(golden.get("title"))

    if a_title and g_title and a_title == g_title:
        return True

    a_base = normalize_title(basename_no_ext(actual.get("dest")))
    g_base = normalize_title(basename_no_ext(golden.get("dest")))
    if a_base and g_base and a_base == g_base:
        return True

    def strip_year_in_paren(s: Optional[str]) -> Optional[str]:
        if not isinstance(s, str):
            return None
        return YEAR_RE.sub("", s)

    a_base2 = normalize_title(basename_no_ext(strip_year_in_paren(actual.get("dest"))))
    g_base2 = normalize_title(basename_no_ext(strip_year_in_paren(golden.get("dest"))))
    return bool(a_base2 and g_base2 and a_base2 == g_base2)


def years_match(actual: Dict[str, Any], golden: Dict[str, Any]) -> bool:
    """True if years are equal or if either is missing."""
    ay = extract_year_field(actual)
    gy = extract_year_field(golden)
    if ay is None or gy is None:
        return True
    return ay == gy


# ---------- Core comparison ----------

def index_records(records: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Index records by placeholder key for quick lookups."""
    idx: Dict[str, Dict[str, Any]] = {}
    for rec in records:
        try:
            key = extract_placeholder(rec)
        except Exception:
            key = f"__unknown__/{id(rec)}"
        idx[key] = rec
    return idx


def compare_manifests(
    actual: Dict[str, Any], golden: Dict[str, Any]
) -> Tuple[Dict[str, Any], Dict[str, int]]:
    """Compare two manifest dicts and produce a detailed report and totals."""
    actual_idx = index_records(actual.get("records", []))
    golden_idx = index_records(golden.get("records", []))

    keys_all = sorted(set(actual_idx.keys()) | set(golden_idx.keys()))

    results: List[Dict[str, Any]] = []
    totals = {
        "total": 0,
        "healthy": 0,
        "with_errors": 0,
        "wrong_type": 0,
        "name_problem": 0,
        "wrong_year": 0,
        "unclassified": 0,
        "missing_actual": 0,
        "unexpected_actual": 0,
    }

    for key in keys_all:
        a = actual_idx.get(key)
        g = golden_idx.get(key)
        errors: List[str] = []

        if a is None and g is not None:
            errors.append("missing_actual")
        elif a is not None and g is None:
            errors.append("unexpected_actual")
        else:
            a_cls = get_classification(a) or ""
            g_cls = get_classification(g) or ""
            if a_cls != g_cls:
                errors.append("wrong_type")

            if not titles_match(a, g):
                errors.append("name_problem")

            if not years_match(a, g):
                errors.append("wrong_year")

            if a_cls == "unclassified" and g_cls in {"movie", "tv", "movie_kids"}:
                errors.append("unclassified")

        totals["total"] += 1
        if errors:
            totals["with_errors"] += 1
            for e in errors:
                if e in totals:
                    totals[e] += 1
        else:
            totals["healthy"] += 1

        results.append({
            "placeholder": key,
            "actual": summarize_record(a),
            "expected": summarize_record(g),
            "errors": errors
        })

    summary = {
        "total": totals["total"],
        "healthy": totals["healthy"],
        "with_errors": totals["with_errors"],
        "by_error_type": {
            "wrong_type": totals["wrong_type"],
            "name_problem": totals["name_problem"],
            "wrong_year": totals["wrong_year"],
            "unclassified": totals["unclassified"],
            "missing_actual": totals["missing_actual"],
            "unexpected_actual": totals["unexpected_actual"],
        }
    }

    report = {
        "summary": summary,
        "details": results
    }
    return report, totals


def summarize_record(rec: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Compact per-record view for result JSON."""
    if rec is None:
        return None
    return {
        "decision": get_classification(rec),
        "title": rec.get("title"),
        "year": extract_year_field(rec),
        "dest": rec.get("dest") or rec.get("destination")
    }


# ---------- Batch helpers ----------

def derive_results_path(actual_path: Path) -> Path:
    """
    Given a manifest path, return the corresponding results path in the same folder.

    Mapping:
      _manifest.json            -> _results.json
      _manifest_v_123.json      -> _results_v_123.json
    """
    m = MANIFEST_VERSIONED_RE.match(actual_path.name)
    if not m:
        # Fallback: generic replacement
        name = actual_path.stem.replace("_manifest", "_results") + actual_path.suffix
        return actual_path.with_name(name)

    ver = m.group("ver")
    if ver:
        return actual_path.with_name(f"_results_v_{ver}.json")
    return actual_path.with_name("_results.json")


def print_totals_line(prefix: str, report: Dict[str, Any]) -> None:
    """Print a one-line summary for a report with a prefix."""
    s = report["summary"]
    print(
        f"{prefix} Total: {s['total']} | Healthy: {s['healthy']} | With errors: {s['with_errors']} | "
        f"wrong_type: {s['by_error_type']['wrong_type']} | "
        f"name_problem: {s['by_error_type']['name_problem']} | "
        f"wrong_year: {s['by_error_type']['wrong_year']} | "
        f"unclassified: {s['by_error_type']['unclassified']} | "
        f"missing_actual: {s['by_error_type']['missing_actual']} | "
        f"unexpected_actual: {s['by_error_type']['unexpected_actual']}"
    )


def merge_totals(acc: Dict[str, int], s: Dict[str, Any]) -> None:
    """Accumulate totals from a per-file summary into an aggregate dict."""
    acc["total"] += int(s["total"])
    acc["healthy"] += int(s["healthy"])
    acc["with_errors"] += int(s["with_errors"])
    by = s["by_error_type"]
    for k in ["wrong_type", "name_problem", "wrong_year", "unclassified",
              "missing_actual", "unexpected_actual"]:
        acc[k] += int(by[k])


def empty_totals() -> Dict[str, int]:
    """Return a zeroed totals accumulator for batch mode."""
    return {
        "total": 0,
        "healthy": 0,
        "with_errors": 0,
        "wrong_type": 0,
        "name_problem": 0,
        "wrong_year": 0,
        "unclassified": 0,
        "missing_actual": 0,
        "unexpected_actual": 0,
    }


# ---------- CLI ----------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify EmuleToPlex manifest(s) against golden objective."
    )

    mx = parser.add_mutually_exclusive_group()
    mx.add_argument(
        "--actual",
        default=r".\prueba\PlexLibrary\_manifest.json",
        help="Path to a single actual manifest JSON produced by emuletoplex.py",
    )
    mx.add_argument(
        "--actual-glob",
        help=r"Glob pattern for batch mode, e.g. .\prueba\PlexLibrary\_manifest_v_*.json",
    )

    parser.add_argument(
        "--golden",
        default=r".\prueba\PlexLibraryObjective\_manifest_objective.json",
        help="Path to golden objective manifest JSON",
    )
    parser.add_argument(
        "--output",
        default=r".\prueba\PlexLibrary\_results.json",
        help="Output path for single-file mode only. Ignored in batch mode.",
    )

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    golden_path = Path(args.golden)
    if not golden_path.exists():
        raise SystemExit(f"Golden manifest not found: {golden_path}")
    golden = load_json(golden_path)

    # Batch mode
    if args.actual_glob:
        matches = sorted(Path().glob(args.actual_glob))
        if not matches:
            raise SystemExit(f"No manifests matched glob: {args.actual_glob}")

        aggregate = empty_totals()
        print(f"Batch: {len(matches)} file(s) matched")

        for ap in matches:
            if not ap.exists():
                print(f"Skipping missing file: {ap}")
                continue
            # Compare
            actual = load_json(ap)
            report, _ = compare_manifests(actual, golden)
            # Save next to actual manifest
            outp = derive_results_path(ap)
            save_json(report, outp)
            print_totals_line(prefix=f"[{ap.name}]", report=report)
            merge_totals(aggregate, report["summary"])

        # Aggregate line
        agg_report = {
            "summary": {
                "total": aggregate["total"],
                "healthy": aggregate["healthy"],
                "with_errors": aggregate["with_errors"],
                "by_error_type": {
                    "wrong_type": aggregate["wrong_type"],
                    "name_problem": aggregate["name_problem"],
                    "wrong_year": aggregate["wrong_year"],
                    "unclassified": aggregate["unclassified"],
                    "missing_actual": aggregate["missing_actual"],
                    "unexpected_actual": aggregate["unexpected_actual"],
                }
            }
        }
        print_totals_line(prefix="[AGGREGATE]", report=agg_report)
        return

    # Single-file mode (original behavior)
    actual_path = Path(args.actual)
    output_path = Path(args.output)

    if not actual_path.exists():
        raise SystemExit(f"Actual manifest not found: {actual_path}")

    actual = load_json(actual_path)
    report, _ = compare_manifests(actual, golden)
    save_json(report, output_path)
    print_totals_line(prefix="[SINGLE]", report=report)


if __name__ == "__main__":
    main()
