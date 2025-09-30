# -*- coding: utf-8 -*-
"""
EmuleToPlex runner (Windows-friendly)

Watches eMule "Incoming" folders (or processes test placeholders), classifies
media as Movie or TV with robust preprocessing and optional metadata
normalization, then renames and moves files into a Plex-friendly structure and
optionally triggers a Plex library refresh.

Key features
------------
- Preprocessing pipeline removes release junk, numeric prefixes, aspect ratios,
  duration tokens, bracketed tags, and normalizes separators before parsing.
- Metadata normalization (TMDb preferred; OMDb optional) to disambiguate titles,
  choose correct type and year, and return localized titles.
- Unicode handling with strategy-driven sanitation of non-Latin characters
  (transliterate, drop, or keep), with a strict ASCII whitelist if desired.
- Safe "Unclassified" sink for low-confidence or unresolved items.
- Test mode: accepts .txt placeholders, disables Plex calls, redirects roots,
  and can run one-shot classification without watchdog loops.

CLI
---
python emuletoplex_runner.py [--test-mode] [--test-source PATH]
                             [--test-output-root PATH] [--once] [--dry-run]

Configuration
-------------
config.yaml or config.example.yaml with sections:
- emule.watch_paths, emule.stable_seconds, emule.allowed_extensions,
  emule.sidecar_extensions
- plex.base_url, plex.token, plex.movies_section_id, plex.shows_section_id,
  plex.movies_root, plex.shows_root
- renamer.* (see README and comments below)
- metadata.* (TMDb key etc.)
- folders.unclassified_root
"""

from __future__ import annotations

import sys
import json
import argparse
import logging
import os
import random
import re
import requests
import shutil
import threading
import time
import unicodedata
import yaml


from collections import defaultdict
from dataclasses import dataclass
from dataclasses import asdict
from functools import lru_cache
from logging.handlers import RotatingFileHandler
from pathlib import Path
#from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
from urllib.parse import quote

#from guessit import guessit
#from pymediainfo import MediaInfo
#from rapidfuzz import fuzz
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union, Protocol, Set, runtime_checkable
from unidecode import unidecode
from watchdog.events import FileCreatedEvent, FileMovedEvent, FileSystemEventHandler
from watchdog.observers import Observer

try:
    from guessit import guessit  # type: ignore[reportMissingImports]
except ImportError as e:
    raise RuntimeError(
        "Missing dependency 'guessit'. Install it in THIS interpreter:  pip install guessit"
    ) from e

try:
    from rapidfuzz import fuzz  # type: ignore[reportMissingImports]
except ImportError as e:
    raise RuntimeError(
        "Missing dependency 'rapidfuzz'. Install it:  pip install rapidfuzz"
    ) from e

@runtime_checkable
class _ObserverLike(Protocol):
    def schedule(self, handler, path: str, recursive: bool = False) -> None: ...
    def start(self) -> None: ...
    def stop(self) -> None: ...
    def join(self, timeout: float | None = ...) -> None: ...

# ------------------------------ Configuration -------------------------------

def _deep_merge(dst: Dict[str, Any], src: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge src into dst, returning dst."""
    for k, v in (src or {}).items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            _deep_merge(dst[k], v)
        else:
            dst[k] = v
    return dst

def _normalize_aliases(cfg: Dict[str, Any]) -> None:
    """
    Backward-compatible alias mapping:
      - top-level 'plex_token' -> 'plex.token' if not already set
      - top-level 'tmdb_api_key' -> 'metadata.tmdb_api_key' if not already set
    Keeps old files from exploding.
    """
    # Ensure nested containers exist
    if "plex" not in cfg or not isinstance(cfg["plex"], dict):
        cfg["plex"] = {}
    if "metadata" not in cfg or not isinstance(cfg["metadata"], dict):
        cfg["metadata"] = {}

    # Alias: plex_token -> plex.token
    if "plex_token" in cfg and "token" not in cfg["plex"]:
        cfg["plex"]["token"] = cfg.pop("plex_token")

    # Alias: tmdb_api_key (top-level) -> metadata.tmdb_api_key
    if "tmdb_api_key" in cfg and "tmdb_api_key" not in cfg["metadata"]:
        cfg["metadata"]["tmdb_api_key"] = cfg.pop("tmdb_api_key")

def load_config() -> Dict[str, Any]:
    """
    Load config.yaml, then deep-merge secrets.yaml.
    Supports legacy aliases so older flat keys won't crash the party.
    """
    base = Path(__file__).resolve().parent
    cfg = {}

    cfg_file = base / "config.yaml"
    if cfg_file.exists():
        with cfg_file.open("r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}

    secrets_file = base / "secrets.yaml"
    if secrets_file.exists():
        with secrets_file.open("r", encoding="utf-8") as f:
            secrets = yaml.safe_load(f) or {}
            cfg = _deep_merge(cfg, secrets)

    _normalize_aliases(cfg)
    return cfg

# ------------------------------ Logger Definition -------------------------------

logger = logging.getLogger("EmuleToPlex")

# --- Milestone-2 instrumentation: tail trimming counters ---
# ---- Counters (module-global, reset per run) ----

TAIL_TRIMMED_COUNT = 0
TAIL_GUARD_BLOCKED_COUNT = 0
HEAD_CREDIT_DROP_COUNT = 0
HEAD_PERSON_DROP_COUNT = 0
WEAK_TITLE_FALLBACK_COUNT = 0

def reset_tail_counters() -> None:
    """
    Reset uploader-tail trimming counters at the start of each run.
    """
    global TAIL_TRIMMED_COUNT, TAIL_GUARD_BLOCKED_COUNT, WEAK_TITLE_FALLBACK_COUNT
    TAIL_TRIMMED_COUNT = 0
    TAIL_GUARD_BLOCKED_COUNT = 0
    WEAK_TITLE_FALLBACK_COUNT = 0   # ← ADD THIS LINE


KNOWN_PICKS_PATH = Path("known_picks.json")
try:
    _KNOWN_PICK_CACHE: Dict[str, Dict[str, Any]] = json.loads(KNOWN_PICKS_PATH.read_text(encoding="utf-8"))
except Exception:
    _KNOWN_PICK_CACHE = {}
KNOWN_PICK_DIRTY = False


def _save_known_pick_cache() -> None:
    global KNOWN_PICK_DIRTY
    if not KNOWN_PICK_DIRTY:
        return
    try:
        KNOWN_PICKS_PATH.write_text(json.dumps(_KNOWN_PICK_CACHE, ensure_ascii=False, indent=2), encoding="utf-8")
        KNOWN_PICK_DIRTY = False
    except Exception:
        logger.exception("Failed to persist known picks cache")



# Instrumentation counters (Milestone C)
def emit_instrumentation_summary(log: logging.Logger) -> None:
    """
    Print one-line Milestone-C instrumentation counters at the end of the run.
    """
    log.info(
        "C-metrics: tail_trimmed=%d tail_guard_blocked=%d head_credit_drops=%d head_person_drops=%d weak_title_fallbacks=%d",
        TAIL_TRIMMED_COUNT, TAIL_GUARD_BLOCKED_COUNT,
        HEAD_CREDIT_DROP_COUNT, HEAD_PERSON_DROP_COUNT,
        WEAK_TITLE_FALLBACK_COUNT
    )

# ------------------------------ Regex library -------------------------------

ASPECT_RATIO_RE = re.compile(r"\((?:\d{1,2}[.,]\d{2})\)")  # e.g. (1,77) or (1.85)
DURATION_MIN_RE = re.compile(r"\b\d{2,3}'\b")              # e.g. 101'
NUM_PREFIX_RE = re.compile(r"^\s*(?!(?:19|20)\d{2}\b)\d{1,3}[\-_.]+\s+")             # e.g. "2- " or "01. "
BRACKETS_RE = re.compile(r"\[.*?\]")                       # [BRRip] [DUAL] ...

RELEASE_TAGS_RE = re.compile(
    r"""
\b(
    Blu[- ]?Ray|BR[- ]?Rip|BDRip|WEB[- ]?DL|WEB[- ]?Rip|HDRip|DVDRip|Remux|MicroHD|
    x265|x264|XviD|DivX|HEVC|H\.?265|H\.?264|AV1|
    AC3|A[C\- ]?3|EAC3|E[A\- ]?C3|DDP|DD\+|DTS(?:-HD|HD)?|AAC|MP3|FLAC|
    # idiomas y marcas (abreviaturas y en claro)
    MULTI|DUAL|VO(?:SE)?|V\.?O\.?S\.?E|VOS|SUBS?|SUB(?:SPA|ENG|ES|EN)|
    ESPAÑOL|ESPA|ESP|ES|CAST(?:ELLANO)?|
    SPANISH|ENGLISH|FRENCH|GERMAN|ITALIAN|PORTUGUESE|PORTUGUES|PORTUGUÉS|
    JAPANESE|JAPONES|JAPONÉS|CHINESE|CHINO|KOREAN|COREANO|CATALAN|CATALA|CATALÁN|
    LAT(?:INO)?|LATAM|ING(?:L[EÉ]S)?|EN|ENG|ITA|FRA|ALEMAN|DEU|RUSO|RU|CHINO|CHI|JP|JAP|
    # tokens comunes en chino
    国语中字|中字|国配|简体|繁体|中文字幕|
    # resoluciones y calidad
    2160p|1080p|720p|480p|4K|8K|10b(?:it)?|10bit|8bit|HDR10|HDR|HLG|Dolby(?:Vision)?|Atmos|\bHD\b|
    Proper|Repack|Limited|Extended|Director'?s\ *Cut|Unrated|
    \d{3,4}p|[12]\d{2,3}x\d{3,4}|
    \bby\s+\w+\b
)\b
""",
    re.IGNORECASE | re.VERBOSE,
)

DOMAINS_PARENS_RE = re.compile(r"\((?:https?://)?(?:www\.)?[a-z0-9][\w.-]+\.(?:com|net|org|info|ru|to)\)", re.IGNORECASE)
#Dominio en cualquier parte (sin necesidad de paréntesis)
#Detecta tokens de dominio con o sin esquema/WWW, soporta subdominios y varios TLDs
DOMAINS_ANY_RE = re.compile(
    r"(?i)\b(?:https?://)?(?:www\.)?(?:[a-z0-9-]+\.)+(?:com|net|org|info|ru|to|co|es|it|fr)\b"
)

LANG_TAGS_RE = re.compile(
    r"(?i)(?:\b(?:SPANISH|ENGLISH|FRENCH|GERMAN|ITALIAN|PORTUGUESE|PORTUGUES|JAPANESE|CHINESE|KOREAN|CATALAN|CAST(?:ELLANO)?|ESPAÑ?OL|ESP|ENG|EN|ITA|FRA|DEU|VOSE|VOS|SUBS?|LAT(?:AM)?|DUAL|MULTI|BILINGÜE|BILINGUE|DUALAUDIO|MULTIAUDIO)\b|ESP-?ENG|ES-?EN|VO-?LAT(?:INO)?|ESPANOLINGLES|SUBFORZ(?:ADAS?|ADOS?)|VO-?SUB|VO-?ESP|ESP-?ING|SPA-?ENG)"
)

# Person-like full-name list, e.g., "Joaquin Phoenix, Rooney Mara" or "Ana de Armas y Ryan Gosling"
PERSON_NAME_RE = re.compile(
    r"(?:[A-ZÁÉÍÓÚÜÑ][a-záéíóúüñ]+){1,3}(?:\s+(?:de|del|la|da|dos|do|van|von|di|du))?\s+(?:[A-ZÁÉÍÓÚÜÑ][a-záéíóúüñ]+){1,3}"
)

PERSON_LIST_RE = re.compile(
    rf"(?i)\b(?:{PERSON_NAME_RE.pattern})(?:\s*(?:,|y|and|&)\s*(?:{PERSON_NAME_RE.pattern})){{1,5}}\b"
)

# Short ALL-CAPS tail (≤3 tokens) at end of line, e.g., "WEB-DL DDP5 1", "ESP ENG"
ALLCAPS_TAIL_RE = re.compile(
    r"(?:^|[\s\-–—:;])([A-Z0-9]{2,}(?:\s+[A-Z0-9]{2,}){0,2})\s*$"
)

TV_MARKER_RE = re.compile(
    r"(?i)(S\d{1,2}E\d{2}|\b\d{1,2}x\d{2}\b|\bTemporada\b|\bSeason\b|Cap(?:\.|itulo|ítulo)?\s*\d{2,4})"
)

QUALITY_STEM_TOKENS = [
    "2160p","1080p","720p","480p","4k","8k","360p",
    "hdr","hlg","10b","10bit","bdrip","webrip","hdrip","dvdrip","microhd",
    "x264","x265","hevc","h264","ac3","dts","aac","flac",
    "dualaudio","dual-audio","multiaudio","bilingue","vose","latam","latino","italiano","ita","ingles","eng","subs","sub","vost"
]

COMPACT_TOKEN_REPLACEMENTS = (
    # WEB/BD/HD + resolution fused: WEBRip1080p -> WEBRip 1080p
    (re.compile(r"(?i)\b(web|bd|hd|hdr|microhd)(\d{3,4}p)\b"), r"\1 \2"),

    # Resolution followed by "x####" fused: 720pX264 -> 720p x264
    (re.compile(r"(?i)\b(\d{3,4}p)(x\d{3,4})\b"), r"\1 \2"),

    # Codec + resolution fused: x2641080p / h2642160p -> x264 1080p / h264 2160p
    (re.compile(r"(?i)\b(x264|x265|h\.?264|h\.?265)(\s*\d{3,4}p)\b"), r"\1 \2"),

    # Release family + resolution fused: webrip2160p -> webrip 2160p
    (re.compile(r"(?i)\b(webrip|web-?dl|b[dr]rip|hdrip|dvdrip)(\d{3,4}p)\b"), r"\1 \2"),

    # Dual/multi/audio & common language blends fused
    (re.compile(r"(?i)\b(dual)(audio)\b"), r"\1 \2"),
    (re.compile(r"(?i)\b(multi)(audio)\b"), r"\1 \2"),
    (re.compile(r"(?i)\b(vo)(se)\b"), r"\1 \2"),
    (re.compile(r"(?i)\b(vo)(esp)\b"), r"\1 \2"),
    (re.compile(r"(?i)\b(esp)(eng)\b"), r"\1 \2"),

    # Normalize UHD/HDR/DoVi family into stable tokens so later cleaners behave deterministically.
    # ultra hd / uhd -> 4k
    (re.compile(r"(?i)\bultra[-\s]?hd\b"), "4k"),
    (re.compile(r"(?i)\buhd\b"), "4k"),
    # hdr10+ -> hdr10+
    (re.compile(r"(?i)\bhdr10\+\b"), "hdr10+"),
    # dolby vision / dovi variants -> dovi
    (re.compile(r"(?i)\bdolby\s*vision\b"), "dovi"),
    (re.compile(r"(?i)\bdovi\b"), "dovi"),
)

# Normaliza separadores de palabra. Puntos/guiones bajos a espacio.
WORD_SEPS_RE = re.compile(r"[._]+")                        # ".", "_" -> space

# Palabras que delatan que “episodio” en realidad es un día del año
SUS_EP_WORDS_RE = re.compile(r"\b(dias?|día|días|day|jours?|days?)\b", re.IGNORECASE)

# Basura de comienzo tipo dominios/uploader: “www.algo.com - ”, “by Fulano - ”, etc.
START_JUNK_RE = re.compile(
    r"^(?:\s*(?:www\.)?[a-z0-9][\w.-]{1,}\.(?:com|net|org|info|ru|to)\b[^\w]*)|^(?:\s*by\s+\w+\b[^\w]*)",
    re.IGNORECASE,
)

INVALID_WIN_CHARS = r'<>:"/\\|?*'
RESERVED_WIN_NAMES = {
    "CON","PRN","AUX","NUL",
    *(f"COM{i}" for i in range(1,10)),
    *(f"LPT{i}" for i in range(1,10)),
}

CAP_COMPRESSED_RE = re.compile(r"\bCap(?:\.|itulo|ítulo)?\s*(\d{3,4})\b", re.IGNORECASE)

CREDITS_HEAD_RE = re.compile(
    r"(?is)^\s*(?:di|de|by)\s+[^\-:(\[]+?\s+(?:con|with)\s+[^\-:(\[]+?(?=$|[-–;:(\[])"
)

_KNOWN_PICK_CACHE: Dict[str, Dict[str, Any]] = {}
KNOWN_PICK_DIRTY = False
USE_KNOWN_PICKS = False
KNOWN_PICKS_PATH = Path("known_picks.json")

#--------------------------CACHE HELPERS--------------------------------------

def init_known_pick_cache(enabled: bool) -> None:
    """
    Initialize the known-picks cache only when explicitly enabled by config/CLI.
    This avoids leaking state across runs and preserves determinism for “without” runs.
    """
    global _KNOWN_PICK_CACHE, KNOWN_PICK_DIRTY, USE_KNOWN_PICKS
    USE_KNOWN_PICKS = bool(enabled)
    _KNOWN_PICK_CACHE = {}
    KNOWN_PICK_DIRTY = False
    if not USE_KNOWN_PICKS:
        return
    try:
        _KNOWN_PICK_CACHE = json.loads(KNOWN_PICKS_PATH.read_text(encoding="utf-8"))
    except Exception:
        _KNOWN_PICK_CACHE = {}

def _save_known_pick_cache() -> None:
    """
    Persist the known-picks cache if and only if it was enabled and mutated.
    """
    global KNOWN_PICK_DIRTY
    if not USE_KNOWN_PICKS or not KNOWN_PICK_DIRTY:
        return
    try:
        KNOWN_PICKS_PATH.write_text(
            json.dumps(_KNOWN_PICK_CACHE, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )
        KNOWN_PICK_DIRTY = False
    except Exception:
        logger.exception("Failed to persist known picks cache")

# --------------------------- Dataclasses / helpers ---------------------------

def _has_three_alpha(text: str) -> bool:
    """Return True if *text* contains any alphabetic run of length ≥ 3."""
    return bool(re.search(r"[A-Za-z]{3,}", text or ""))


def _two_alpha_words(text: str) -> bool:
    """Return True if *text* has at least two alphabetic words (≥ 3 chars each)."""
    return len(re.findall(r"[A-Za-z]{3,}", text or "")) >= 2


@dataclass
class Dest:
    """Represents a destination decision for a file.

    kind:
        "movie" | "movie_kids" | "tv" | "tv_kids" | "unclassified"
    """
    kind: str
    folder: Path
    file: Path
    title: str

@dataclass
class ManifestRec:
    src: str
    decision: str
    dest: str
    title: Optional[str] = None
    tmdb_id: Optional[int] = None
    score: Optional[int] = None
    error: Optional[str] = None

def write_manifest_and_summary(handler: "IngestHandler",
                               cfg: Dict[str, Any],
                               args: argparse.Namespace,
                               logger: logging.Logger) -> None:
    """
    Escribe _manifest.json deduplicando por 'src' y preferiendo el ÚLTIMO estado
    (p. ej., si la 1ª pasada cae en Unclassified y la 2ª lo rescata).
    """
    all_records = list(getattr(handler, "manifest", []))

    # Optional safety: skip if test source missing
    if getattr(args, "test_mode", False) and getattr(args, "test_source", None):
        if not Path(args.test_source).exists():
            logger.debug("Skipping manifest: test source does not exist: %s", args.test_source)
            return

    def _is_classified(decision: str) -> bool:
        return decision in {"movie", "movie_kids", "tv", "tv_kids"}

    # Reduce by NORMALIZED BASENAME KEY:
    # - Later records override earlier ones
    # - If competing records exist for the same file, prefer a classified decision
    last_by_key: Dict[str, Tuple[int, ManifestRec]] = {}  # key -> (idx, rec)
    for idx, r in enumerate(all_records):
        key = basename_key_from_src(r.src, cfg)

        if key not in last_by_key:
            last_by_key[key] = (idx, r)
            continue

        prev_idx, prev = last_by_key[key]

        # Default: last wins
        take_new = True

        # If previous is classified and this one is unclassified, keep previous
        if _is_classified(prev.decision) and r.decision == "unclassified":
            take_new = False

        # If previous is unclassified and this one is classified, take new
        if prev.decision == "unclassified" and _is_classified(r.decision):
            take_new = True

        if take_new:
            last_by_key[key] = (idx, r)

    # Keep stable ordering by last index (cosmetic)
    records = [pair[1] for pair in sorted(last_by_key.values(), key=lambda p: p[0])]

    # 1) Colapsa por 'src' exacto (el último gana)
    """
    dedup_by_src: dict[str, ManifestRec] = {}
    for r in all_records:
        dedup_by_src[r.src] = r
    records = list(dedup_by_src.values())
    """

    # 2) Colapsa por 'basename' para fusionar cambios de ruta del mismo archivo
    """
    by_basename: dict[str, ManifestRec] = {}
    for r in records:
        key = Path(r.src).name
        by_basename[key] = r  # el último gana
    records = list(by_basename.values())
    """

    # Bonus defensive pass:
    # if both an unclassified and a classified record exist for the same basename,
    # keep the classified one. This aligns the manifest with what’s actually on disk.
    def _is_unclassified(decision: str) -> bool:
        return decision == "unclassified"

    by_name = {}
    for r in records:
        key = Path(r.src).name.casefold()
        if key not in by_name:
            by_name[key] = r
            continue
        # If previous was unclassified and current is classified, replace it
        if _is_unclassified(by_name[key].decision) and not _is_unclassified(r.decision):
            by_name[key] = r
    records = list(by_name.values())
    
    # Resumen por decisión final
    summary = defaultdict(int)
    for r in records:
        summary[r.decision] += 1

    def _stem_from_dest(rec: ManifestRec) -> str:
        return Path(rec.dest).stem if rec.dest else ""

    def _has_name_problem(rec: ManifestRec) -> bool:
        stem = _stem_from_dest(rec)
        if not stem:
            return False
        lower = stem.lower()
        if RELEASE_TAGS_RE.search(stem) or LANG_TAGS_RE.search(stem):
            return True
        if 'UPLOADER_LIST_RE' in globals() and UPLOADER_LIST_RE.search(stem):
            return True
        if re.search(r"_\d+$", stem):
            return True
        compact = lower.replace(" ", "")
        for tok in QUALITY_STEM_TOKENS:
            marker = tok.replace(" ", "").lower()
            if not marker:
                continue
            if re.search(rf"(?:^|[._\-]){re.escape(marker)}(?:$|[._\-])", compact):
                return True
        return False

    def _assigned_year(rec: ManifestRec) -> Optional[int]:
        stem = _stem_from_dest(rec)
        if not stem:
            return None
        m = YEAR_RE.search(stem)
        if not m:
            return None
        try:
            return int(m.group(0).strip("()"))
        except Exception:
            return None

    def _years_in_name(rec: ManifestRec) -> Set[int]:
        return {int(m) for m in re.findall(r"(?:19|20)\d{2}", Path(rec.src).stem)}

    def _has_wrong_year(rec: ManifestRec) -> bool:
        assigned = _assigned_year(rec)
        if assigned is None:
            return False
        years = _years_in_name(rec)
        return (not years) or (assigned not in years)

    def _has_wrong_type(rec: ManifestRec) -> bool:
        has_markers = bool(TV_MARKER_RE.search(Path(rec.src).stem))
        if has_markers:
            return rec.decision not in {"tv", "tv_kids", "unclassified"}
        return rec.decision in {"tv", "tv_kids"}

    name_problem_count = sum(1 for r in records if _has_name_problem(r))
    wrong_year_count = sum(1 for r in records if _has_wrong_year(r))
    wrong_type_count = sum(1 for r in records if _has_wrong_type(r))

    payload = {
        "total": len(records),
        "summary": dict(summary),
        "metrics": {
            "name_problems": name_problem_count,
            "wrong_year": wrong_year_count,
            "wrong_type": wrong_type_count,
            # Milestone-2 instrumentation
            "tail_trimmed": int(globals().get("TAIL_TRIMMED_COUNT", 0)),
            "tail_guard_blocked": int(globals().get("TAIL_GUARD_BLOCKED_COUNT", 0)),
        },
        "records": [asdict(r) for r in records],
    }

    logger.info(
        "Quality metrics: name_problems=%d | wrong_year=%d | wrong_type=%d | tail_trimmed=%d | tail_guard_blocked=%d",
        name_problem_count,
        wrong_year_count,
        wrong_type_count,
        int(globals().get("TAIL_TRIMMED_COUNT", 0)),
        int(globals().get("TAIL_GUARD_BLOCKED_COUNT", 0)),
    )

    # Dónde guardar
    if args.test_mode:
        movies_root = Path(cfg_get(cfg, "plex.movies_root", "test_output/Peliculas"))
        out_path = movies_root.parent / "_manifest.json"
    else:
        out_path = Path("_manifest.json")

    try:
        out_path.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        logger.exception("Failed to prepare output directory for manifest: %s", out_path.parent)
        return

    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(
        "Resumen: total=%d | movies=%d | tv=%d | unclassified=%d | otros=%s",
        payload["total"],
        summary.get("movie", 0) + summary.get("movie_kids", 0),
        summary.get("tv", 0) + summary.get("tv_kids", 0),
        summary.get("unclassified", 0),
        {k: v for k, v in summary.items()
         if k not in {"movie", "movie_kids", "tv", "tv_kids", "unclassified"}}
    )
    
    _save_known_pick_cache()

# ------------------------------- CLI / config --------------------------------

def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for operational modes.

    Returns
    -------
    argparse.Namespace
        Parsed flags and parameters.
    """
    p = argparse.ArgumentParser(description="EmuleToPlex runner")
    p.add_argument(
        "--test-mode",
        action="store_true",
        help="Enable test mode: process .txt placeholders, disable Plex refresh, redirect roots.",
    )
    p.add_argument(
        "--test-source",
        type=str,
        help="Folder containing .txt placeholders to classify.",
    )
    p.add_argument(
        "--test-output-root",
        type=str,
        help="Where to build simulated Movies/TV structure in test mode (default: ./test_output).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't move or create files; just log actions.",
    )
    p.add_argument(
        "--once",
        action="store_true",
        help="Process current files once and exit (no watchdog loop).",
    )
    p.add_argument(
        "--phase1-only",
        action="store_true",
        help="Phase-1: skip second-pass rescues for deterministic baseline."
    )
    p.add_argument(
        "--no-known-picks",
        action="store_true",
        help="Disable known_picks.json cache when metadata is weak."
    )

    return p.parse_args()


def load_config() -> Dict[str, Any]:
    """
    Load YAML configuration from config.yaml or config.example.yaml.

    Returns
    -------
    dict
        Configuration dictionary, possibly empty if no file is found.
    """
    cfg_path = Path("config.yaml")
    if not cfg_path.exists():
        cfg_path = Path("config.example.yaml")
    if cfg_path.exists():
        with cfg_path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    return {}


def cfg_get(cfg: Dict[str, Any], dotted_path: str, default: Any = None) -> Any:
    """
    Retrieve nested configuration values with dotted paths.

    Parameters
    ----------
    cfg : dict
        Configuration dictionary.
    dotted_path : str
        Dotted path, e.g., "metadata.tmdb_api_key".
    default : Any
        Default value if the path is not present.

    Returns
    -------
    Any
        Resolved configuration value or default.
    """
    cur: Any = cfg
    for key in dotted_path.split("."):
        cur = cur.get(key) if isinstance(cur, dict) else None
        if cur is None:
            return default
    return cur


def apply_test_overrides(cfg: Dict[str, Any], args: argparse.Namespace) -> Dict[str, Any]:
    """
    Apply test-mode overrides to the configuration:
    - Disable Plex token/base_url
    - Redirect roots to --test-output-root (Peliculas/Series + *_Infantiles)
    - Accept .txt as an allowed extension
    - Reduce stability wait for faster tests
    """
    if not args.test_mode:
        return cfg

    cfg = dict(cfg or {})

    # Plex disabled in test
    plex = dict(cfg.get("plex") or {})
    plex["token"] = None
    plex["base_url"] = None

    root = Path(args.test_output_root or "test_output").resolve()
    plex["movies_root"] = str(root / "Peliculas")
    plex["shows_root"] = str(root / "Series")
    plex["movies_kids_root"] = str(root / "Peliculas_Infantiles")
    plex["shows_kids_root"] = str(root / "Series_Infantiles")
    cfg["plex"] = plex

    # Accept .txt in test mode, speed up stability
    emule = dict(cfg.get("emule") or {})
    emule["allowed_extensions"] = [".txt"]
    emule["sidecar_extensions"] = []  # nada de .srt, etc., en pruebas
    emule["stable_seconds"] = 0       # sin espera en pruebas
    cfg["emule"] = emule

    return cfg

# --------------------------------- Logging -----------------------------------

def setup_logger(cfg: Dict[str, Any]) -> logging.Logger:
    """
    Set up a RotatingFileHandler logger plus console output.

    Parameters
    ----------
    cfg : dict
        Configuration to read log level and file path.

    Returns
    -------
    logging.Logger
        Configured logger instance named "emuletoplex".
    """
    logger = logging.getLogger("EmuleToPlex")
    
    # Prevent duplicate logs on re-setup
    if logger.handlers:
        for h in list(logger.handlers):
            logger.removeHandler(h)
    logger.propagate = False
    
    logger.setLevel(getattr(logging, cfg_get(cfg, "logging.level", "INFO").upper()))
    log_file = cfg_get(cfg, "logging.log_file", "emuletoplex.log")
    max_bytes = int(cfg_get(cfg, "logging.max_bytes", 5 * 1024 * 1024))
    backup_count = int(cfg_get(cfg, "logging.backup_count", 5))

    os.makedirs(str(Path(log_file).parent), exist_ok=True)
    handler = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8")
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    handler.setFormatter(fmt)
    logger.addHandler(handler)

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    logger.addHandler(console)
    return logger

# --------------------------- Preprocessing / Helpers ------------------------

def looks_like_leading_number_title(s: str) -> bool:
    """
    True si el nombre parece un título normal que empieza por número:
    ej. '12 monos', '21 gramos', '3 metros sobre el cielo'.
    No confundir con marcadores de episodio.
    """
    s = s.strip()
    # número + palabra con letras, sin marcador de episodio
    if re.match(r"^\d+\s+[A-Za-zÁÉÍÓÚÜÑáéíóúüñ][^\d]+$", s):
        # si contiene patrones claros de episodio, no aplica
        if re.search(r"(?i)\bS\d{1,2}E\d{1,2}\b|\b\d+x\d{2}\b|\bTemporada\b", s):
            return False
        return True
    return False

def pick_title_from_hyphens(stem: str) -> Optional[str]:

    """
    Extrae la parte más probable de título cuando hay guiones con año, p. ej.:
    '1999 - Título' -> 'Título' o 'Título - 2007' -> 'Título'.
    Devuelve None si no huele a título (pocas letras, etc.).
    """

    # 1999 - Título
    m = re.match(r'^\s*((?:19|20)\d{2})\s*-\s*(.+)$', stem)
    if m:
        cand = m.group(2).strip()
        if len(re.findall(r'[A-Za-z]{3,}', cand)) >= 2:
            return cand
    # Título - 2007
    m = re.match(r'^(.+?)\s*-\s*((?:19|20)\d{2})\b', stem)
    if m:
        cand = m.group(1).strip()
        if len(re.findall(r'[A-Za-z]{3,}', cand)) >= 2:
            return cand
    return None

def _expand_cap_to_sxxexx(s: str) -> str:
    """
    Convierte 'Cap.102' o 'Capitulo 1105' en 'S01E02' o 'S11E05'.
    """
    def _repl(m: re.Match) -> str:
        n = int(m.group(1))
        if n >= 100:
            season, episode = divmod(n, 100)
        else:
            season, episode = 1, n
        return f"S{season:02d}E{episode:02d}"
    return CAP_COMPRESSED_RE.sub(_repl, s)


def _prefer_title_segment(s: str) -> str:
    """
    Divide por ' - ' y elige el segmento que parece título de obra,
    no etiquetas de release ni nombres de persona.
    """
    parts = [p.strip() for p in re.split(r"\s*-\s*", s) if p.strip() and re.search(r"[A-Za-z]{3,}", p)]
    if len(parts) <= 1:
        return s

    def _score(part: str) -> int:
        if START_JUNK_RE.search(part) or RELEASE_TAGS_RE.search(part) or DOMAINS_ANY_RE.search(part):
            return -10
        sc = 0
        if LANG_TAGS_RE.search(part):
            sc -= 6
        if 'UPLOADER_LIST_RE' in globals() and UPLOADER_LIST_RE.search(part):
            sc -= 6
        has_year = bool(re.search(r"\b(19|20)\d{2}\b", part))
        # Prioriza partes con letras y sin ser puro año
        word_count = len(re.findall(r"[A-Za-z]{3,}", part))
        sc += min(word_count, 6)
        if has_year:
            sc += 6
        # Evita quedarse con solo el año
        if word_count == 0 and has_year:
            sc -= 8
        # Penaliza "Nombre Apellido" persona
        if _is_person_like_title(part):
            sc -= 5
        return sc

    best = max(parts, key=_score)
    return best


_EP_SXXEYY = re.compile(r"(?i)\bS(\d{1,2})E(\d{2})\b")
_EP_1XNN   = re.compile(r"(?i)\b(\d{1,2})x(\d{2})\b")
_EP_CAP    = re.compile(r"(?i)\bCap(?:\.|itulo|ítulo)?\s*(\d{2,4})\b")

def parse_episode_markers(s: str) -> Tuple[Optional[int], Optional[int]]:

    """
    Detecta season/episode en las formas SxxEyy, 1x02 o Cap.102.
    Devuelve (season, episode) o (None, None) si no hay marcadores.
    """

    m = _EP_SXXEYY.search(s)
    if m:
        return int(m.group(1)), int(m.group(2))
    m = _EP_1XNN.search(s)
    if m:
        return int(m.group(1)), int(m.group(2))
    m = _EP_CAP.search(s)
    if m:
        num = int(m.group(1))
        # Heurística: Cap.102 => S01E02, Cap.1203 => S12E03, si son 2 dígitos => S01E##.
        if num >= 100:
            s, e = divmod(num, 100)
            s = max(1, s)
            e = max(1, e)
            return s, e
        else:
            return 1, max(1, num)
    return None, None

# ------------------------------ Preprocessing ---------------------------------

def split_compact_tokens(s: str) -> str:
   
    """Split merged quality/codec/release/language markers BEFORE heavier cleanup."""

    out = s
    for pattern, repl in COMPACT_TOKEN_REPLACEMENTS:
        out = pattern.sub(repl, out)
    return re.sub(r"\s{2,}", " ", out)

def normalize_quality_tokens(s: str) -> str:
    """
    Normalize common quality flags into stable space-separated tokens:
    - UHD, UltraHD -> 4k
    - HDR10+ -> hdr10plus
    - Dolby Vision variants -> dovi
    - DV (when clearly HDR context) -> dovi
    Does not touch TV markers like S01E02 or 1x20.
    """
    # Normalize case and separators around well-known flags
    s = re.sub(r"(?i)\bUHD(?:\s*BD)?\b", " 4k ", s)
    s = re.sub(r"(?i)\bUltra\s*HD\b", " 4k ", s)
    s = re.sub(r"(?i)\bHDR10\+\b", " hdr10plus ", s)
    s = re.sub(r"(?i)\bHDR10\b", " hdr10 ", s)
    s = re.sub(r"(?i)\bDolby\s*Vision\b", " dovi ", s)
    # Map bare 'DV' to dovi only when it appears near HDR/4k family
    s = re.sub(r"(?i)\b(?:(?<=hdr)\s*dv|dv\s*(?=hdr)|(?<=4k)\s*dv|dv\s*(?=4k))\b", " dovi ", s)
    # Collapse whitespace once; later passes will trim further
    return re.sub(r"\s+", " ", s)

def drop_credit_or_person_head(s: str) -> str:
    """
    Remove leading credit/uploader-style heads like "de/by <Person> con/with <Person>"
    and bare person-list heads. Keeps years/episode markers when they are part of the
    title body.

    Parameters
    ----------
    s : str
        Raw or partially-normalized title string.

    Returns
    -------
    str
        String without a leading credit/uploader/person head.
    """
    base = s

    # If a known "credits head" pattern exists, cut from its end forward
    if 'CREDITS_HEAD_RE' in globals() and CREDITS_HEAD_RE is not None:
        m = CREDITS_HEAD_RE.match(base)
        if m:
            base = base[m.end():].lstrip()
            global HEAD_CREDIT_DROP_COUNT
            HEAD_CREDIT_DROP_COUNT += 1

    # If the string starts with a person-like list (credits roll)
    if 'PERSON_LIST_RE' in globals() and PERSON_LIST_RE is not None:
        m_list = PERSON_LIST_RE.match(base)
        if m_list:
            base = base[m_list.end():].lstrip(" -:;|")
            global HEAD_PERSON_DROP_COUNT
            HEAD_PERSON_DROP_COUNT += 1

    # Remove short ALL-CAPS tails right after the head removal to avoid poisoning title
    head, sep, tail = base.rpartition(" ")
    if tail and ALLCAPS_TAIL_RE.search(base):
        # Only drop if there's meaningful content before the tail
        if re.search(r"[A-Za-z]{3,}", head or ""):
            base = (head or "").strip()

    return re.sub(r"\s+", " ", base).strip(" -.,")

def _allow_leading_numeral(seg: str) -> bool:
    """
    Accept segments that begin with a numeral followed by an alphabetic token.
    Blocks false positives such as resolutions (720p/1080p/2160p/4320).
    """
    if not seg:
        return False
    m = re.match(r"^\s*(\d{1,4})\s+([A-Za-z])", seg)
    if not m:
        return False
    num = int(m.group(1))
    if num in (360, 480, 720, 1080, 2160, 4320):
        return False
    return True

def prune_parentheses_and_delimiters(s: str) -> str:
    """
    Milestone-C pruning over parentheses and clause delimiters.

    Rules (acceptance v0.23 Phase-1 C):
      - Keep (YYYY) where YYYY is 19xx/20xx.
      - Drop parentheticals that are numeric-only or that become junk after removing
        release/lang/uploader/domains. Keep only if there are real words.
        Bilingual tolerance: allow inner parens with ≥ 2 alphabetic words.
      - Remove domains inside parentheses.
      - Normalize dotted years like 2.020 → 2020. Drop thousand-like numbers (1.234) not years.
      - When splitting by [-–;:/], keep only clauses that contain ≥ 1 [A-Za-z]{3,}.
      - Commas are NOT applied here; they are used only to generate alternative queries.
    """
    if not s:
        return s

    # Remove (domain.tld) early to avoid keeping it later by mistake
    s = DOMAINS_PARENS_RE.sub(" ", s)

    # Normalize dotted years and drop thousand-like numbers that aren't years
    s = re.sub(r"\b(19|20)\.(\d{3})\b", r"\1\2", s)
    s = re.sub(r"\b\d{1,3}\.\d{3}\b", " ", s)

    def _paren_repl(m: re.Match) -> str:
        inner = (m.group(1) or "").strip()

        # Keep pure year (19xx/20xx)
        if re.fullmatch(r"(?:19|20)\d{2}", inner):
            return f" ({inner})"

        # No letters at all → drop
        if not re.search(r"[A-Za-z]", inner):
            return " "

        # Remove known noise before deciding to keep
        cleaned = RELEASE_TAGS_RE.sub(" ", inner)
        cleaned = LANG_TAGS_RE.sub(" ", cleaned)
        if 'UPLOADER_LIST_RE' in globals():
            cleaned = UPLOADER_LIST_RE.sub(" ", cleaned)
        cleaned = DOMAINS_ANY_RE.sub(" ", cleaned)
        cleaned = WORD_SEPS_RE.sub(" ", cleaned)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()

        # Keep only meaningful/bilingual parentheses (≥2 alphabetic words)
        return f" ({cleaned})" if _two_alpha_words(cleaned) else " "

    # Apply parentheses cleanup
    s = re.sub(r"\(([^)]*)\)", _paren_repl, s)
    s = re.sub(r"\(\s*\)", " ", s)

    # Clause segmentation by common delimiters and keep only meaningful chunks
    clauses = [seg.strip() for seg in re.split(r"\s*[-–;:/]\s*", s)]
    
    kept = [
        seg for seg in clauses
        if seg and (_has_three_alpha(seg) or _allow_leading_numeral(seg))
    ]
    
    if kept:
        s = " - ".join(kept)
    else:
        # Conservative fallback: pick a clause that carries a preserved (YYYY)
        yearish = next(
            (seg for seg in clauses if re.search(r"\((?:19|20)\d{2}\)", seg) and _has_three_alpha(seg)),
            None
        )
        s = yearish or " ".join(seg for seg in clauses if seg).strip()

    # Normalize whitespace and shave stray punctuation
    return re.sub(r"\s+", " ", s).strip(" -.,")


def preprocess_name(raw: str) -> str:
    """
    Pre-clean a noisy release name to a stable, parse-friendly form.

    Order (Milestones 0/A/B/C):
      1) Split compact tokens (e.g., "WEBRip1080p" → "WEBRip 1080p").
      2) Trim uploader/group tail with boundary guards (preserve SxxEyy, 1xNN, E##,
         Cap.###, Temporada/Season, and years).
      3) Normalize quality/codec/resolution tokens (uhd/hdr/dovi family).
      4) Drop leading credit/uploader clauses at the head.
      5) Expand "Cap.###" into SxxEyy before touching []/() content.
      6) Preserve episode/season markers inside [] and drop the rest.
      7) Normalize dotted years and remove thousand-like numbers (not years).
      8) Parenthesis + delimiter pruning (Milestone-C helper).
      9) Final sweep and re-inject (YYYY) if it only existed in brackets.
    """
    if not raw:
        return ""

    s = str(raw)

    # 1) Surface merged markers early
    s = split_compact_tokens(s)

    # 2) Trim uploader/group tail, protected by episode/year guards (Milestone B)
    s = strip_uploader_tail(s)

    # 3) Normalize quality tokens (UHD/HDR/DoVi, codecs, resolutions)
    s = normalize_quality_tokens(s)

    # 4) Remove credit-like heads: "de/di/by/with/con ..." etc.
    s = drop_credit_or_person_head(s)

    # 5) Expand Cap.### into SxxEyy so later steps see it
    s = _expand_cap_to_sxxexx(s)

    # Capture existing years to re-inject later if needed
    paren_year_m = re.search(r"\(((?:19|20)\d{2})\)", s)
    bracket_year_m = re.search(r"\[((?:19|20)\d{2})\]", s)

    # 6) Brackets: keep SxxEyy / Temporada N / Cap.###, drop the rest
    def _br_repl(m: re.Match) -> str:
        inner = m.group(1) or ""
        m_ep = re.search(r"(S\d{1,2}E\d{1,2})", inner, re.IGNORECASE)
        if m_ep:
            return f" {m_ep.group(1)} "
        m_temp = re.search(r"(?i)(Temporada\s+\d{1,2})", inner)
        if m_temp:
            return f" {m_temp.group(1)} "
        
        m_cap = re.search(r"(?i)Cap(?:\.|itulo|ítulo)?\s*(\d{3,4})", inner)
        if m_cap:
            return f" {_expand_cap_to_sxxexx(m_cap.group(0))} "

        return " "

    s = re.sub(r"\[(.*?)\]", _br_repl, s)

    # 7) Normalize dotted years and numbers
    s = DOMAINS_PARENS_RE.sub(" ", s)
    s = re.sub(r"\b(19|20)\.(\d{3})\b", r"\1\2", s)
    s = re.sub(r"\b\d{1,3}\.\d{3}\b", " ", s)

    # Strip release/lang/uploader crumbs before the C helper examines parentheses
    # 7.b) Universal normalization order (domains → junk → uploader list)
    s = DOMAINS_ANY_RE.sub(" ", s)
    s = RELEASE_TAGS_RE.sub(" ", s)
    s = LANG_TAGS_RE.sub(" ", s)
    if 'UPLOADER_LIST_RE' in globals():
        s = UPLOADER_LIST_RE.sub(" ", s)

    # Drop trivial audio channel patterns like 5.1 / 7-1 / 2 0
    s = re.sub(r"(?i)\b(?:[257]\s*[\.\- ]\s*[01])\b", " ", s)

    # 8) Milestone-C parenthesis + delimiter pruning
    s = prune_parentheses_and_delimiters(s)

    # 9) Final sweep
    s = ASPECT_RATIO_RE.sub(" ", s)
    s = DURATION_MIN_RE.sub(" ", s)
    s = NUM_PREFIX_RE.sub("", s)
    s = WORD_SEPS_RE.sub(" ", s)
    s = re.sub(r"\s*[-–]\s*", " - ", s)
    s = re.sub(r"\s+", " ", s, flags=re.UNICODE).strip()
    s = re.sub(r"^\s*-\s*", "", s)

    # Re-inject a single canonical (YYYY) if it was only in brackets
    if bracket_year_m and not paren_year_m:
        y = bracket_year_m.group(1)
        if y and not re.search(rf"\(\s*{re.escape(y)}\s*\)\s*$", s):
            s = f"{s} ({y})".strip()

    # Must not emit segments without any 3+ alpha run
    return s if _has_three_alpha(s) else ""

def sanitize_filename(name: str, strategy: str = "transliterate", keep_chars: str = r"A-Za-z0-9 .,'()!_-") -> str:

    """
    Sanitize a display or folder/file name to make it Plex/Windows-friendly.

    Parameters
    ----------
    name : str
        Input string to sanitize.
    strategy : {"transliterate","drop","keep"}
        - transliterate: convert non-Latin characters to ASCII approximations.
        - drop: remove any non-ASCII characters.
        - keep: keep Unicode (but still filter dangerous characters).
    keep_chars : str
        Regex character class of allowed ASCII characters after transliteration/drop.

    Returns
    -------
    str
        Sanitized string.
    """
    name = unicodedata.normalize("NFKC", name)
    if strategy == "transliterate":
        name = unidecode(name)
    elif strategy == "drop":
        name = name.encode("ascii", "ignore").decode("ascii", "ignore")
    # else "keep": do nothing special

    name = name.replace(":", " - ")

    # collapse whitespace and filter unwanted characters strictly
    name = " ".join(name.split())
    name = re.sub(fr"[^{keep_chars}]", " ", name)
    name = " ".join(name.split())
    return name


def strip_release_tokens_for_display(title: str) -> str:
    """Remove residual release/quality tokens from final display titles."""
    if not title:
        return title

    t = RELEASE_TAGS_RE.sub(" ", title)
    # Audio channel patterns (5.1, 7-1, etc.)
    t = re.sub(r"(?i)\b(?:[257]\s*[.\- ]\s*[01])\b", " ", t)
    # Common resolution/bit-depth leftovers
    t = re.sub(r"(?i)\b\d{3,4}p\b", " ", t)
    t = re.sub(r"(?i)\b(?:10b(?:it)?|8bit|hdr10\+?|hdr|hlg)\b", " ", t)
    # Codec leftovers that sometimes survive best-segment selection
    t = re.sub(r"(?i)\b(?:x26[45]|hevc|av1|h\.?26[45])\b", " ", t)
    # Language tags that sneak into aliases
    t = LANG_TAGS_RE.sub(" ", t)
    t = " ".join(t.split())
    return t

def basename_key_from_src(src: str, cfg: Dict[str, Any]) -> str:
    """
    Build a normalized comparison key for a record's 'src'.

    Rationale:
    - During 2nd pass, files inside 'Unclassified' have a sanitized filename
      (brackets, dots, etc.). The original 'src' kept in the manifest is the
      raw placeholder name from EmuleIncomingTxt.
    - To match both reliably, we normalize BOTH sides with the SAME sanitizer
      used to create Unclassified filenames.

    Returns
    -------
    str
        Lowercased normalized key based on the sanitized STEM of the file name.
    """
    strategy = cfg_get(cfg, "renamer.non_latin_strategy", "transliterate")
    keep_chars = cfg_get(cfg, "renamer.keep_chars", r"A-Za-z0-9 .,'()!_-")
    stem = Path(src).stem
    norm = sanitize_filename(stem, strategy=strategy, keep_chars=keep_chars)
    return norm.casefold()


def basename_key_from_name(name: str, cfg: Dict[str, Any]) -> str:
    """
    Same as basename_key_from_src but takes a plain filename (or Path-like).
    """
    strategy = cfg_get(cfg, "renamer.non_latin_strategy", "transliterate")
    keep_chars = cfg_get(cfg, "renamer.keep_chars", r"A-Za-z0-9 .,'()!_-")
    stem = Path(name).stem
    norm = sanitize_filename(stem, strategy=strategy, keep_chars=keep_chars)
    return norm.casefold()

YEAR_TOKEN_RE = re.compile(r"\b(?:19|20)\d{2}\b")

def extract_filename_years(text: str) -> List[int]:
    """
    Return every 4-digit year token (19xx/20xx) from a raw name.
    This feeds a hard constraint for metadata picking to stop
    drifting to homonyms or newer franchise entries.

    Notes
    -----
    - We intentionally ignore resolution tokens (2160p, 1080p, etc.)
      because YEAR_TOKEN_RE only matches 19xx/20xx.
    - The order is preserved (left to right) to pick a default hint.
    """
    seen: set[int] = set()
    years: List[int] = []
    for m in YEAR_TOKEN_RE.finditer(text.replace(".", " ")):
        y = int(m.group(0))
        if 1900 <= y <= 2099 and y not in seen:
            seen.add(y)
            years.append(y)
    return years

# --------------------------- File state checks --------------------------------

def is_locked(path: Path) -> bool:
    """
    Check if a file appears to be locked by attempting to open for append.

    Parameters
    ----------
    path : pathlib.Path
        File to test.

    Returns
    -------
    bool
        True if locked or unreadable, False otherwise.
    """
    try:
        with open(path, "ab"):
            return False
    except Exception:
        return True


def is_stable(path: Path, stable_seconds: int) -> bool:
    """
    Determine if a file size is stable over a given interval.

    Parameters
    ----------
    path : pathlib.Path
        File to test.
    stable_seconds : int
        Seconds to wait between size checks.

    Returns
    -------
    bool
        True if size unchanged and file still exists after waiting.
    """
    if not path.exists():
        return False
    size1 = path.stat().st_size
    time.sleep(stable_seconds)
    if not path.exists():
        return False
    size2 = path.stat().st_size
    return size1 == size2


def allowed_extension(path: Path, allowed: Optional[Iterable[str]]) -> bool:
    """
    Check if a file has an allowed extension.

    Parameters
    ----------
    path : pathlib.Path
        File to test.
    allowed : Iterable[str] or None
        Allowed extensions (case-insensitive). If None or empty, a default set is used.

    Returns
    -------
    bool
        True if extension is allowed, else False.
    """
    if not allowed:
        allowed = [".mkv", ".mp4", ".avi", ".mov", ".m4v"]
    allowed_set = {e.lower() for e in allowed}
    return path.suffix.lower() in allowed_set

#-------------------------------------------------------------------------------



# --- CLEAN QUERY FOR TMDb ---
YEAR_RE = re.compile(r'\((?:19|20)\d{2}\)')
BAD_TOKENS_RE = re.compile(
    r"(?i)\b("
    r"spanish|castellano|english|vose|sub(?:s|titulos?)?|subs|"
    r"webrip|web-?dl|b[dr]rip|hdrip|microhd|hdr10(?:\+)?|uhd|4k|"
    r"h\.?264|x26[45]|hevc|10b(?:it)?|8bit|dovi|dv|hdr|"
    r"ac-?3|eac-?3|dts(?:-?hd)?|atmos|"
    r"imax|remux|dual|multi|"
    r"\d{3,4}p|720p|1080p|2160p|"
    r"cam|ts|tc|r5|"
    r"bluray|blu-?ray|dvd|dvdrip|bdrip|brrip|webrip|webrdl|"
    r"by|para|subforz(?:adas?)?|forced"
    r"|tt\d{7,8}"
    r")\b"
)

UPLOADER_TAIL_RE = re.compile(
    r"""
    (                               # any of the following, anchored near the end
        \s+(?:by|por|per|para)\s+ [^\[\]()]+ $         # textual "by Remy", "por Grupo"
      | \s* [\-–—] \s* [A-Za-z][\w.\-]{1,15} \s* $     # hyphen handle: "- xusman", "— remy"
      | \s* \[ [A-Z0-9][A-Z0-9._\-]{1,12} \] \s* $     # group tags: "[GRP]", "[UHD.REMUX]"
      | \s* @ [\w.\-]{3,} \s* $                        # @handle
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)

UPLOADER_BOUNDARY_RE = re.compile(
    r"(?:\b(?:19|20)\d{2}\b|"          # year 19xx/20xx
    r"\bS\d{1,2}E\d{1,3}\b|"           # SxxEyyy
    r"\b\d{1,2}x\d{1,3}\b|"            # 1xNNN
    r"\bE\d{1,3}\b|"                    # E###
    r"Cap(?:\.|itulo|ítulo)?\s*\d{1,4}|"# Cap.### / Capitulo ###
    r"\bTemporada\b|\bSeason\b)",       # season words
    re.IGNORECASE,
)


def strip_uploader_tail(s: str) -> str:
    """
    Trim trailing uploader/group tails while preserving year/episode contexts.

    Behavior
    --------
    - Detect tail patterns: "by|por|per|para <handle>", hyphen-handle tails, bracketed group tags
      and @handles near the end of the string.
    - If the detected tail contains boundaries (year, SxxEyy, 1xNN, E##, Cap.###, Temporada/Season),
      do not trim; keep the boundary segment and everything after it.
    - Otherwise trim the whole detected tail.
    - As a final touch, nuke naked domains and lone (http...) parentheses left at the end.
    """
    m = UPLOADER_TAIL_RE.search(s)
    if not m:
        # quick cleanup of any domain/URL crumbs even if no tail was matched
        s = DOMAINS_ANY_RE.sub(" ", s)
        s = re.sub(r"\((?:\s*https?://)?[^\s)]+\)", " ", s)
        return re.sub(r"\s+", " ", s).strip(" -.,_")

    prefix = s[:m.start()]
    tail = s[m.start():]

    # If tail contains year/episode/season context, keep from that boundary on.
    boundary = UPLOADER_BOUNDARY_RE.search(tail)
    if boundary:
        # Guard wins: we DO NOT trim because a boundary token is present
        keep = tail[boundary.start():].lstrip()        
        prefix = prefix.rstrip()
        s = f"{prefix} {keep}".strip() if prefix and keep else (keep or prefix or "")
        # Instrumentation
        global TAIL_GUARD_BLOCKED_COUNT
        TAIL_GUARD_BLOCKED_COUNT += 1
    else:
        # no boundary: drop the uploader/group tail entirely
        s = prefix.rstrip()
        # Instrumentation
        global TAIL_TRIMMED_COUNT
        TAIL_TRIMMED_COUNT += 1

    # Cleanup: domains and URL-only parentheses
    s = DOMAINS_ANY_RE.sub(" ", s)
    s = re.sub(r"\((?:\s*https?://)?[^\s)]+\)", " ", s)

    return re.sub(r"\s+", " ", s).strip(" -.,_")

def _recover_title_after_tail_trim(original: str, stripped: str) -> str:
    """
    If the uploader tail was removed and the remaining title is just one
    very short token, recover the previous head token from the prefix
    before the tail to avoid crippling the query (Milestone-2 bonus).
    This is conservative and only fires for truly tiny titles.

    Parameters
    ----------
    original : str
        Text before strip_uploader_tail.
    stripped : str
        Text after strip_uploader_tail.

    Returns
    -------
    str
        Either 'stripped' unchanged, or with one helpful head token
        prepended (e.g., "Rio" -> "Gran Rio").
    """
    # Only consider if the title shrank and looks too tiny to be useful.
    tiny = stripped.strip()
    if not tiny:
        return stripped
    # One word of 1–3 alphanum chars is considered "too tiny".
    if not re.fullmatch(r"[A-Za-z0-9]{1,3}", tiny):
        return stripped

    m = UPLOADER_TAIL_RE.search(original)
    if not m:
        return stripped

    # If a guardable boundary (SxxEyy, 1xNN, E##, Cap.###, Temporada...) was present in the tail,
    # do NOT attempt recovery; milestone-2 wants the guard to block aggressive trimming.
    tail_piece = original[m.start():]
    if UPLOADER_BOUNDARY_RE.search(tail_piece):
        return stripped

    # Take the last decent head token preceding the tail.
    head = original[:m.start()]
    tokens = re.findall(r"[A-Za-z0-9]{3,}", head)
    if not tokens:
        return stripped

    last = tokens[-1]
    if last.lower() == tiny.lower():
        return stripped

    # Prepend the recovered token; keep spacing tidy.
    return f"{last} {tiny}".strip()

def _split_candidate_segments(text: str) -> list[str]:
    """
    Split a noisy title into candidate segments using commas as a *secondary*
    delimiter, then apply the stronger delimiter set (- – ; : /).

    The goal is to free the core bilingual title from language/uploader tails
    that are commonly comma-bound in filenames, without changing the primary
    pruning rules of Milestone-C.

    Parameters
    ----------
    text : str
        Source string for candidate query segments.

    Returns
    -------
    list[str]
        Cleaned, non-empty segments suitable for _good_query gating.
    """
    # First, gently separate comma-bound clauses
    comma_chunks = [c.strip() for c in re.split(r"\s*,\s*", text) if c.strip()]

    # Then apply the stronger, existing delimiter set
    segs: list[str] = []
    for chunk in comma_chunks:
        segs.extend([s.strip() for s in re.split(r"\s*[-–;:/]\s*", chunk) if s.strip()])

    # Collapse accidental duplicates and keep order
    seen = set()
    out: list[str] = []
    for s in segs:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out

def _best_parenthetical_candidate(s: str) -> Optional[str]:
    """
    Find a safe, content-bearing chunk inside parentheses to use as an
    auxiliary title when the main stem is weak. Reject anything that looks
    like release/lang/uploader/domain junk.

    Parameters
    ----------
    s : str
        Source string (pre-clean, but post split/trim).

    Returns
    -------
    Optional[str]
        A candidate with at least two alphabetic words, or None.
    """
    # Extract raw parenthetical guts, e.g. "(The Movie)" -> "The Movie"
    inner_chunks = re.findall(r"\(([^)]{2,})\)", s)
    for chunk in inner_chunks:
        cand = WORD_SEPS_RE.sub(" ", chunk).strip()
        # Must have at least TWO alphabetic words of length >= 3
        if len(re.findall(r"\b[A-Za-z]{3,}\b", cand)) < 2:
            continue
        # Reject common junk
        if (RELEASE_TAGS_RE.search(cand)
                or LANG_TAGS_RE.search(cand)
                or DOMAINS_ANY_RE.search(cand)
                or ('UPLOADER_LIST_RE' in globals() and UPLOADER_LIST_RE.search(cand))):
            continue
        return re.sub(r"\s{2,}", " ", cand)
    return None

def clean_query_text(q: str) -> str:
    """
    Build a minimal, metadata-friendly query string from a noisy name.

    Guarantees (v0.23 Phase-1):
      - Keeps Milestone-B protections (uploader tail guard, tiny-title recovery).
      - Applies Milestone-C pruning:
          * Drops numeric-only or junk parentheses; keeps (YYYY).
          * Keeps bilingual inner-paren chunks only when they have ≥ 2 alpha words.
          * Splits by - – ; : / and drops segments with no [A-Za-z]{3}.
          * Hyphen rule: prefer the leftmost strong segment; if weak, allow a
            single fallback to the first later segment that carries a preserved
            (YYYY) and still has a 3+ alpha run.
    """
    global WEAK_TITLE_FALLBACK_COUNT
    
    if not q:
        return ""

    # 1) Normalize separators and split compact boundaries early
    before_split = q
    s = WORD_SEPS_RE.sub(" ", q)
    s = split_compact_tokens(s)

    # 2) Tail trim with guards (Milestone B)
    before_tail = s
    s = strip_uploader_tail(s)
    tail_trimmed = len(s) < len(before_tail)

    # 3) Normalize quality/codec flags; do not touch TV markers
    s = normalize_quality_tokens(s)

    # 4) Remove obvious release/lang/uploader crumbs and domains
    # 4) Universal normalization order (domains → junk → uploader list)
    s = DOMAINS_ANY_RE.sub(" ", s)
    s = RELEASE_TAGS_RE.sub(" ", s)
    s = LANG_TAGS_RE.sub(" ", s)
    if 'UPLOADER_LIST_RE' in globals():
        s = UPLOADER_LIST_RE.sub(" ", s)

    # 5) Parenthesis + delimiter pruning (Milestone-C)
    s = prune_parentheses_and_delimiters(s)

    # 6) Keep ONLY the leftmost hyphenated segment if it looks like a real title.
    # If the leftmost is weak, allow a single fallback to the first subsequent
    # segment that carries a preserved (YYYY) and has a 3+ alpha run.
    s_before_hyphen = s
    parts = [p.strip() for p in re.split(r"\s*-\s*", s) if p.strip()]
    if parts:
        if re.search(r"[A-Za-z]{3}", parts[0]):
            s = parts[0]
        else:
            for p in parts[1:]:
                if re.search(r"\((?:19|20)\d{2}\)", p) and re.search(r"[A-Za-z]{3}", p):
                    s = p
                    WEAK_TITLE_FALLBACK_COUNT += 1
                    break
    
    # 6.b) Si la cabeza tras pruning empieza por número y es débil,
    #promueve el primer segmento con (YYYY) que tenga ≥1 token alfabético de 3+.
    head = s.strip()
    if re.match(r"^\d+", head) and not re.search(r"[A-Za-z]{3}", head):
        # Usamos la versión previa al recorte por guion para ver los demás segmentos
        candidates = [p.strip() for p in re.split(r"\s*[-–;:/]\s*", s_before_hyphen) if p.strip()]
        for cand in candidates[1:]:
            if re.search(r"\((?:19|20)\d{2}\)", cand) and re.search(r"[A-Za-z]{3}", cand):
                s = cand
                WEAK_TITLE_FALLBACK_COUNT += 1
                break
    
    # 7) Token tidy and numeric normalization
    s = re.sub(r"\b(19|20)\.(\d{3})\b", r"\1\2", s)
    s = re.sub(r"\b\d{1,3}\.\d{3}\b", " ", s)
    s = re.sub(r"[^\S\r\n]+", " ", s).strip(" -.,_")
    s = re.sub(r"\s{2,}", " ", s)

    # 8) If tail trim nuked almost everything, attempt conservative recovery
    if tail_trimmed:
        s = _recover_title_after_tail_trim(before_tail, s)

    # 9) If still weak (1 short word, no year/TV markers), borrow best inner-paren
    if not re.search(r"(?:S\d{1,2}E\d{1,2}|\b(?:19|20)\d{2}\b|1x\d{1,3}|E\d{1,3})", s):
        tokens = re.findall(r"[A-Za-z]{3,}", s)
        if len(tokens) < 2:
            extra = _best_parenthetical_candidate(before_split)
            if extra:
                s = f"{s} ({extra})".strip()

    # 10) Final squash and rule enforcement
    s = re.sub(r"\s{2,}", " ", s).strip(" -.,_")
    return s if _has_three_alpha(s) else ""



IMDB_RE = re.compile(r'(?i)\btt(\d{7,8})\b')

@lru_cache(maxsize=512)
def _tmdb_get(url: str, **params):
    """
    Cached GET for TMDb endpoints.
    Use as: _tmdb_get(url, api_key=..., language=..., timeout=8, any_other_param=...)
    'timeout' se pasa separado al requests.get y se elimina de params.
    """
    timeout = params.pop("timeout", 8)
    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

# Query text that explicitly suggests a documentary
_DOC_HINTS = re.compile(r"(?i)\b(docu|documental|documentary|biopic)\b")

def tmdb_find_by_imdb(api_key: str, imdb_id: str, timeout: int = 8) -> dict:
    url = f"https://api.themoviedb.org/3/find/{imdb_id}"
    j = _tmdb_get(url, api_key=api_key, external_source="imdb_id", timeout=timeout)
    for bucket in ("movie_results", "tv_results"):
        if j.get(bucket):
            x = j[bucket][0]
            x["media_type"] = "movie" if bucket == "movie_results" else "tv"
            return x
    return {}

def tmdb_search_with_fallback(api_key: str, q: str, year_hint: Optional[int],
                              include_adult: bool, timeout: int,
                              media: Optional[str],
                              primary_lang: str = "es-ES") -> List[dict]:
    # Orden: es+year -> es -> en+year -> en
    langs = [primary_lang, "en-US"] if primary_lang.lower() != "en-us" else ["en-US"]
    tries = []
    for lang in langs:
        if year_hint:
            tries.append((lang, year_hint))
        tries.append((lang, None))

    all_results: List[dict] = []
    for lang, y in tries:
        res = tmdb_multi_search(api_key, q, language=lang, timeout=timeout,
                                include_adult=include_adult, media=media, year=y)
        if res:
            return res
        # si vacío, acumulamos para logging si quieres
    return all_results  # vacío si ninguna combinación dio

def _tmdb_pick_alt_title(api_key: str, media_type: str, tmdb_id: int, prefer: List[str], timeout: int = 8) -> Optional[str]:
    if media_type not in {"movie", "tv"}:
        return None
    url = f"https://api.themoviedb.org/3/{'movie' if media_type=='movie' else 'tv'}/{tmdb_id}/alternative_titles"
    try:
        j = _tmdb_get(url, api_key=api_key, timeout=timeout)
    except Exception:
        logger.exception("TMDb alternative titles failed")
        return None

    # Estructuras diferentes: movie -> titles[], tv -> results[]
    alts = []
    if media_type == "movie":
        alts = j.get("titles") or []
        # campos: iso_3166_1, title
        for cc in prefer:
            for a in alts:
                if a.get("iso_3166_1") == cc and a.get("title"):
                    return a.get("title")
    else:
        alts = j.get("results") or []
        # campos: iso_3166_1, title
        for cc in prefer:
            for a in alts:
                if a.get("iso_3166_1") == cc and a.get("title"):
                    return a.get("title")
    return None

# ------------------------------- Metadata API ---------------------------------

def tmdb_multi_search(api_key: str, query: str, language: str = "es-ES",
                      include_adult: bool = False, timeout: int = 8,
                      media: Optional[str] = None, year: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Query TMDb with preference for a specific media type and year when available.
    media: "movie" | "tv" | None. If None, uses multi-search.
    year: for movie -> 'year', for tv -> 'first_air_date_year'
    """
    if media == "movie":
        url = "https://api.themoviedb.org/3/search/movie"
        params = {
            "api_key": api_key,
            "query": query,
            "language": language,
            "include_adult": "true" if include_adult else "false",
        }
        if year:
            params["year"] = int(year)
    elif media == "tv":
        url = "https://api.themoviedb.org/3/search/tv"
        params = {
            "api_key": api_key,
            "query": query,
            "language": language,
            "include_adult": "true" if include_adult else "false",
        }
        if year:
            params["first_air_date_year"] = int(year)
    else:
        url = "https://api.themoviedb.org/3/search/multi"
        params = {
            "api_key": api_key,
            "query": query,
            "language": language,
            "include_adult": "true" if include_adult else "false",
        }

    logger.info("TMDb search: media=%s query=%r lang=%s include_adult=%s year=%s",
                media or "multi", query, language, "true" if include_adult else "false", year)

    try:
        data = _tmdb_get(url, timeout=timeout, **params).get("results", []) or []
        logger.info("TMDb search: results=%d", len(data))
    except Exception:
        logger.exception("TMDb search failed")
        data = []
    return data

def _clean_for_score(s: str) -> str:
    s = re.sub(r"\b\d{4}\b", " ", s)
    s = re.sub(r"[^\w\s]", " ", s)
    return " ".join(s.split()).lower()

#Guardas anti-personas y anti-docu en el picker

_PERSON_LIKE = re.compile(r"^[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2}$")

def _is_person_like_title(t: str) -> bool:
    t = t.strip()
    return bool(_PERSON_LIKE.match(t)) and len(t.split()) <= 3

#-------------------------------------------------

def pick_best_metadata(results: List[Dict[str, Any]], cleaned_query: str,
                       year_hint: Optional[int] = None, tv_hint: bool = False,
                       allowed_years: Optional[set[int]] = None
                       ) -> Tuple[Optional[Dict[str, Any]], int]:

    """
    Elige mejor candidato usando fuzzy + bonus por cercanía de año y penalización de TV sin marcas.
    Aplica guardas: dígitos “clave” de 2-3 cifras (ej. 365) deben aparecer en el título del candidato.
    Devuelve (item, score_ajustado 0..100+).
    """
    cq = _clean_for_score(cleaned_query)
    # dígitos de 2-3 cifras que no son años 19xx/20xx
    digits_req = set(re.findall(r"\b(?!(?:19|20))\d{2,3}\b", cleaned_query))

    best, best_score = None, -1
    for item in results:
        raw_titles = " ".join(filter(None, [
            item.get("title"), item.get("name"),
            item.get("original_title"), item.get("original_name")
        ])) or ""

        # Veto de 'person-like' salvo que el query lo indique explícitamente
        if any(_is_person_like_title(x) for x in [
            item.get("title") or "", item.get("name") or "",
            item.get("original_title") or "", item.get("original_name") or ""
        ]):
            if not _DOC_HINTS.search(cleaned_query):
                continue

        # Guardia de dígitos clave (p.ej. 365)
        if digits_req:
            titles_pack = " ".join(filter(None, [
                item.get("title"), item.get("name"),
                item.get("original_title"), item.get("original_name")
            ])).lower()
            if not any(d.lower() in titles_pack for d in digits_req):
                # si no está en títulos, intenta overview
                overview = (item.get("overview") or "").lower()
                if not any(d.lower() in overview for d in digits_req):
                    continue

        t1 = _clean_for_score(item.get("title") or item.get("name") or "")
        t2 = _clean_for_score(item.get("original_title") or item.get("original_name") or "")
        combo = (t1 + " " + t2).strip()
        base = max(
            fuzz.token_set_ratio(cq, t1),
            fuzz.token_set_ratio(cq, t2),
            fuzz.token_set_ratio(cq, combo)
        )

        raw_titles_lower = " ".join(filter(None, [
            item.get("title"), item.get("name"),
            item.get("original_title"), item.get("original_name")
        ])).lower()
        if RELEASE_TAGS_RE.search(raw_titles_lower) or LANG_TAGS_RE.search(raw_titles_lower) or ('UPLOADER_LIST_RE' in globals() and UPLOADER_LIST_RE.search(raw_titles_lower)):
            base -= 15

        # Año candidato
        ystr = (item.get("release_date") or item.get("first_air_date") or "")[:4]
        cand_year = int(ystr) if ystr.isdigit() else None            
        
        # HARD filter: if filename provided allowed_years, reject mismatching years
        if allowed_years and cand_year and cand_year not in allowed_years:
            continue
        if year_hint and cand_year and abs(cand_year - year_hint) > 1 and base < 90:
            # Year drift beyond 1 year is suspicious unless the textual match is very strong
            continue
        
        cand_type = (item.get("media_type") or "").lower()

        score = base
        if year_hint and cand_year:
            dy = abs(cand_year - year_hint)
            if dy == 0:   score += 20
            elif dy == 1: score += 12
            elif dy <= 2: score += 6
            else:         score -= min(10, 2 * dy)

            # Candado fuerte: si hay year_hint y el candidato se va >1 año, palo serio
            if year_hint and cand_year and abs(cand_year - year_hint) > 1:
                score -= 30

        # Penalize documentaries unless the query actually hints docu/biopic
        gids = item.get("genre_ids") or []
        is_doc = False
        if isinstance(gids, list) and any(isinstance(x, int) for x in gids):
            # TMDb Documentary genre id
            is_doc = 99 in gids

        # Multi-search sometimes returns 'person' entries; treat as docu-like noise
        if (item.get("media_type") or "").lower() == "person":
            is_doc = True

        if is_doc and not _DOC_HINTS.search(cleaned_query):
            score -= 15
        
        if cand_type == "tv" and not tv_hint:
            score -= 15

        if score > best_score:
            best_score, best = score, item

    return best, int(best_score)

def normalize_with_metadata(cfg: Dict[str, Any], cleaned_title: str,
                            year_hint: Optional[int] = None, tv_hint: bool = False,
                            allowed_years: Optional[set[int]] = None
                            ) -> Tuple[Tuple[Optional[str], Optional[str], Optional[int], int, Optional[int], Optional[int], Optional[List[str]]]:]:
    """
    Normaliza (type, title, year) usando TMDb y devuelve:
    (type, title, year, score, tmdb_id, age, genres)
    """
    if not cfg_get(cfg, "metadata.enabled", True):
        return None, None, None, 0, None, None, None

    provider = cfg_get(cfg, "metadata.provider", "tmdb")
    lang = cfg_get(cfg, "metadata.language", "es-ES")
    timeout = int(cfg_get(cfg, "metadata.timeout_seconds", 8))
    base_threshold = int(cfg_get(cfg, "metadata.fuzzy_threshold", 80))
    if year_hint and not tv_hint:
        # si traes año confiable y no apesta a TV, baja un poco el listón
        threshold = max(72, base_threshold - 6)
    else:
        threshold = base_threshold

    try:
        if provider != "tmdb":
            return None, None, None, 0, None, None, None

        api_key = cfg_get(cfg, "metadata.tmdb_api_key")
        if not api_key:
            logger.warning("TMDb disabled: missing metadata.tmdb_api_key in config")
            return None, None, None, 0, None, None, None

        #------------------------ IMDb short-circuit -----------------------
        m_imdb = IMDB_RE.search(cleaned_title)
        if m_imdb:
            imdb_id = f"tt{m_imdb.group(1)}"
            try:
                best = tmdb_find_by_imdb(api_key, imdb_id, timeout=timeout)
                if best:
                    mtype = (best.get("media_type") or "").lower()
                    tmdb_id = best.get("id")

                    # --- MOVIE ---
                    if mtype == "movie":
                        t = best.get("title") or best.get("original_title") or cleaned_title
                        date = best.get("release_date") or ""
                        year = int(date[:4]) if len(date) >= 4 and date[:4].isdigit() else None
                        # IMDb: si no hay año en detalle, completa con year_hint
                        if year is None and year_hint:
                            year = year_hint

                        age = _tmdb_get_movie_cert_age(api_key, int(tmdb_id), timeout=timeout) if tmdb_id else None
                        genres: List[str] = []
                        try:
                            det = _tmdb_get(
                                f"https://api.themoviedb.org/3/movie/{int(tmdb_id)}",
                                api_key=api_key, language=lang, timeout=timeout
                            )
                            genres = [g.get("name") for g in (det.get("genres") or []) if g.get("name")]
                        except Exception:
                            pass

                        # Localización de título legible
                        if t and is_mostly_non_latin(t):
                            alt = _tmdb_title_in_lang(api_key, "movie", int(tmdb_id), "en-US", timeout=timeout)
                            if alt:
                                t = alt
                        # Preferir alternativos oficiales ES/US si existen
                        prefer_cc = ["ES", "US"]
                        alt_loc = _tmdb_pick_alt_title(api_key, "movie", int(tmdb_id), prefer_cc, timeout=timeout)
                        if alt_loc:
                            t = alt_loc
                        # Post-procesado final
                        override = KNOWN_TITLE_OVERRIDES.get(("movie", int(tmdb_id))) if tmdb_id else None
                        if override:
                            t = override
                        t = dedupe_title_phrases(t)

                        return "movie", t, year, 100, (int(tmdb_id) if tmdb_id else None), age, genres

                    # --- TV ---
                    if mtype == "tv":
                        t = best.get("name") or best.get("original_name") or cleaned_title
                        date = best.get("first_air_date") or ""
                        year = int(date[:4]) if len(date) >= 4 and date[:4].isdigit() else None
                        # IMDb: si no hay año en detalle, completa con year_hint
                        if year is None and year_hint:
                            year = year_hint

                        age = _tmdb_get_tv_cert_age(api_key, int(tmdb_id), timeout=timeout) if tmdb_id else None
                        genres: List[str] = []
                        try:
                            det = _tmdb_get(
                                f"https://api.themoviedb.org/3/tv/{int(tmdb_id)}",
                                api_key=api_key, language=lang, timeout=timeout
                            )
                            genres = [g.get("name") for g in (det.get("genres") or []) if g.get("name")]
                        except Exception:
                            pass

                        # Localización de título legible
                        if t and is_mostly_non_latin(t):
                            alt = _tmdb_title_in_lang(api_key, "tv", int(tmdb_id), "en-US", timeout=timeout)
                            if alt:
                                t = alt
                        prefer_cc = ["ES", "US"]
                        alt_loc = _tmdb_pick_alt_title(api_key, "tv", int(tmdb_id), prefer_cc, timeout=timeout)
                        if alt_loc:
                            t = alt_loc
                        # Post-procesado final
                        override = KNOWN_TITLE_OVERRIDES.get(("tv", int(tmdb_id))) if tmdb_id else None
                        if override:
                            t = override
                        t = dedupe_title_phrases(t)

                        return "tv", t, year, 100, (int(tmdb_id) if tmdb_id else None), age, genres

            except Exception:
                logger.exception("IMDb lookup failed for %s", imdb_id)

        # Soporte de ALT ASCII vía "||ALT||"
        primary = cleaned_title
        alt_ascii = None
        if "||ALT||" in cleaned_title:
            primary, alt_ascii = [p.strip() for p in cleaned_title.split("||ALT||", 1)]

        base_for_alts = (primary or cleaned_title).replace("||ALT||", " ")

        # 1) Si parece TV, usa el encabezado antes de SxxEyy/1x03/Temporada como primer intento
        tv_title_head = None
        m_tv_head = re.split(r"(?:\bS\d{1,2}E\d{1,2}\b|\b\d+x\d{2}\b|\bTemporada\b.*)", (primary or cleaned_title), maxsplit=1)
        if m_tv_head and m_tv_head[0].strip():
            tv_title_head = m_tv_head[0].strip()

        # 2) Mejor frase latina multi-palabra dentro del título
        latin_multi = None
        m_all = re.findall(r"[A-Za-z][A-Za-z]+(?:\s+[A-Za-z][A-Za-z]+)+", base_for_alts)
        if m_all:
            latin_multi = max(m_all, key=len).strip()

        # 3) Segmento cercano al año (útil en nombres “... - 2007 - Título ...”)
        segment_near_year = None
        parts = [p.strip() for p in re.split(r"\s*-\s*", base_for_alts) if p.strip()]
        y_idx = None
        if year_hint:
            for i, p in enumerate(parts):
                if re.fullmatch(r"(?:19|20)\d{2}", p):
                    y_idx = i
                    break
        if y_idx is not None and y_idx + 1 < len(parts):
            cand = parts[y_idx + 1]
            if (y_idx + 2 < len(parts)) and re.search(r"[A-Za-z]", parts[y_idx + 2]) and len(parts[y_idx + 2].split()) <= 4:
                cand = f"{cand} - {parts[y_idx + 2]}"
            if not START_JUNK_RE.search(cand) and not RELEASE_TAGS_RE.search(cand) and not DOMAINS_ANY_RE.search(cand):
                segment_near_year = cand.strip()

        # Filtro de queries “útiles”
        def _q_ok(q: Optional[str]) -> bool:
            """
            Is *q* a meaningful title segment for TMDb?
            - Must contain ≥ 1 [A-Za-z]{3,}.
            - Must not be domains or release/lang/uploader noise.
            - Allow single-word queries only if len(word) ≥ 4 or a year_hint exists
            (year_hint is captured from the outer scope).
            """
            if not q:
                return False
            if DOMAINS_ANY_RE.search(q) or RELEASE_TAGS_RE.search(q) or START_JUNK_RE.search(q):
                return False
            if not _has_three_alpha(q):
                return False

            words = q.split()
            if len(words) == 1 and not (len(words[0]) >= 4 or year_hint):
                return False
            return True

        # Candidato por guiones/año
        hyphen_cand = pick_title_from_hyphens(primary)

        # Orden de intentos: si TV -> head primero; si no, segmento_cerca_de_año ayuda
        queries: List[str] = []
        if tv_hint and _q_ok(tv_title_head):
            queries.append(tv_title_head)

        # ❶ "core" sin paréntesis
        core = primary
        # elimina parentesis no anuales
        core = re.sub(r"\((?!19|20)\d{2}[^)]*\)", " ", core)
        # elimina cualquier otro paréntesis
        core = re.sub(r"\([^)]*\)", " ", core)
        core = " ".join(core.split())
        if _q_ok(core):
            queries.append(core)

        # ❷ variante sin "(YYYY)" final si quedó
        yrless = re.sub(r"\s*\((?:19|20)\d{2}\)\s*$", "", core).strip()
        if yrless and yrless != core and _q_ok(yrless):
            queries.append(yrless)

        # ❸ resto igual que antes
        for q in [segment_near_year, latin_multi, alt_ascii, primary]:
            if _q_ok(q):
                queries.append(q)

        # Alias injection removed: rely on generic cleaners and TMDb alternatives

        # Dedupe y limita queries
        cleaned_seen: Set[str] = set()
        limited_queries: List[str] = []
        for q in queries:
            cq = clean_query_text(q)
            key = cq.lower() if cq else ""
            if not key or key in cleaned_seen:
                continue
            cleaned_seen.add(key)
            limited_queries.append(q)
            if len(limited_queries) >= 6:
                break
        queries = limited_queries

        tried: Set[str] = set()
        best, score = None, 0
        base_include_adult = bool(cfg_get(cfg, "metadata.include_adult", False))
        media_order = ["tv"] if tv_hint else ["movie", None, "tv"]  # None => multi

        # ❶.5 hyphen_cand primero si existe
        if _q_ok(hyphen_cand):
            queries.insert(0, hyphen_cand)

        # Bucle principal de queries con dos rondas (include_adult False/True)
        tmdb_call_limit = int(cfg_get(cfg, "metadata.tmdb_call_limit", 40))
        tmdb_calls = 0
        combined: List[dict] = []
        for adult_round in (False, True):
            if adult_round and base_include_adult:
                break  # ya estaban activas desde el principio
            incl_adult = adult_round or base_include_adult

            for q0 in queries:
                if q0 in tried:
                    continue
                tried.add(q0)

                q0_clean = clean_query_text(q0)
                if not q0_clean or len(q0_clean) < 2:
                    continue
                b, sc = None, 0
                combined: List[dict] = []

                for media_pref in media_order:
                    if tmdb_calls >= tmdb_call_limit:
                        break
                    results = tmdb_search_with_fallback(api_key, q0_clean, year_hint,
                                        include_adult=incl_adult, timeout=timeout,
                                        media=media_pref, primary_lang=lang)
                    tmdb_calls += 1
                    combined.extend(results)
                    logger.info("TMDb candidates for %r (%s): %d", q0, media_pref or "multi", len(results))

                    bb, ssc = pick_best_metadata(results, q0, year_hint=year_hint, tv_hint=tv_hint, allowed_years=allowed_years)
                    if ssc > sc:
                        b, sc = bb, ssc
                    if b and sc >= threshold:
                        break

                if tmdb_calls >= tmdb_call_limit:
                    logger.warning("TMDb call limit reached (%d) for %r", tmdb_call_limit, cleaned_title)
                    break

                if sc > score:
                    best, score = b, sc
                if best and score >= threshold:
                    break

            if best and score >= threshold or tmdb_calls >= tmdb_call_limit:
                break

            # “Match único razonable”
            if (not best or score < threshold) and combined:
                cq = _clean_for_score(q0)
                tight = []
                for it in combined:
                    t1 = _clean_for_score(it.get("title") or it.get("name") or "")
                    t2 = _clean_for_score(it.get("original_title") or it.get("original_name") or "")
                    base = max(
                        fuzz.token_set_ratio(cq, t1),
                        fuzz.token_set_ratio(cq, t2),
                        fuzz.token_set_ratio(cq, (t1 + " " + t2).strip())
                    )
                    if base >= max(70, threshold - 10):
                        tight.append((base, it))
                if len(tight) == 1:
                    best, score = tight[0][1], int(tight[0][0])
                    break

        # Si no hay candidato suficientemente bueno, intenta subdividir por separadores y reintentar
        if not best or score < threshold:
            alt_queries: List[str] = []

            # Comma-aware secondary split via the central helper.
            # This keeps delimiters policy in one place (Milestone-C) and stays deterministic.
            segs = _split_candidate_segments(base_for_alts)

            generic_re = re.compile(
                r"^(?:Temporada|Season|Cap(?:\.|itulo|ítulo)?|S\d{1,2}E\d{1,2})\b",
                re.IGNORECASE
            )
            YEAR_PARENS_RE = re.compile(r"\((?:19|20)\d{2}\)")

            def _good_query(q: str) -> bool:
                """Reject noisy or too-generic queries; allow single token only if long or with year hint."""
                if generic_re.search(q):
                    return False
                if DOMAINS_ANY_RE.search(q) or START_JUNK_RE.search(q) or RELEASE_TAGS_RE.search(q):
                    return False
                if not re.search(r"[A-Za-z]{3}", q):
                    return False
                words = q.split()
                if len(words) < 2 and not (len(words) == 1 and (len(words[0]) >= 4 or year_hint)):
                    return False
                return True

            # 1) Segment-derived candidates
            for seg in segs:
                if _good_query(seg):
                    alt_queries.append(seg)

            # 2) Seed with a safe bilingual inner-parenthesis candidate, if any
            paren_cand = _best_parenthetical_candidate(base_for_alts)
            if paren_cand and _good_query(paren_cand) and paren_cand not in alt_queries:
                alt_queries.insert(0, paren_cand)

            # 3) Year-bearing candidates first; Python sort is stable so relative order is preserved
            alt_queries.sort(key=lambda x: 0 if YEAR_PARENS_RE.search(x) else 1)

            # 4) Deduplicate by the *cleaned* form, preserve order, and cap to 6
            alt_clean_seen: Set[str] = set()
            filtered_alt: List[str] = []
            for q in alt_queries:
                cq = clean_query_text(q)
                key = cq.lower() if cq else ""
                if not key or key in alt_clean_seen:
                    continue
                alt_clean_seen.add(key)
                filtered_alt.append(q)
                if len(filtered_alt) >= 6:
                    break
            alt_queries = filtered_alt

            # 5) Try up to 3 alt queries with the same scoring/threshold policy
            for q in alt_queries[:3]:
                if q in tried or tmdb_calls >= tmdb_call_limit:
                    continue
                tried.add(q)

                q_clean = clean_query_text(q)
                if not q_clean or len(q_clean) < 2:
                    continue

                combined2: List[dict] = []
                best2, score2 = None, 0
                for media_pref in media_order:
                    if tmdb_calls >= tmdb_call_limit:
                        break
                    if 'tmdb_search_with_fallback' in globals():
                        results2 = tmdb_search_with_fallback(
                            api_key, q_clean, year_hint,
                            include_adult=incl_adult, timeout=timeout,
                            media=media_pref, primary_lang=lang
                        )
                    else:
                        results2 = tmdb_multi_search(
                            api_key, q_clean, language=lang, timeout=timeout,
                            include_adult=incl_adult, media=media_pref, year=year_hint
                        )
                    tmdb_calls += 1
                    combined2.extend(results2)

                    bb2, ss2 = pick_best_metadata(
                        results2, q, year_hint=year_hint, tv_hint=tv_hint, allowed_years=allowed_years
                    )
                    if ss2 > score2:
                        best2, score2 = bb2, ss2
                    if best2 and score2 >= threshold:
                        break

                if tmdb_calls >= tmdb_call_limit:
                    break

                # Tighten if still under threshold: allow a single fuzzy tie-break rescue
                if (not best2 or score2 < threshold) and combined2:
                    cq2 = _clean_for_score(q)
                    tight2 = []
                    for it in combined2:
                        t1 = _clean_for_score(it.get("title") or it.get("name") or "")
                        t2 = _clean_for_score(it.get("original_title") or it.get("original_name") or "")
                        base = max(
                            fuzz.token_set_ratio(cq2, t1),
                            fuzz.token_set_ratio(cq2, t2),
                            fuzz.token_set_ratio(cq2, (t1 + " " + t2).strip())
                        )
                        if base >= max(70, threshold - 10):
                            tight2.append((base, it))
                    if len(tight2) == 1:
                        best2, score2 = tight2[0][1], int(tight2[0][0])

                # Accept alt only if it clearly improves the current best.
                # Demand at least +5 improvement or crossing threshold with buffer.
                if best2 and score2 >= max(threshold, score + 5):
                    # Extra guard: if this alt flips type (movie/tv) vs a prior candidate,
                    # require a slightly larger margin to avoid fragile type switches.
                    # Acepta alt solo si mejora de forma clara
                    prev_type = None
                    if best:
                        prev_type = (
                            best.get("media_type")
                            or ("movie" if (best.get("title") or best.get("original_title")) else
                                "tv" if (best.get("name") or best.get("original_name")) else None)
                        )
                    next_type = (
                        best2.get("media_type")
                        or ("movie" if (best2.get("title") or best2.get("original_title")) else
                            "tv" if (best2.get("name") or best2.get("original_name")) else None)
                    )

                    if prev_type and next_type and prev_type != next_type and not tv_hint:
                        if score2 >= score + 10:
                            best, score = best2, score2
                            break
                    else:
                        best, score = best2, score2
                        break



        # Sin candidato válido tras todas las tandas
        if not best or score < threshold:
            return None, None, None, int(score or 0), None, None, None

        # Determinar tipo cuando no venga en 'multi'
        mtype = (best.get("media_type") or
                 ("movie" if best.get("title") or best.get("original_title") else
                  "tv" if best.get("name") or best.get("original_name") else None))
        tmdb_id = best.get("id")
        year = None
        age = None
        genres: List[str] = []

        if mtype == "movie":
            t = best.get("title") or best.get("original_title") or cleaned_title
            date = best.get("release_date") or ""
            year = int(date[:4]) if len(date) >= 4 and date[:4].isdigit() else None

            # Adoptar año solo si el match es suficientemente bueno; si no, prioriza year_hint
            if year_hint is not None and year is not None and abs(year - year_hint) > 1 and score < threshold:
                year = year_hint
            elif year is None and year_hint is not None:
                year = year_hint

            if tmdb_id:
                age = _tmdb_get_movie_cert_age(api_key, int(tmdb_id), timeout=timeout)
                try:
                    det = _tmdb_get(
                        f"https://api.themoviedb.org/3/movie/{int(tmdb_id)}",
                        api_key=api_key, language=lang, timeout=timeout
                    )
                    genres = [g.get("name") for g in det.get("genres") or [] if g.get("name")]
                except Exception:
                    genres = []

            # Localización de título legible
            if t and is_mostly_non_latin(t):
                alt = _tmdb_title_in_lang(api_key, "movie", int(tmdb_id), "en-US", timeout=timeout)
                if alt:
                    t = alt
            prefer_cc = ["ES", "US"]
            alt_loc = _tmdb_pick_alt_title(api_key, "movie", int(tmdb_id), prefer_cc, timeout=timeout)
            if alt_loc:
                t = alt_loc
            # Post-procesado final
            override = KNOWN_TITLE_OVERRIDES.get(("movie", int(tmdb_id))) if tmdb_id else None
            if override:
                t = override
            t = dedupe_title_phrases(t)

            logger.info(
                "TMDb pick (movie): id=%s | title='%s' | orig='%s' | year=%s | score=%s | age=%s | genres=%s",
                tmdb_id, t, best.get("original_title"), year, score, age, genres
            )
            return "movie", t, year, int(score), int(tmdb_id) if tmdb_id else None, age, genres

        elif mtype == "tv":
            t = best.get("name") or best.get("original_name") or cleaned_title
            date = best.get("first_air_date") or ""
            year = int(date[:4]) if len(date) >= 4 and date[:4].isdigit() else None

            # Igual que en movie: no te cases con el año de TMDb si el score no llega al corte
            if year_hint is not None and year is not None and abs(year - year_hint) > 1 and score < threshold:
                year = year_hint
            elif year is None and year_hint is not None:
                year = year_hint

            if tmdb_id:
                age = _tmdb_get_tv_cert_age(api_key, int(tmdb_id), timeout=timeout)
                try:
                    det = _tmdb_get(
                        f"https://api.themoviedb.org/3/tv/{int(tmdb_id)}",
                        api_key=api_key, language=lang, timeout=timeout
                    )
                    genres = [g.get("name") for g in det.get("genres") or [] if g.get("name")]
                except Exception:
                    genres = []

            # Localización de título legible
            if t and is_mostly_non_latin(t):
                alt = _tmdb_title_in_lang(api_key, "tv", int(tmdb_id), "en-US", timeout=timeout)
                if alt:
                    t = alt
            prefer_cc = ["ES", "US"]
            alt_loc = _tmdb_pick_alt_title(api_key, "tv", int(tmdb_id), prefer_cc, timeout=timeout)
            if alt_loc:
                t = alt_loc
            # Post-procesado final
            override = KNOWN_TITLE_OVERRIDES.get(("tv", int(tmdb_id))) if tmdb_id else None
            if override:
                t = override
            t = dedupe_title_phrases(t)

            logger.info(
                "TMDb pick (tv): id=%s | title='%s' | orig='%s' | year=%s | score=%s | age=%s | genres=%s",
                tmdb_id, t, best.get("original_name"), year, score, age, genres
            )
            return "tv", t, year, int(score), int(tmdb_id) if tmdb_id else None, age, genres

    except Exception:
        logger.exception("normalize_with_metadata: unexpected error for %r", cleaned_title)

    return None, None, None, 0, None, None, None


# --- Helpers de calidad y desduplicación de títulos ---
WINDOWS_SAFE_MAX_PATH = 240     # margen prudente para NTFS sin prefijos \\?\
MAX_DIR_COMPONENT     = 80      # longitud máx. de la carpeta hoja
MAX_FILE_NAME         = 120     # longitud máx. del fichero (con extensión)

def _truncate_filename(name: str, max_len: int) -> str:
    root, ext = os.path.splitext(name)
    if len(name) <= max_len:
        return name
    keep = max(8, max_len - len(ext))
    return (root[:keep] + ext) if keep > 0 else name[:max_len]

def _truncate_folder(name: str, max_len: int) -> str:
    return name if len(name) <= max_len else name[:max_len]

def _shorten_for_windows(dst: Path) -> Path:
    """
    Sanea y recorta el último directorio y el nombre de archivo para
    mantener la ruta total en un rango seguro en Windows.
    """
    # sanea última carpeta y archivo
    parent = dst.parent.with_name(sanitize_path_component(dst.parent.name))
    fname  = sanitize_path_component(dst.name)

    # recorta tamaños máximos de componente
    parent = parent.with_name(_truncate_folder(parent.name, MAX_DIR_COMPONENT))
    fname  = _truncate_filename(fname, MAX_FILE_NAME)

    candidate = parent / fname

    # si aún es demasiado largo, recorta más el nombre del archivo
    if os.name == "nt":
        total = len(str(candidate))
        if total >= WINDOWS_SAFE_MAX_PATH:
            root, ext = os.path.splitext(fname)
            # recorte adicional según exceso + pequeño margen
            extra = total - WINDOWS_SAFE_MAX_PATH + 5
            keep  = max(8, len(root) - extra)
            if keep > 0:
                fname = root[:keep] + ext
            else:
                # último recurso: hard cut
                fname = (root + ext)[:max(16, WINDOWS_SAFE_MAX_PATH - len(str(parent)) - 1)]
            candidate = parent / fname

    return candidate

def _rmdir_if_empty(p: Path) -> bool:
    """
    Elimina el directorio si está vacío. Devuelve True si lo borra.
    Ignora silenciosamente si no existe o no está vacío.
    """
    try:
        if p.exists() and p.is_dir() and not any(p.iterdir()):
            p.rmdir()
            return True
    except Exception:
        pass
    return False

def cleanup_unclassified_roots(cfg: Dict[str, Any]) -> int:
    """
    Recorrido bottom-up que elimina TODAS las carpetas vacías bajo las raíces Unclassified
    de películas y series. Devuelve cuántas carpetas borró.
    """
    unclass_name = cfg_get(cfg, "folders.unclassified_root", "Unclassified")

    roots: list[Path] = []
    if cfg.get("plex"):
        mroot = cfg_get(cfg, "plex.movies_root", "") or ""
        sroot = cfg_get(cfg, "plex.shows_root", "") or ""
        if mroot:
            roots.append(Path(mroot).resolve() / unclass_name)
        if sroot:
            roots.append(Path(sroot).resolve() / unclass_name)

    # Añade la raíz superior unificada
    try:
        roots.append(compute_unclassified_root(cfg).resolve())
    except Exception:
        pass

    # Dedupe por si coincide con alguna legacy
    roots = list({p.resolve() for p in roots})
    
    removed = 0
    for r in roots:
        if not r.exists():
            continue
        # walk bottom-up
        for dirpath, dirnames, filenames in os.walk(r, topdown=False):
            p = Path(dirpath)
            # no borres la raíz misma si está vacía, es útil que exista
            if p == r:
                continue
            try:
                if not any(p.iterdir()):
                    p.rmdir()
                    removed += 1
            except Exception:
                # sin dramas si Windows se enfada con alguna ruta
                pass
    return removed


def prefer_ascii_parenthetical(s: str) -> str:
    """
    Si hay un título alternativo ASCII entre paréntesis, lo prioriza para búsquedas.
    No altera el display final; solo ayuda a TMDb.
    """
    m = re.search(r"\(([^)]+)\)", s)
    if not m:
        return s
    inner = m.group(1).strip()
    if re.search(r"[A-Za-z]{3}", inner) and all(ord(ch) < 128 for ch in inner):
        # Mantenemos el base, pero sabemos que hay alt ASCII para buscar
        return f"{s}  ||ALT||  {inner}"
    return s


def sanitize_path_component(text: str) -> str:
    # Sustituye caracteres inválidos por espacio
    text = re.sub(f"[{re.escape(INVALID_WIN_CHARS)}]", " ", text)
    # Colapsa puntos repetidos y separadores estúpidos
    text = re.sub(r"[.]{2,}", ".", text)
    text = re.sub(r"\s*[-_]\s*$", "", text)          # quita guiones/underscores al final
    # Normaliza espacios
    text = re.sub(r"\s+", " ", text).strip()
    # Windows no permite terminar en punto o espacio
    text = text.rstrip(" .")
    # Evita nombres reservados
    if text.upper() in RESERVED_WIN_NAMES:
        text = f"_{text}_"
    return text

def compute_unclassified_root(cfg: Dict[str, Any]) -> Path:
    """
    Devuelve la ruta de 'Unclassified' al MISMO NIVEL que Peliculas/Series.

    Lógica:
    - Si hay movies_root y shows_root, intenta usar su padre común.
    - Si solo hay uno, usa su .parent.
    - Si no hay Plex configurado, cae en el cwd.
    """
    name = cfg_get(cfg, "folders.unclassified_root", "Unclassified")
    base: Optional[Path] = None

    if cfg.get("plex"):
        mr = cfg_get(cfg, "plex.movies_root", "") or ""
        sr = cfg_get(cfg, "plex.shows_root", "") or ""
        parents: List[Path] = []
        if mr:
            parents.append(Path(mr).resolve().parent)
        if sr:
            parents.append(Path(sr).resolve().parent)
        if parents:
            if len(parents) == 1:
                base = parents[0]
            else:
                # Intenta padre común; si no hay, coge el del primero
                try:
                    common = os.path.commonpath([str(p) for p in parents])
                    base = Path(common)
                except Exception:
                    base = parents[0]

    if not base:
        base = Path(".").resolve()

    return base / name

def is_mostly_non_latin(s: str, threshold: float = 0.6) -> bool:
    """
    True si la mayoría de letras no son latinas. Sirve para decidir fallback de idioma.
    """
    total = sum(1 for ch in s if ch.isalpha())
    if total == 0:
        return False
    latin = 0
    for ch in s:
        if ch.isalpha():
            try:
                name = unicodedata.name(ch)
            except ValueError:
                name = ""
            if "LATIN" in name:
                latin += 1
    return (total - latin) / total >= threshold

def name_quality(s: str) -> dict:
    """
    Métrica mínima de calidad del nombre final: longitud, nº de letras, no-latino, etc.
    """
    q = {}
    q["len"] = len(s)
    q["has_year_parens"] = bool(re.search(r"\(\d{4}\)$", s))
    q["letters"] = sum(c.isalpha() for c in s)
    q["digits"] = sum(c.isdigit() for c in s)
    q["nonlatin_heavy"] = is_mostly_non_latin(s)
    q["ok"] = (q["len"] >= 4 and q["letters"] >= 3 and not q["nonlatin_heavy"])
    return q

def dedupe_title_phrases(title: str) -> str:
    """
    Elimina repeticiones contiguas de frases/palabras en el título
    (ej: 'Chang Jin Hu Chang Jin Hu The Battle...' -> 'Chang Jin Hu The Battle...')
    """
    words = title.split()
    n = len(words)
    # elimina repeticiones de frases de 4..2 palabras
    for k in range(4, 2 - 1, -1):
        i, out = 0, []
        while i < n:
            if i + 2 * k <= n and words[i:i + k] == words[i + k:i + 2 * k]:
                out.extend(words[i:i + k])
                i += 2 * k
                # colapsa repeticiones múltiples
                while i + k <= n and words[i - k:i] == words[i:i + k]:
                    i += k
            else:
                out.append(words[i])
                i += 1
        words, n = out, len(out)
    # elimina palabras duplicadas contiguas sueltas
    out, prev = [], None
    for w in words:
        if w != prev:
            out.append(w)
        prev = w
    return " ".join(out)

BAD_TITLE_TOKENS = {"di", "de", "del", "la", "el", "con", "by", "and", "y", "alt"}

def _fix_obvious_bad_title(title: Optional[str], guess_title: Optional[str], cleaned: str) -> str:
    t = (title or "").strip()
   
    # Evita títulos que son "Nombre Apellido" si hay alternativa razonable
    if _is_person_like_title(t):
        alt = (guess_title or cleaned)
        if alt and not _is_person_like_title(alt):
            return alt

    if not t:
        return (guess_title or cleaned)
    
    # solo números o patrón tipo "5 1" o "5.1" o "7-1"
    if re.fullmatch(r"\d+(?:\s*[\.\- ]\s*\d+)?", t):
        return (guess_title or cleaned)
    # canales de audio colándose como título
    if re.search(r"(?i)\b(?:[257]\s*[\.\- ]\s*[01])\b", t):
        return (guess_title or cleaned)
        
    # títulos muy cortos y con preps/ruido
    words = t.split()
    if len(words) <= 2 and any(w.lower() in BAD_TITLE_TOKENS for w in words):
        base = re.split(r"\s+-\s+|\s*\((?:19|20)\d{2}\)\s*", cleaned)[0].strip()
        return (guess_title or base or t)
    
    # uploader/group junk detected as the 'title'
    if UPLOADER_LIST_RE.search(t) or re.fullmatch(r"(?i)(rip|die)", t):
        base = re.split(r"\s+-\s+|\s*\((?:19|20)\d{2}\)\s*", cleaned)[0].strip()
        return (guess_title or base or t)
    
    return t

UPLOADER_WORDS = [
    "xusman","remy","geot","lele753","nuita","nueng","aspide","canibales",
    "exploradoresp2p","nocturniap2p","filibusteros","paso77","toy-foracrew",
    "hispashare","mokesky","king76","wolfmax4k","lyis","napoleon21","diavliyo",
    "papa noel","guayiga","gautxori","mck","lyrici","byred","sienteme"
]
UPLOADER_RE = re.compile(r"(?i)\b(?:by|para)\s+\w+\b")
UPLOADER_LIST_RE = re.compile(r"(?i)\b(" + "|".join(map(re.escape, UPLOADER_WORDS)) + r")\b")

COMMON_NON_UPLOADER_TAILS: Set[str] = {
    "extended", "uncut", "version", "montaje", "director", "cut",
    "remaster", "remastered", "edition", "edicion", "edición"
}

def _looks_like_uploader_tail(tail: str) -> bool:
    """
    Return True if the tail string looks like an uploader/group handle or tag.
    This is a soft hint; boundary guard remains authoritative.
    """
    t = tail.strip(" []-–—").lower()
    if not t:
        return False
    if 'UPLOADER_LIST_RE' in globals() and UPLOADER_LIST_RE.search(t):
        return True
    if re.fullmatch(r"@?[\w.\-]{3,16}", t):
        return True
    if re.fullmatch(r"[a-z0-9][a-z0-9._\-]{2,12}", t) and t not in COMMON_NON_UPLOADER_TAILS:
        return True
    return False

KNOWN_TITLE_OVERRIDES: Dict[Tuple[str, int], str] = {
    ("movie", 62): "2001: A Space Odyssey",
}

LOCALIZED_ALIAS_HINTS: Dict[str, str] = {}
KNOWN_YEAR_LOCKS: Dict[str, int] = {}

def strip_uploader_from_title(title: str) -> str:
    """
    Remove uploader/group signatures from a chosen title,
    e.g. 'by Geot', 'xusman', 'lele753', site tags, etc.
    """
    t = title
    t = UPLOADER_RE.sub(" ", t)
    t = UPLOADER_LIST_RE.sub(" ", t)
    t = DOMAINS_ANY_RE.sub(" ", t)
    t = re.sub(r"\s+", " ", t).strip(" -_,.")
    return t


def _cache_key_for_name(name: str, cfg: Dict[str, Any]) -> str:
    return basename_key_from_name(name, cfg)


def _remember_pick(key: str, kind: str, title: str, year: Optional[int], tmdb_id: Optional[int]) -> None:
    global KNOWN_PICK_DIRTY
    if not key or not title:
        return
    base_type = "movie" if "movie" in kind else "tv"
    _KNOWN_PICK_CACHE[key] = {
        "type": base_type,
        "title": title,
        "year": year,
        "tmdb_id": tmdb_id,
    }
    KNOWN_PICK_DIRTY = True
    _save_known_pick_cache()

# ------------------------------- Classifier -----------------------------------

def classify_and_build_paths(cfg: Dict[str, Any], path: Path) -> Dest:
    """
    Classify a file as movie/TV/unclassified and build destination folder/file paths.

    Pipeline:
      1) Preprocess filename (stem) to strip noise and normalize.
      2) GuessIt parse on cleaned name.
      3) Suspicion heuristics for TV misclassifications (years as seasons, huge episode numbers + 'dias').
      4) Metadata normalization (TMDb) and kids flag (<12).
      5) Title dedupe + quality check + sanitation.
      6) Path building (kids/non-kids).
      7) Fallback to Unclassified.
    """
    raw_name = path.stem
    cleaned = preprocess_name(raw_name)

    # Collect ALL plausible years from the raw filename
    filename_years = extract_filename_years(raw_name)
    allowed_years = set(filename_years) if filename_years else None

    cache_key = _cache_key_for_name(path.name, cfg)
    cached_pick = _KNOWN_PICK_CACHE.get(cache_key)

    # Phase-1 baseline lock: allow disabling learned cache to avoid non-deterministic rescues
    use_known_picks = bool(cfg_get(cfg, "use_known_picks", cfg_get(cfg, "metadata.use_known_picks", True))) #To be confirmed insertion

    if not use_known_picks:
        cached_pick = None

    # Two variants: one for GuessIt/TV hints, one for TMDb queries (with ALT)
    cleaned_for_guess = cleaned
    cleaned_for_tmdb  = prefer_ascii_parenthetical(cleaned)  # may append " ||ALT|| "

    # Year hint must be extracted from the ALT-free string
    m_year = re.search(r"\(((?:19|20)\d{2})\)", cleaned_for_guess)
    
    #year_hint = int(m_year.group(1)) if m_year else None
    
    # Prefer the first year token in the filename as a hint; hard filter uses all
    year_hint = filename_years[0] if filename_years else None

    # GuessIt must NOT see ALT to avoid poisoning the parse
    g = guessit(cleaned_for_guess)
    guess_title = g.get("title") or g.get("series")
    gtype = g.get("type")
    year = g.get("year")
    season = g.get("season")
    episode = g.get("episode")

    # SIEMPRE tener un título base para no reventar más tarde
    title = guess_title or cleaned

    # TV hints must be computed over a source that keeps all raw markers
    # (raw_name may still contain 1x03, Cap.105, Temporada 1, etc.)
    _tv_source = f"{raw_name} || {cleaned_for_guess}"
    tv_hint = bool(re.search(
        r"(S\d{1,2}E\d{1,2})"                      # SxxExx
        r"|\bTemporada\b"                           # palabra temporada
        r"|\bCap(?:\.|itulo|ítulo)?\s*\d{2,4}\b"    # Cap.101 / Capitulo 1105
        r"|\b\d+x\d{2}\b",                          # 1x03
        _tv_source, re.IGNORECASE
    ))

    # Si hay marcadores de episodio en el nombre, usa esos valores como prioridad
    s_parsed, e_parsed = parse_episode_markers(raw_name)
    if s_parsed is not None:
        season  = s_parsed
    if e_parsed is not None:
        episode = e_parsed

    # Si existen marcadores o tv_hint, fuerza 'episode' si GuessIt no lo puso
    if tv_hint and gtype != "episode":
        gtype = "episode"    
        
    logger.debug(
        "cleaned=%r | year_hint=%r | tv_hint=%r | guess_title=%r | gtype=%r | season=%r | episode=%r",
        cleaned_for_guess, year_hint, tv_hint, g.get("title") or g.get("series"),
        g.get("type"), g.get("season"), g.get("episode")
    )
  
    # Si GuessIt dijo 'episode' pero NO hay marcadores de TV y el título
    # empieza por un número seguido de palabra, asumimos película (caso '12 Monos').
    if gtype == "episode" and not tv_hint:
        if looks_like_leading_number_title(cleaned_for_guess):
            gtype = "movie"
            season, episode = None, None

    # Heurísticas de sospecha
    suspicious = False
    if gtype == "episode":
        if isinstance(season, int) and 1900 <= season <= 2099:
            suspicious = True
        if isinstance(episode, int) and episode >= 366 and SUS_EP_WORDS_RE.search(cleaned):
            suspicious = True

    # Metadata TMDb
    threshold = int(cfg_get(cfg, "metadata.fuzzy_threshold", 80))
    meta_type, meta_title, meta_year, meta_score, meta_id, meta_age, meta_genres = normalize_with_metadata(
        cfg, cleaned_for_tmdb, year_hint=year_hint, tv_hint=tv_hint, allowed_years=allowed_years
    )

    if cfg_get(cfg, "use_known_picks", True) and (meta_type is None or (meta_score or 0) < threshold) and cached_pick:
        cached_type = cached_pick.get("type")
        if cached_type:
            meta_type = cached_type
            meta_score = max(meta_score or 0, threshold + 5)
        if cached_pick.get("title") and not meta_title:
            meta_title = cached_pick.get("title")
        if cached_pick.get("year") and not meta_year:
            meta_year = cached_pick.get("year")
        if cached_pick.get("tmdb_id") and not meta_id:
            meta_id = cached_pick.get("tmdb_id")
        if not year_hint and cached_pick.get("year"):
            year_hint = cached_pick.get("year")

    # Prefer movie when there are NO TV markers and metadata is confidently 'movie'
    # This avoids misclassifying movies as TV episodes when filenames are messy.
    if (not tv_hint) and meta_type == "movie" and gtype == "episode" and meta_score is not None and meta_score >= threshold:
        gtype = "movie"
        title = meta_title or guess_title or cleaned
        year = meta_year or year
        
    # Regla nueva para Infantiles
    kids_cfg = (cfg.get("kids") or {})
    max_age = int(kids_cfg.get("max_age", 7))
    require = {g.lower() for g in kids_cfg.get("require_genre_any", ["Family", "Animation", "Kids"])}
    blacklist = [w.lower() for w in kids_cfg.get("blacklist_keywords", ["biopic","war","historical","drama"])]

    genres_l = [g.lower() for g in (meta_genres or [])]
    orig_l = (meta_title or guess_title or cleaned).lower()

    is_kids = (
        (
            (meta_age is not None and meta_age <= max_age)
            or any(g in require for g in genres_l)
        )
        and not any(b in orig_l for b in blacklist)
    )

    # Resolver tipo y título preferido
    if meta_type:
        # If we have no prior type or file looks suspicious, trust metadata
        if suspicious or not gtype:
            gtype = meta_type
            title = meta_title or guess_title or cleaned
            year  = meta_year  or year
        else:
            # Allow switching types only when confident; forbid jumping TO TV without tv_hint
            if meta_type != gtype and meta_score is not None and meta_score >= threshold:
                if meta_type == "tv" and not tv_hint:
                    pass  # don't switch to TV without filename TV hints
                else:
                    gtype = meta_type
                    title = meta_title or guess_title or cleaned
                    year  = meta_year  or year
            else:
                title = guess_title or meta_title or cleaned

    # If metadata confidently says TV and we still aren't an 'episode', assume S01E01
    if (meta_type == "tv") and (meta_score is not None) and (meta_score >= threshold) and (gtype != "episode"):
        if tv_hint and (gtype != "episode"):
            gtype   = "episode"
            season  = season  or 1
            episode = episode or 1    

    # Año final para carpeta:
    # - Si hay pick de TMDb para película, manda el año del pick.
    # - Si no hay año y tenemos year_hint fiable, úsalo.
    # No hardcoded year locks; rely on filename years + metadata confidence

    if meta_type == "movie" and meta_year:
        if allowed_years and meta_year not in allowed_years and year_hint is not None:
            year = year_hint
        elif year_hint is not None and abs(meta_year - year_hint) > 1 and (meta_score or 0) < threshold:
            year = year_hint
        else:
            year = meta_year
    elif gtype == "movie" and (not year) and year_hint is not None:
        year = year_hint
    elif year_hint is not None and year is not None and abs(year - year_hint) > 1 and (meta_score or 0) < threshold:
        year = year_hint

    # Arreglo de títulos claramente malos
    title = _fix_obvious_bad_title(title, guess_title, cleaned)

    # Strip uploader signatures that slipped through
    title = strip_uploader_from_title(title)

    display_title = strip_release_tokens_for_display(title)
    display_title = dedupe_title_phrases(display_title)
    if display_title:
        title = display_title

    if not year:
        m_y = re.search(r"\b(19|20)\d{2}\b", raw_name)
        if m_y:
            try:
                year = int(m_y.group(0))
            except Exception:
                pass

    # If metadata failed and this "movie" title looks too weak, don't file it as movie
    if (meta_type is None) and (gtype == "movie"):
        tokens = (title or "").split()
        if (len(tokens) < 2) or (len(title or "") <= 3 and not year):
            gtype = None  # force Unclassified later

    # Calidad de título: dedupe + sanitation + fallback transliterado si queda pobre
    strategy = cfg_get(cfg, "renamer.non_latin_strategy", "transliterate")
    keep_chars = cfg_get(cfg, "renamer.keep_chars", r"A-Za-z0-9 .,'()!_-")

    original_meta_title = title
    title = dedupe_title_phrases(title)
    title = sanitize_filename(title, strategy=strategy, keep_chars=keep_chars)

    q = name_quality(title)
    if not q["ok"]:
        alt = sanitize_filename(original_meta_title, strategy="transliterate", keep_chars=keep_chars)
        alt = dedupe_title_phrases(alt)
        if name_quality(alt)["ok"]:
            logging.getLogger("EmuleToPlex").debug(
                "Title quality low ('%s'), using transliterated alt '%s'", title, alt
            )
            title = alt

    # Raíces
    movies_root = Path(cfg_get(cfg, "plex.movies_root", "") or "").resolve() if cfg.get("plex") else None
    shows_root  = Path(cfg_get(cfg, "plex.shows_root", "")  or "").resolve() if cfg.get("plex") else None
    movies_kids_root = Path(cfg_get(cfg, "plex.movies_kids_root", "") or "").resolve() if cfg.get("plex") else None
    shows_kids_root  = Path(cfg_get(cfg, "plex.shows_kids_root", "")  or "").resolve() if cfg.get("plex") else None

    if not movies_kids_root and movies_root:
        movies_kids_root = movies_root.parent / (movies_root.name + "_Infantiles")
    if not shows_kids_root and shows_root:
        shows_kids_root = shows_root.parent / (shows_root.name + "_Infantiles")

    unclass_root_name = cfg_get(cfg, "folders.unclassified_root", "Unclassified")
    suffix = path.suffix.lower()

    # TV
    if gtype == "episode" and (shows_root or shows_kids_root):
        s_num = f"{int(season):02d}" if isinstance(season, int) else "01"
        if isinstance(episode, list):
            parts = [f"{int(e):02d}" for e in episode if isinstance(e, int)]
            e_num = "E".join(parts) if parts else "01"
        elif isinstance(episode, int):
            e_num = f"{int(episode):02d}"
        else:
            e_num = "01"

        series_title = strip_release_tokens_for_display(title) or title
        series_folder = sanitize_filename(series_title, strategy=strategy, keep_chars=keep_chars)
        episode_title_src = strip_release_tokens_for_display(title) or title
        base_display  = sanitize_filename(f"{episode_title_src} - S{s_num}E{e_num}", strategy=strategy, keep_chars=keep_chars)

        root = shows_kids_root if (is_kids and shows_kids_root) else shows_root
        if root:
            dest_folder = root / series_folder / f"Season {s_num}"
            dest_file   = dest_folder / f"{base_display}{suffix}"
            kind = "tv_kids" if is_kids else "tv"
            _remember_pick(cache_key, kind, title, year, meta_id)
            return Dest(kind, dest_folder, dest_file, title)

    # Si parece película y no hay 'year' pero sí 'year_hint', úsalo
    if (gtype == "movie") and (not year):
        if 'year_hint' in locals() and year_hint:
            year = year_hint
    
    # Movie
    if gtype == "movie" and (movies_root or movies_kids_root):
        ypart       = f" ({year})" if year else ""
        folder_raw  = f"{title}{ypart}"
        folder_disp = sanitize_filename(folder_raw, strategy=strategy, keep_chars=keep_chars)

        root = movies_kids_root if (is_kids and movies_kids_root) else movies_root
        if root:
            dest_folder = root / folder_disp
            dest_file   = dest_folder / f"{folder_disp}{suffix}"
            kind = "movie_kids" if is_kids else "movie"
            _remember_pick(cache_key, kind, title, year, meta_id)
            return Dest(kind, dest_folder, dest_file, title)

    # Unclassified (ahora SIEMPRE en raíz superior, no dentro de Peliculas/Series)
    unclass_root = compute_unclassified_root(cfg)

    name_disp = sanitize_filename(path.stem, strategy=strategy, keep_chars=keep_chars)
    dest_folder = unclass_root / name_disp
    dest_file = dest_folder / f"{name_disp}{suffix}"

    return Dest("unclassified", dest_folder, dest_file, title or name_disp)

# ------------------------------- File moving ----------------------------------

def move_with_sidecars(src: Path, dst: Path, sidecars: Optional[Iterable[str]], dry_run: bool = False) -> List[Path]:
    """
    Move a file and its sidecar companions (.srt, .nfo, etc.) to the destination.
    Recorta componentes si la ruta es demasiado larga para Windows.
    """
    moved: List[Path] = []

    # Genera un destino seguro para Windows (sanitizado + truncado)
    dst = _shorten_for_windows(dst)

    # Asegura directorio destino
    dst.parent.mkdir(parents=True, exist_ok=True)

    # Mueve el principal
    if not dry_run:
        shutil.move(str(src), str(dst))
    moved.append(dst)

    # Mueve sidecars si existen
    if sidecars:
        base_src = src.with_suffix("")  # base del origen
        base_dst = dst.with_suffix("")  # base del destino
        for ext in sidecars:
            ext_norm = ext if ext.startswith(".") else f".{ext}"
            ext_norm = ext_norm.lower()

            cand = base_src.with_suffix(ext_norm)
            if cand.exists():
                target = base_dst.with_suffix(ext_norm)
                # Por si el sidecar + extensión nos rompe el límite, re-aplica recorte
                target = _shorten_for_windows(target)
                try:
                    if not dry_run:
                        target.parent.mkdir(parents=True, exist_ok=True)
                        shutil.move(str(cand), str(target))
                    moved.append(target)
                except Exception:
                    # best-effort, no paramos por un sidecar
                    pass

    return moved

@retry(wait=wait_fixed(5), stop=stop_after_attempt(10), retry=retry_if_exception_type(requests.RequestException))
def plex_refresh_by_section(base_url: str, token: str, section_id: Union[int, str], path: Optional[str] = None,
                            logger: Optional[logging.Logger] = None) -> bool:
    """
    Trigger a Plex library refresh, optionally path-scoped.

    Parameters
    ----------
    base_url : str
        Plex server base URL (e.g., http://127.0.0.1:32400).
    token : str
        Plex X-Plex-Token.
    section_id : int or str
        Plex library section key.
    path : str or None
        Optional path under the section to refresh.
    logger : logging.Logger or None
        Logger for tracing.

    Returns
    -------
    bool
        True if the request did not raise and Plex accepted the trigger.
    """
    headers = {"X-Plex-Token": token}
    if path:
        url = f"{base_url}/library/sections/{section_id}/refresh?path={quote(path)}"
    else:
        url = f"{base_url}/library/sections/{section_id}/refresh"
    if logger:
        logger.info(f"Triggering Plex refresh: {url}")
    r = requests.get(url, headers=headers, timeout=10)
    r.raise_for_status()
    return True


# --------------------------------- Watcher ------------------------------------

class IngestHandler(FileSystemEventHandler):
    """
    Watchdog event handler that ingests new or moved files according to the
    classification pipeline and configured actions.
    """

    def __init__(self, cfg: Dict[str, Any], logger: logging.Logger, test_mode: bool = False, dry_run: bool = False) -> None:
        """
        Parameters
        ----------
        cfg : dict
            Configuration dictionary.
        logger : logging.Logger
            Logger for progress and diagnostics.
        test_mode : bool
            If True, treat .txt as allowed, skip Plex, skip lock/stability checks (or make them trivial).
        dry_run : bool
            If True, do not perform moves; log intended actions only.
        """
        super().__init__()
        self.cfg = cfg
        self.logger = logger
        self.test_mode = test_mode
        self.dry_run = dry_run
        self._lock = threading.Lock()
        self.manifest: List[ManifestRec] = []
        self._autosave_every = max(1, int(cfg_get(cfg, "logging.autosave_every", 50)))
        self._processed_since_autosave = 0

    def on_created(self, event: FileCreatedEvent) -> None:
        """Trigger ingestion for newly created files."""
        if isinstance(event, FileCreatedEvent):
            self._maybe_ingest(Path(event.src_path))

    def on_moved(self, event: FileMovedEvent) -> None:
        """Trigger ingestion for files moved into watched folders."""
        if isinstance(event, FileMovedEvent):
            self._maybe_ingest(Path(event.dest_path))

    def _maybe_ingest(self, path: Path) -> None:
        """
        Evaluate and process a candidate file if it passes constraints.

        Parameters
        ----------
        path : pathlib.Path
            File path to consider for ingestion.
        """
        if path.is_dir():
            return

        # Test-mode hard guard: solo placeholders .txt
        if self.test_mode and path.suffix.lower() != ".txt":
            # Evita tocar vídeos reales aunque alguien escanee rutas equivocadas
            self.manifest.append(ManifestRec(src=str(path), decision="skipped_nontxt_in_test", dest=""))
            if hasattr(self, "logger"):
                self.logger.debug("Test mode: skipping non-placeholder %s", path)
            return
                
        allowed = self.cfg.get("emule", {}).get("allowed_extensions", [])
        if not allowed_extension(path, allowed):
            self.manifest.append(ManifestRec(src=str(path), decision="skipped_ext", dest=""))
            return

        stable_seconds = int(self.cfg.get("emule", {}).get("stable_seconds", 120))
        sidecars = self.cfg.get("emule", {}).get("sidecar_extensions", [])

        self.logger.info(f"Detected candidate: {path}")

        # In test mode, we skip heavy stability/lock checks to keep it snappy
        if not self.test_mode:
            if not is_stable(path, stable_seconds):
                self.manifest.append(ManifestRec(src=str(path), decision="skipped_unstable", dest=""))
                self.logger.info(f"Skipping {path} (not stable after {stable_seconds}s)")
                return
            if is_locked(path):
                self.manifest.append(ManifestRec(src=str(path), decision="skipped_locked", dest=""))
                self.logger.info(f"Skipping {path} (locked by another process)")
                return

        
        classify_error: Optional[str] = None

        try:
            dest = classify_and_build_paths(self.cfg, path)
            #self.logger.info(f"Classified as {dest.kind}. Moving to {dest.file}")
        except Exception as e:
            # Fallback directo a Unclassified
            # Unclassified de nivel superior también en fallback de excepción
            unclass_root = compute_unclassified_root(self.cfg)
            name_disp = sanitize_filename(path.stem)
            dest_folder = unclass_root / name_disp
            dest_file = dest_folder / f"{name_disp}{path.suffix.lower()}"
            dest = Dest("unclassified", dest_folder, dest_file, name_disp)

            # Un único trazo de excepción
            self.logger.exception("Classification error on %s, sending to Unclassified", path)
            classify_error = str(e)

        # Conflictos
        on_conflict = (self.cfg.get("renamer", {}) or {}).get("on_conflict", "suffix").lower()
        target = dest.file
        if target.exists():
            if on_conflict == "skip":
                self.logger.warning(f"Destination exists, skipping: {target}")
                self.manifest.append(ManifestRec(src=str(path), decision="skipped_conflict", dest=str(target), title=dest.title))
                return
            elif on_conflict == "overwrite":
                pass
            else:  # suffix
                i = 1
                while True:
                    alt = target.with_name(f"{target.stem}_{i}{target.suffix}")
                    if not alt.exists():
                        target = alt
                        break
                    i += 1
                dest = Dest(dest.kind, target.parent, target, dest.title)

        # Previsualiza destino real (ya acortado) para log consistente
        preview = _shorten_for_windows(target)
        self.logger.info(f"Classified as {dest.kind}. Moving to {preview}")
        
        rec = ManifestRec(src=str(path), decision=dest.kind, dest=str(preview), title=dest.title)

        #Un único registro final por archivo. Se conserva el error para diagnóstico sin romper el recuento.
        if classify_error:
            rec.error = classify_error
        
        try:
            moved = move_with_sidecars(path, preview, sidecars, dry_run=self.dry_run)
            
            # Si el destino acortado difiere del original 'target', borra la carpeta larga si quedó vacía
            try:
                if preview != target:
                    _rmdir_if_empty(target.parent)
            except Exception:
                pass
            
            # actualiza el destino en el manifest con la ruta final usada
            if moved:
                rec.dest = str(moved[0])
            self.manifest.append(rec)
            self.logger.info(f"Moved: {', '.join(map(str, moved))}")

        except Exception as e:
            self.logger.exception("Move error on %s -> %s", path, target)
            self.manifest.append(ManifestRec(src=str(path), decision="error_move", dest=str(target), error=str(e)))
            return

        self._processed_since_autosave += 1
        if self._processed_since_autosave >= self._autosave_every:
            try:
                self._autosave_manifest()
            finally:
                self._processed_since_autosave = 0

        # Plex refresh (disabled in test-mode)
        plex = self.cfg.get("plex", {}) or {}
        token = plex.get("token")
        base_url = plex.get("base_url")

        if (not self.test_mode) and token and base_url:
            section_id = None
            if dest.kind == "movie":
                section_id = plex.get("movies_section_id")
            elif dest.kind == "movie_kids":
                section_id = plex.get("movies_kids_section_id") or plex.get("movies_section_id")
            elif dest.kind == "tv":
                section_id = plex.get("shows_section_id")
            elif dest.kind == "tv_kids":
                section_id = plex.get("shows_kids_section_id") or plex.get("shows_section_id")
            else:
                section_id = plex.get("movies_section_id") or plex.get("shows_section_id")

            try:
                if section_id:
                    plex_refresh_by_section(
                        base_url, token, section_id, path=str(target.parent), logger=self.logger
                    )
                    self.logger.info("Plex refresh triggered successfully")
                else:
                    self.logger.warning("No section ID configured; set movies_section_id/shows_section_id or *_kids_section_id")
            except Exception as e:
                self.logger.error(f"Plex refresh failed: {e}")
        else:
            if self.test_mode:
                self.logger.warning("Test mode: Plex refresh disabled")

    def _autosave_manifest(self) -> None:
        snapshot = {
            "records": [asdict(r) for r in self.manifest],
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        }
        try:
            Path("_manifest.autosave.json").write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
            self.logger.debug("Autosave manifest snapshot written")
        except Exception:
            self.logger.exception("Autosave manifest failed")

    def second_pass_unclassified(self) -> None:
        """
        Run a second-pass classification over 'Unclassified' items.

        Workflow:
        1) Descubre las carpetas 'Unclassified' bajo movies_root y shows_root.
        2) Para cada stub .txt:
        - Reintenta clasificación normal con el nombre actual.
        - Si sigue como 'unclassified', genera candidatos extra (frase latina
            multi-palabra más larga, segmento tras año en nombres con guiones,
            romanización si procede) y reintenta inyectando un ALT en memoria
            sin tocar disco.
        3) Si algún intento clasifica correctamente:
        - Mueve el stub (y sidecars) al destino final.
        - Sustituye en el manifest cualquier registro previo del mismo 'src'
            por el nuevo registro rescatado.
        4) Loguea el número total de rescatados.

        No modifica otras funcionalidades, respeta PEP8 y añade comentarios clave.
        """

        unclass_name = cfg_get(self.cfg, "folders.unclassified_root", "Unclassified")
        
        # Determina raíces 'Unclassified' (top-level + legacy)
        roots_set: Set[Path] = set()

        # Nueva raíz superior
        try:
            roots_set.add(compute_unclassified_root(self.cfg).resolve())
        except Exception:
            pass

        # Raíces legacy dentro de Peliculas/Series (por compatibilidad/migración)
        if self.cfg.get("plex"):
            mr = cfg_get(self.cfg, "plex.movies_root", "") or ""
            sr = cfg_get(self.cfg, "plex.shows_root", "") or ""
            if mr:
                roots_set.add((Path(mr).resolve() / unclass_name).resolve())
            if sr:
                roots_set.add((Path(sr).resolve() / unclass_name).resolve())

        #Lista final sin duplicados
        #roots = [p for p in roots_set if p.exists()]
        roots = sorted(
            (p for p in roots_set if p.exists()),
            key=lambda p: p.as_posix().casefold()
        )

        def _build_extra_candidates(stem: str) -> List[str]:
            """
            Construye candidatos alternativos de búsqueda a partir del nombre base:
            - Frase latina multi-palabra más larga (mejor señal de título).
            - Segmento inmediatamente posterior a un año en nombres con guiones.
            - Romanización si hay caracteres no latinos.
            Se devuelve una lista deduplicada ya pasada por 'clean_query_text'.
            """
            cands: List[str] = []
            base = stem

            # Normalize dots/underscores for readability in extra queries
            base = WORD_SEPS_RE.sub(" ", base)
            base = LANG_TAGS_RE.sub(" ", base)
            if 'UPLOADER_LIST_RE' in globals():
                base = UPLOADER_LIST_RE.sub(" ", base)
            base_lower = base.casefold()

            def _clean_chunk(text: str) -> Optional[str]:
                cleaned = LANG_TAGS_RE.sub(" ", text)
                if 'UPLOADER_LIST_RE' in globals():
                    cleaned = UPLOADER_LIST_RE.sub(" ", cleaned)
                cleaned = RELEASE_TAGS_RE.sub(" ", cleaned)
                cleaned = " ".join(cleaned.split())
                if len(re.findall(r"[A-Za-z]{3,}", cleaned)) < 1:
                    return None
                return cleaned

            # Mejor frase latina multi-palabra (reduce ruido de tags)
            m_all = re.findall(r"[A-Za-z][A-Za-z]+(?:\s+[A-Za-z][A-Za-z]+)+", base)
            if m_all:
                best_phrase = max(m_all, key=len).strip()
                cleaned_phrase = _clean_chunk(best_phrase)
                if cleaned_phrase:
                    cands.append(cleaned_phrase)

            # Segmento justo después de un año en nombres con guiones: "1999 - Título"
            parts = [p.strip() for p in re.split(r"\s*-\s*", base) if p.strip()]
            for i, p in enumerate(parts):
                if re.fullmatch(r"(?:19|20)\d{2}", p):
                    if i + 1 < len(parts):
                        cleaned_part = _clean_chunk(parts[i + 1])
                        if cleaned_part:
                            cands.append(cleaned_part)

            for key, alias in LOCALIZED_ALIAS_HINTS.items():
                if key and key in base_lower:
                    cleaned_alias = _clean_chunk(alias)
                    if cleaned_alias:
                        cands.append(cleaned_alias)

            # Romanización si hay no-latín
            try:
                from unidecode import unidecode
                roman = unidecode(base)
                if roman and roman != base:
                    cleaned_roman = _clean_chunk(roman)
                    if cleaned_roman:
                        cands.append(cleaned_roman)
            except Exception:
                # No rompas el flujo si 'unidecode' no está o falla
                pass

            # inner parentheses often hold English title or aka; split on common delimiters
            for inner in re.findall(r"\(([^)]+)\)", stem):
                for chunk in re.split(r"[;,/|\-]+", inner):
                    chunk = chunk.strip()
                    # Ignore pure language/upload tags
                    if not chunk or RELEASE_TAGS_RE.search(chunk) or DOMAINS_ANY_RE.search(chunk):
                        continue
                    cleaned_chunk = _clean_chunk(chunk)
                    if cleaned_chunk:
                        cands.append(cleaned_chunk)

            # Limpieza fuerte para uso en queries
            cands = [clean_query_text(x) for x in cands if x]

            # Dedupe manteniendo orden
            seen = set()
            return [x for x in cands if not (x in seen or seen.add(x))]

        rescued = 0

        for root in roots:
            if not root.exists():
                continue

            for txt in sorted(root.rglob("*.txt"), key=lambda p: p.name.casefold()):
                stem = txt.stem
                try:
                    # 1) Reintento normal con el pipeline actual
                    dest = classify_and_build_paths(self.cfg, txt)

                    if dest.kind == "unclassified":
                        # 2) Candidatos extra y "inyección" ALT (no toca disco)
                        extras = _build_extra_candidates(stem)
                        for alt in extras:
                            injected = f"{stem} ||ALT|| {alt}{txt.suffix}"
                            temp = txt.with_name(injected)
                            try:
                                dest2 = classify_and_build_paths(self.cfg, temp)
                            finally:
                                # No creamos ni movemos 'temp'; solo usamos su nombre para el parser
                                pass
                            if dest2.kind != "unclassified":
                                dest = dest2
                                break

                    if dest.kind != "unclassified":
                        # Rescatado: mover stub real y actualizar manifest
                        self.logger.info("2nd pass rescued: %s -> %s", txt.name, dest.kind)
                        target = _shorten_for_windows(dest.file)

                        rec = ManifestRec(
                            src=str(txt),
                            decision=dest.kind,
                            dest=str(target),
                            title=dest.title
                        )

                        moved = move_with_sidecars(txt, target, [], dry_run=self.dry_run)
                        if moved:
                            rec.dest = str(moved[0])

                        # Robust manifest replacement using a NORMALIZED BASENAME KEY
                        base_key = basename_key_from_name(txt.name, self.cfg)

                        # Find previous records by normalized key and keep original src if any
                        prevs = [
                            r for r in self.manifest
                            if basename_key_from_src(r.src, self.cfg) == base_key
                        ]
                        if prevs:
                            # Keep the original source path (pre-Unclassified) for clean history
                            orig_src = prevs[-1].src
                            rec.src = orig_src
                        else:
                            # First time we see this file in manifest; keep current stub src
                            rec.src = str(txt)

                        # Remove ANY previous record of this file (by normalized key)
                        self.manifest = [
                            r for r in self.manifest
                            if basename_key_from_src(r.src, self.cfg) != base_key
                        ]

                        # Insert final record (classified)
                        self.manifest.append(rec)

                        rescued += 1

                except Exception:
                    # No detengas toda la pasada por un fallo puntual
                    self.logger.exception("2nd pass failed for %s", txt)

        if rescued:
            self.logger.info("Second pass rescued %d items from Unclassified", rescued)
    
				
# --- NUEVO: helpers de certificación/edad TMDb ---

def _tmdb_get_movie_cert_age(api_key: str, movie_id: int, timeout: int = 8) -> Optional[int]:
  
    url = f"https://api.themoviedb.org/3/movie/{movie_id}/release_dates"

    logger.debug("TMDb movie certifications: id=%s", movie_id)

    try:
        data = _tmdb_get(url, api_key=api_key, timeout=timeout).get("results") or []
        logger.debug("TMDb movie certifications: entries=%d", len(data))
    except Exception:
        logger.exception("TMDb movie certifications failed")
        return None

    return _extract_age_from_cert_blocks(data)

def _tmdb_get_tv_cert_age(api_key: str, tv_id: int, timeout: int = 8) -> Optional[int]:

    url = f"https://api.themoviedb.org/3/tv/{tv_id}/content_ratings"

    logger.debug("TMDb tv certifications: id=%s", tv_id)
    
    try:
        data = _tmdb_get(url, api_key=api_key, timeout=timeout).get("results") or []
        logger.debug("TMDb tv certifications: entries=%d", len(data))
    except Exception:
        logger.exception("TMDb tv certifications failed")
        return None

    return _extract_age_from_cert_blocks(data)

def _extract_age_from_cert_blocks(blocks: List[Dict[str, Any]]) -> Optional[int]:
    # Preferencia ES > US > GB > cualquier otro
    prefer = ["ES", "US", "GB"]
    by_country = {b.get("iso_3166_1"): b for b in blocks if b.get("iso_3166_1")}
    for cc in prefer + [k for k in by_country.keys() if k not in prefer]:
        b = by_country.get(cc)
        if not b:
            continue
        # Estructuras diferentes para movie vs tv; normalizamos a una lista de strings
        # Normalize both movie (release_dates) and TV (rating) into a list of strings
        certs: List[str] = []
        if "release_dates" in b and b.get("release_dates"):
            certs = [(x.get("certification") or "").strip() for x in b["release_dates"]]
        elif "rating" in b:
            certs = [(b.get("rating") or "").strip()]
        certs = [c for c in certs if c]
        if not certs:
            continue

        cand_ages: List[int] = []
        for c in certs:
            a = _cert_to_age(c, cc)
            if a is not None:
                cand_ages.append(a)
        if cand_ages:
            return min(cand_ages)
    return None

def _cert_to_age(cert: str, cc: str) -> Optional[int]:
    c = cert.upper().replace(" ", "")
    if not c:
        return None
    if cc == "ES":
        if c in {"TP", "APTA", "0"}: return 0
        if c.startswith("7"): return 7
        if c.startswith("12"): return 12
        if c.startswith("16"): return 16
        if c.startswith("18"): return 18
    if cc == "US":
        # Cine
        if c in {"G"}: return 0
        if c in {"PG"}: return 7
        if c in {"PG-13", "PG13"}: return 13
        if c in {"R"}: return 17
        if c in {"NC-17", "NC17"}: return 18
        # TV
        if c in {"TV-Y"}: return 0
        if c in {"TV-Y7"}: return 7
        if c in {"TV-G"}: return 0
        if c in {"TV-PG"}: return 10
        if c in {"TV-14"}: return 14
        if c in {"TV-MA"}: return 17
    if cc == "GB" or cc == "UK":
        if c in {"U"}: return 0
        if c in {"PG"}: return 8
        if c in {"12", "12A"}: return 12
        if c in {"15"}: return 15
        if c in {"18"}: return 18
    # Fallback: primeros números que aparezcan
    m = re.search(r"(\d{1,2})", c)
    return int(m.group(1)) if m else None				

def _tmdb_title_in_lang(api_key: str, media_type: str, tmdb_id: int, language: str, timeout: int = 8) -> Optional[str]:
    if media_type not in {"movie", "tv"}:
        return None
    base = f"https://api.themoviedb.org/3/{'movie' if media_type=='movie' else 'tv'}/{tmdb_id}"
    
    logger.debug("TMDb title lookup: media=%s id=%s lang=%s", media_type, tmdb_id, language)

    try:
        j = _tmdb_get(base, api_key=api_key, language=language, timeout=timeout)
    except Exception:
        logger.exception("TMDb title lookup failed")
        return None

    if media_type == "movie":
        return j.get("title") or j.get("original_title")
    else:
        return j.get("name") or j.get("original_name")

# ----------------------------------- main -------------------------------------

def main() -> None: 
    """
    Entry point:
    - Parse args
    - Load and adjust config for test-mode
    - Optionally run a one-shot scan (including --test-source) and exit
    - Or start watchdog observers for configured watch paths
    """
    args = parse_args()
    cfg = load_config()
    cfg = apply_test_overrides(cfg, args)
    
    # Here??? - Patch C (vC_7)- After merging CLI and YAML config into cfg: 

    use_known = bool(cfg_get(cfg, "use_known_picks", cfg_get(cfg, "metadata.use_known_picks", True)))
    init_known_pick_cache(use_known)
    
    logger = setup_logger(cfg)

    # After configuring logger in main():
    reset_tail_counters()

    # Determinism hygiene (Milestone 0): make runs reproducible
    # - Encourage stable hashing and any incidental randomness
    os.environ.setdefault("PYTHONHASHSEED", "0")
    try:
        random.seed(0)
    except Exception:
        # Random not critical, but don't fail startup because of it
        pass    

    # ------------------------------
    # Phase-1 Milestone 0: Baseline lock (flags + seed logging)
    # ------------------------------
    # Defaults for Phase-1 tests: skip second pass, don't use known picks cache.
    if args.test_mode:
        cfg.setdefault("phase1_only", True)
        cfg.setdefault("use_known_picks", False)

    # CLI overrides
    if getattr(args, "phase1_only", False):
        cfg["phase1_only"] = True
    if getattr(args, "no_known_picks", False):
        cfg["use_known_picks"] = False

    # Log the effective flags and the hash seed (for determinism traceability)
    phase1_only = bool(cfg_get(cfg, "phase1_only", False))
    use_known_picks = bool(cfg_get(cfg, "use_known_picks", cfg_get(cfg, "metadata.use_known_picks", True))) #To be confirmed insertion

    current_seed = os.environ.get("PYTHONHASHSEED", "randomized")
    logger.info(
        "Phase1 flags: phase1_only=%s, use_known_picks=%s, PYTHONHASHSEED=%s",
        phase1_only, use_known_picks, current_seed
    )
    if args.test_mode and (not current_seed or str(current_seed).lower() == "randomized"):
        logger.warning("PYTHONHASHSEED is not fixed. For deterministic Phase-1 runs set: PYTHONHASHSEED=0")

    # --- Test-mode one-shot path, guarded early ---
    if args.test_mode:
        if not args.test_source:
            logger.error("--test-mode requires --test-source")
            sys.exit(2)

        test_src = Path(args.test_source)
        if not test_src.exists():
            logger.error("Test source not found: %s", test_src)
            sys.exit(2)

        # Ensure the test output root exists (moves and manifest will go here)
        if args.test_output_root:
            Path(args.test_output_root).mkdir(parents=True, exist_ok=True)

        handler = IngestHandler(cfg, logger, test_mode=True, dry_run=args.dry_run)

        # process only *.txt placeholders in a deterministic, case-insensitive order
        test_txts = sorted(test_src.glob("*.txt"), key=lambda p: p.name.casefold())
        for f in test_txts:
            handler._maybe_ingest(f)

        # Phase-1 baseline lock: optionally skip second pass rescues entirely
        phase1_only = bool(cfg_get(cfg, "phase1_only", False))
        if not phase1_only:
            try:
                handler.second_pass_unclassified()
            except Exception:
                logger.exception("Second pass failed")
        else:
            logger.info("phase1_only=true → skipping second pass rescues for baseline stability")

        # Manifest y resumen después de rescatar
        write_manifest_and_summary(handler, cfg, args, logger)

        # Limpieza de carpetas vacías en Unclassified
        try:
            pruned = cleanup_unclassified_roots(cfg)
            if pruned:
                logger.info("Unclassified cleanup: removed %d empty folders", pruned)
        except Exception:
            logger.exception("Unclassified cleanup failed")
        return
        

    # --- Normal mode ---
    watch_paths: List[str] = (cfg.get("emule", {}) or {}).get("watch_paths", []) or []
    handler = IngestHandler(cfg, logger, test_mode=False, dry_run=args.dry_run)

    # Initial scan of configured watch paths
    for p in watch_paths:
        pth = Path(p)
        if pth.exists():
            logger.info(f"Scanning configured path: {pth}")
            for f in sorted(pth.glob("*"), key=lambda p: p.name.casefold()):
                handler._maybe_ingest(f)

    # Exit immediately in one-shot scenario
    if args.once:
        # 2ª pasada antes de escribir manifest
        if not cfg_get(cfg, "phase1_only", False):
            try:
                handler.second_pass_unclassified()
            except Exception:
                logger.exception("Second pass failed")

        write_manifest_and_summary(handler, cfg, args, logger)

        # Limpieza final
        try:
            pruned = cleanup_unclassified_roots(cfg)
            if pruned:
                logger.info("Unclassified cleanup: removed %d empty folders", pruned)
        except Exception:
            logger.exception("Unclassified cleanup failed")
        return

    # Otherwise, start watchdog observers for continuous operation
    observers: List[_ObserverLike] = []
    try:
        for p in watch_paths:
            pth = Path(p)
            if not pth.exists():
                pth.mkdir(parents=True, exist_ok=True)
            logger.info(f"Watching: {pth}")
            obs: _ObserverLike = Observer()
            obs.schedule(handler, str(pth), recursive=False)
            obs.start()
            observers.append(obs)

        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Interrupted by user, stopping observers...")
    finally:
        for obs in observers:
            obs.stop()
        for obs in observers:
            obs.join()

        # 2ª pasada: rescata Unclassified antes de cerrar manifest
        if not cfg_get(cfg, "phase1_only", False):
            try:
                handler.second_pass_unclassified()
            except Exception:
                logger.exception("Second pass failed")

        # Manifest y resumen al parar los observers
        write_manifest_and_summary(handler, cfg, args, logger)

        # After writing manifest/results and before exiting:
        emit_instrumentation_summary(logger)

        # Limpieza final de carpetas Unclassified vacías
        try:
            pruned = cleanup_unclassified_roots(cfg)
            if pruned:
                logger.info("Unclassified cleanup: removed %d empty folders", pruned)
        except Exception:
            logger.exception("Unclassified cleanup failed")

if __name__ == "__main__":
    main()
