# Agents Overview

This document maps the EmuleToPlex runner into “agents” — the key classes, functions, and components that act with clear responsibilities and collaborate to ingest files, normalize metadata, classify content, move/rename items to Plex‑ready structures, and trigger Plex refreshes.

Order: entrypoints → file watching → preprocessing → metadata → classification → moving/Plex → test/manifest → logging/errors.

**Entrypoints**
- `parse_args`
  - Role: Parse CLI flags (`--test-mode`, `--test-source`, `--test-output-root`, `--once`, `--dry-run`).
  - Inputs/Outputs: Reads argv; returns `argparse.Namespace`.
  - Interactions: Consumed by `main`.
- `load_config`
  - Role: Load YAML config from `config.yaml` or `config.example.yaml`.
  - Inputs/Outputs: Reads YAML file(s); returns `dict` config.
  - Interactions: Used by `main`, helpers via `cfg_get`.
- `cfg_get`
  - Role: Dotted‑path access to nested config settings.
  - Inputs/Outputs: `(cfg, dotted_path, default)` → resolved value.
  - Interactions: Widely used by most agents to read configuration.
- `apply_test_overrides`
  - Role: Mutate cfg for test mode (disable Plex, redirect roots, allow `.txt`, drop stability waits).
  - Inputs/Outputs: `(cfg, args)` → new cfg dict.
  - Interactions: Called by `main` before building the pipeline.
- `setup_logger`
  - Role: Configure rotating file logger plus level from config.
  - Inputs/Outputs: `(cfg)` → `logging.Logger`.
  - Interactions: Used by `main` and downstream agents for structured logs.
- `main`
  - Role: Program director. Wires args+config+logger, chooses test/once/watch modes, spins observers, and coordinates manifest writing and cleanup.
  - Inputs/Outputs: None/exit code side‑effect; orchestrates run lifecycle.
  - Interactions: Builds `IngestHandler`, starts `watchdog.Observer`s, calls second‑pass rescues, `write_manifest_and_summary`, and `cleanup_unclassified_roots` on exit.

**File Watching & Event Handling**
- `watchdog.Observer` (external)
  - Role: Filesystem watcher for configured `emule.watch_paths`.
  - Inputs/Outputs: OS file events → callbacks to handler.
  - Interactions: Schedules `IngestHandler` for each path.
- `IngestHandler(FileSystemEventHandler)`
  - Role: Core ingestion agent; reacts to file create/move, enforces guards, classifies, moves, and triggers Plex refresh.
  - Inputs: Events (`on_created`, `on_moved`), cfg, test flags; file paths.
  - Outputs: File moves; `_manifest` entries; logs; optional Plex refresh calls.
  - Interactions: Uses `is_stable`, `is_locked`, `allowed_extension`; `classify_and_build_paths`; `move_with_sidecars`; `plex_refresh_by_section`; `compute_unclassified_root`; `cleanup_unclassified_roots`; `second_pass_unclassified` for rescues; `write_manifest_and_summary` (via `main`).
  - Key methods:
    - `on_created`, `on_moved`: entry points to `_maybe_ingest`.
    - `_maybe_ingest(path)`: per‑file pipeline: guard checks → classify → conflict resolution → move + sidecars → manifest append → Plex refresh.
    - `second_pass_unclassified()`: rescuer that re‑tries Unclassified stubs using extra candidates (latin multi‑word phrases, segments after year, romanization, inner AKA chunks) without touching disk until a confident match is found.
  - Manifest: Keeps an in‑memory list of `ManifestRec`; de‑duped/merged later when writing `_manifest.json`.

**Preprocessing Pipeline**
- Regex library/constants: `ASPECT_RATIO_RE`, `DURATION_MIN_RE`, `NUM_PREFIX_RE`, `BRACKETS_RE`, `RELEASE_TAGS_RE`, `DOMAINS_*_RE`, `WORD_SEPS_RE`, `SUS_EP_WORDS_RE`, `START_JUNK_RE`, etc.
  - Role: Recognize and remove release junk, domains/uploader tokens, aspect ratios, duration tokens, suspect episode words, etc.
  - Inputs/Outputs: Raw name → cleaner string segments.
  - Interactions: Used by preprocessing helpers and scoring functions.
- `preprocess_name(raw)`
  - Role: Clean raw filename before parsing (strip junk, preserve TV markers, normalize separators/years, reorder typical patterns, pick best hyphen segment, Unicode normalize).
  - Inputs/Outputs: `raw stem` → cleaned string.
  - Interactions: Feeds GuessIt and metadata query prep.
- `parse_episode_markers(s)`
  - Role: Extract season/episode markers (SxxEyy, 1xNN, Cap.###/#, etc.).
  - Inputs/Outputs: String → `(season:int|None, episode:int|None)`.
  - Interactions: Informs TV classification and episode numbers.
- `pick_title_from_hyphens`, `_prefer_title_segment`, `_expand_cap_to_sxxexx`, `strip_uploader_tail`, `clean_query_text`, `prefer_ascii_parenthetical`
  - Role: Improve title segments and search queries; keep useful TV markers; surface ASCII alt titles.
  - Inputs/Outputs: Strings → refined strings.
  - Interactions: Used by `classify_and_build_paths` and `normalize_with_metadata` when crafting TMDb queries and deciding display titles.
- `sanitize_filename(name, strategy, keep_chars)` and `sanitize_path_component`
  - Role: Make filesystem‑safe, Plex‑friendly names; transliterate/drop/keep non‑Latin; filter illegal characters; normalize whitespace; avoid reserved names.
  - Inputs/Outputs: Display/path component → sanitized string.
  - Interactions: Used in folder/file building and Unclassified sink.
- Keys & tokens: `basename_key_from_src/name`, `extract_filename_years`, `is_mostly_non_latin`, `name_quality`, `dedupe_title_phrases`
  - Role: Normalize keys for manifest de‑dupe; gather year hints; heuristics for Unicode/quality; reduce repeated phrases in titles.
  - Inputs/Outputs: Strings → normalized keys/metrics/clean titles.
  - Interactions: Used across manifest logic and classification.

**Metadata Normalization (TMDb)**
- `_tmdb_get(url, **params)`
  - Role: Cached GET wrapper for TMDb endpoints with simple param handling.
  - Inputs/Outputs: URL + params → JSON dict.
  - Interactions: Called by all TMDb helpers.
- `tmdb_multi_search`, `tmdb_search_with_fallback`
  - Role: Query TMDb with language + year fallback strategy; prefer specific media (`movie`/`tv`) when known; include adult filter.
  - Inputs/Outputs: `(api_key, query, language, include_adult, timeout, media, year)` → list of candidate dicts.
  - Interactions: Used by `normalize_with_metadata` to gather candidates.
- `tmdb_find_by_imdb`
  - Role: Resolve metadata directly from IMDb IDs found in names.
  - Inputs/Outputs: `(api_key, imdb_id)` → movie/tv result dict (annotated with `media_type`).
  - Interactions: Early, high‑confidence path in `normalize_with_metadata`.
- `pick_best_metadata`
  - Role: Score and select best candidate using fuzzy ratios, type hints (TV markers), allowed filename years, and thresholds.
  - Inputs/Outputs: `(results, cleaned_query, year_hint, tv_hint, allowed_years)` → `(best_item, score:int)`.
  - Interactions: Central to metadata disambiguation.
- `normalize_with_metadata`
  - Role: End‑to‑end metadata agent: build query variants, iterate media preferences, choose best hit, fetch/escalate titles (localized/ASCII), pull certification age and genres.
  - Inputs/Outputs: `(cfg, cleaned_title, year_hint, tv_hint, allowed_years)` → `(type:movie|tv|None, title, year, score, tmdb_id, age, genres:list)`.
  - Interactions: Consumed by `classify_and_build_paths` to confirm type, title, year, kids flag.
- Certification helpers: `_tmdb_get_movie_cert_age`, `_tmdb_get_tv_cert_age`, `_extract_age_from_cert_blocks`, `_cert_to_age`
  - Role: Map TMDb release ratings/content ratings to minimum age (ES/US/GB preference, fallback numeric parse).
  - Inputs/Outputs: TMDb IDs/blocks → `age:int|None`.
  - Interactions: Used to compute `is_kids` classification.
- Title localization: `_tmdb_title_in_lang`, `_tmdb_pick_alt_title`
  - Role: Retrieve localized or alternative titles (country preference ES→US), fallback to ASCII when non‑Latin heavy.
  - Inputs/Outputs: `(api_key, media_type, tmdb_id, language|countries)` → title string.
  - Interactions: Post‑process selected metadata title.

**Classification & Decision Making**
- `Dest` (dataclass)
  - Role: Final routing decision for a file: kind, destination folder+file, display title.
  - Inputs/Outputs: Attributes (`kind`, `folder`, `file`, `title`).
  - Interactions: Built by `classify_and_build_paths`; consumed by mover and Plex refresh.
- `ManifestRec` (dataclass)
  - Role: Record of processing for one source path, with decision, destination, optional `title`, `tmdb_id`, `score`, `error`.
  - Inputs/Outputs: Serializable record for `_manifest.json`.
  - Interactions: Collected by `IngestHandler`, consolidated by `write_manifest_and_summary`.
- `classify_and_build_paths(cfg, path)`
  - Role: Core classifier. Pipeline: preprocess → GuessIt parse → infer TV markers → year hints → metadata normalization → kids rules → decide type/title/year → build Plex path or Unclassified fallback.
  - Inputs/Outputs: `(cfg, path:Path)` → `Dest` (movie|movie_kids|tv|tv_kids|unclassified).
  - Interactions: Uses many preprocess/metadata helpers; `compute_unclassified_root`; `sanitize_filename`; kids config; returns `Dest` to handler.
- Kids rules (in `classify_and_build_paths`)
  - Role: Determine kids routing using age threshold, required genres, blacklist tokens.
  - Inputs/Outputs: `(meta_age, meta_genres, title)` + `cfg.kids` → `is_kids:bool`.
  - Interactions: Selects between standard vs *_kids roots and section IDs.

**Moving, Conflicts, Windows Path Safety**
- `move_with_sidecars(src, dst, sidecars, dry_run)`
  - Role: Move main file and matching sidecars to the final destination, creating folders and enforcing Windows path limits.
  - Inputs/Outputs: `(src, dst, sidecars)` → list of moved `Path`s; filesystem side‑effects.
  - Interactions: Used by `IngestHandler` and second‑pass rescuer.
- `_shorten_for_windows`, `_truncate_folder`, `_truncate_filename`, `sanitize_path_component`
  - Role: Ensure destination path/components remain under Windows limits and valid.
  - Inputs/Outputs: Path/strings → safe path.
  - Interactions: Invoked before real moves and when previewing targets.
- Conflict policy (via `renamer.on_conflict`)
  - Role: Resolve existing destination collisions: `skip` | `overwrite` | `suffix` (default adds numeric suffixes).
  - Inputs/Outputs: Target path state → adjusted target path or skip.
  - Interactions: Applied in `_maybe_ingest` before move.
- Unclassified sink: `compute_unclassified_root`
  - Role: Compute a unified `Unclassified` root at the same level as Movies/Series (common parent), or current directory if Plex roots unavailable.
  - Inputs/Outputs: `cfg` → `Path` to Unclassified root.
  - Interactions: Used by classification fallback and error paths.
- Cleanup: `_rmdir_if_empty`, `cleanup_unclassified_roots`
  - Role: Remove empty folders under all Unclassified roots (legacy and unified) after runs.
  - Inputs/Outputs: `cfg` → count of removed directories.
  - Interactions: Invoked by `main` at the end of runs.

**Plex Integration**
- `plex_refresh_by_section(base_url, token, section_id, path=None)`
  - Role: Trigger Plex library refresh for a section, optionally scoping to the moved folder path.
  - Inputs/Outputs: HTTP request side‑effect; returns True if accepted.
  - Interactions: Called by `IngestHandler` after successful move when not in test mode; chooses section based on `Dest.kind` with kids fallbacks.

**Test‑Mode Overrides & Manifest Writing**
- `apply_test_overrides` (see Entrypoints)
  - Role: Ensure only `.txt` placeholders are processed and destinations go under `--test-output-root`.
- `write_manifest_and_summary(handler, cfg, args, logger)`
  - Role: Consolidate handler manifest into `_manifest.json` with de‑duplication by normalized basename: prefer the last state and keep classified over unclassified when both exist.
  - Inputs/Outputs: `(handler.manifest)` → JSON with `total`, `summary`, and `records`.
  - Interactions: Called by `main` after second‑pass rescues in test mode, one‑shot mode, and on shutdown.

**Logging & Error Handling**
- Logger (from `setup_logger`)
  - Role: Central structured logging (file + level). Used throughout.
  - Interactions: All agents log progress, decisions, and exceptions.
- Exception paths
  - Classification failure: falls back to Unclassified, logs once, captures `error` in `ManifestRec`.
  - Move errors: manifest `decision="error_move"`; continues without halting the whole run.
  - Metadata/network errors: logged with stack traces; pipeline degrades gracefully (e.g., unclassified).

**Data Flow Summary**
- Events → `IngestHandler._maybe_ingest` → Guards (ext/stable/lock) → `classify_and_build_paths` (preprocess + metadata) → Conflict resolution → `move_with_sidecars` → Manifest append → `plex_refresh_by_section` (if configured) → On exit: `second_pass_unclassified` → `write_manifest_and_summary` → `cleanup_unclassified_roots`.
