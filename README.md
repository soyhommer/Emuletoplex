# EmuleToPlex (Windows)

A Windows-friendly watcher that processes your eMule *Incoming* folders (or placeholder tests), classifies media as Movies or TV, sanitizes names, moves finished downloads into Plex-ready folders, and optionally hits Plex to refresh the library.

## What It Does
- Watches every configured path in `emule.watch_paths`, applying stability and lock checks before touching files.
- Runs a preprocessing pipeline to strip domains, release tags, aspect ratios, numeric prefixes, and other junk before parsing.
- Uses GuessIt plus TMDb metadata (fuzzy score, year hints, alternative titles) to decide between movie vs TV and choose the right title/year.
- Applies Unicode sanitisation with configurable keep/drop/transliterate strategies for Plex/Windows-safe filenames.
- Splits kids-friendly content into dedicated roots based on TMDb age ratings and genres.
- Writes a `_manifest.json` summary (counts + final decisions) and reruns a second-pass rescuer over `Unclassified` entries.
- Moves sidecar files together, shortens long destinations for Windows limits, and triggers Plex refreshes per section when configured.
- Provides a test mode that only ingests `.txt` placeholders, redirects destinations under `--test-output-root`, and skips Plex APIs.

## Requirements
- Windows 10/11 with Python 3.11+.
- Virtual environment with `pip install -r requirements.txt`.
- TMDb API key when `metadata.enabled: true`.
- Plex token and section IDs for automatic refresh calls.

## Installation (fresh clone)
```powershell
cd C:\EmuleToPlex
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
cp config.example.yaml config.yaml
# Edit config.yaml to match your folders, Plex token, kids rules, etc.
```

## Configuration
Configuration is read from `config.yaml` (falls back to `config.example.yaml`). Key sections:
- `emule`: `watch_paths`, `stable_seconds`, `allowed_extensions`, `sidecar_extensions`.
- `plex`: `base_url`, `token`, section IDs (`movies_section_id`, `shows_section_id`, `*_kids_section_id`) and root paths (`movies_root`, `shows_root`, `*_kids_root`).
- `metadata`: toggle TMDb normalization, API key, preferred language, fuzzy threshold, timeouts, adult filter.
- `renamer`: conflict policy (`on_conflict`), release-tag cleanup, and non-latin handling (`non_latin_strategy`, `keep_chars`).
- `folders`: `unclassified_root` name (created near your Plex roots).
- `kids`: age ceiling, required genres, blacklist tokens to route family-friendly titles.
- `logging`: log file path, level, rotation config.

> Test mode automatically overrides some of these (disables Plex token/base_url, allows `.txt`, redirects roots, drops stability wait).

## Command-line usage
```
python emuletoplex_runner.py [--test-mode] [--test-source PATH]
                             [--test-output-root PATH] [--once] [--dry-run]
```

### Flags
- `--test-mode` – Expect `.txt` placeholders only, disable Plex refresh, redirect Plex roots, skip stability/lock waits.
- `--test-source PATH` – Directory with `.txt` placeholder files to ingest (required when `--test-mode` is on).
- `--test-output-root PATH` – Base directory where simulated `Peliculas/Series` trees are created during test mode (default `./test_output`).
- `--once` – Process the current backlog, run the second-pass rescuer, write `_manifest.json`, exit.
- `--dry-run` – Log actions without creating/moving files.

## Runtime flow
- Files entering via watchdog events go through `_maybe_ingest`, which enforces extension, stability, and lock checks.
- `classify_and_build_paths` cleans up the raw name, parses with GuessIt, queries TMDb (respecting year hints and TV markers), decides kids/non-kids, then builds the destination path.
- Conflicts follow `renamer.on_conflict` (`skip`, `overwrite`, default `suffix`).
- Moves include matching sidecars; destinations are sanitised and truncated safely for Windows.
- Every decision is logged and appended to an in-memory manifest; on exit a `_manifest.json` summary (with per-decision counts) is written next to the roots.
- After each run the script performs a second-pass rescue of `Unclassified` placeholders using extra title candidates, then deletes empty Unclassified folders.
- Plex refreshes are triggered per section ID (kids fall back to main section if dedicated IDs are absent) when `base_url` and `token` are set and `--test-mode` is off.

## Test mode workflow
```powershell
.\.venv\Scripts\python.exe .\make_placeholders.py --source "D:\eMule\Incoming" --out .\test_placeholders
.\.venv\Scripts\python.exe emuletoplex_runner.py --test-mode --test-source .\test_placeholders --test-output-root .\test_output --once
```
- Output structure: `.\test_output\Peliculas`, `.\test_output\Series`, and `_manifest.json` one level above.
- Real media files are ignored in test mode; only `.txt` placeholders are processed.
- Plex calls are skipped but logs and manifests match the real workflow.

## Running continuously
For interactive sessions you can run:
```powershell
.\.venv\Scripts\python.exe emuletoplex_runner.py
```
The watcher stays active until interrupted and performs manifest writing and cleanup on exit.

## Install as a Windows Service (NSSM)
```powershell
nssm install EmuleToPlex "C:\EmuleToPlex\.venv\Scripts\python.exe" "C:\EmuleToPlex\emuletoplex_runner.py"
nssm set EmuleToPlex AppDirectory "C:\EmuleToPlex"
nssm set EmuleToPlex Start SERVICE_AUTO_START
nssm set EmuleToPlex AppStopMethodConsole 15000
nssm start EmuleToPlex
```

### Alternative (pywin32 service)
```powershell
.\.venv\Scripts\python.exe emuletoplex_service.py install
.\.venv\Scripts\python.exe emuletoplex_service.py start
```
Adjust paths inside `emuletoplex_service.py` before installing.

## Plex section IDs
Visit `http://127.0.0.1:32400/library/sections?X-Plex-Token=YOURTOKEN` and copy the `key` attribute for each library (movies, shows, kids).

## Folder layout
- Movies: `Peliculas/Title (Year)/Title (Year).ext`
- Kids movies: `Peliculas_Infantiles/...` (auto-created when kids root is configured or inferred).
- TV: `Series/Show Name/Season 01/Show Name - S01E02.ext`
- Kids TV: `Series_Infantiles/...`
- Unclassified: `<common parent>/Unclassified/<Sanitized Name>/<Sanitized Name>.ext`

## Logs & manifest
- Logs rotate at `logging.log_file` (default `emuletoplex.log`).
- `_manifest.json` lists totals, per-decision counts, and each record (`src`, `dest`, `title`, `tmdb_id`, `score`, `error`).
- Second-pass rescues update manifest entries so the last outcome per source wins.

## Notes
- If you do archive extraction, run it before files land in the watched folder.
- Tune `stable_seconds` for large downloads; combine with eMule's "commit completed file" option to avoid moves mid-download.
- Run with `--dry-run` before rolling changes into production.
- When metadata fails to match confidently, items land in `Unclassified` for manual review or second-pass rescue.

## Uninstall (NSSM service)
```powershell
nssm stop EmuleToPlex
nssm remove EmuleToPlex confirm
```

## License
MIT. You break it, you own the pieces.
