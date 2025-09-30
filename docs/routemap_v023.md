# EmuleToPlex v0.23 — Hardening Routemap

**Source:** Codex analysis of v0.12 → v0.22  
**Target branch:** `v0.23-hardening`  
**Scope:** `emuletoplex_runner.py` (logic), `config.example.yaml` (new flags), optional `docs/acceptance_v023.md` (criteria)

---

## 1) Context & Trend Summary (from Codex)
- **Unclassified:** best at v0.15–v0.16 (10–12), ~14 at v0.18–v0.21, **16 at v0.22**.
- **Name problems:** peaked 28 (v0.13), since v0.16 stays 11–15, **15 at v0.22**.
- **Wrong year:** high 49 (v0.14–v0.15), since v0.16 ≈9–10. Good.
- **Wrong type:** minimal (0–1).
- **Log errors:** v0.19 classification errors; v0.20 `UnboundLocalError` in metadata loop; v0.21–v0.22 clean. TMDb call limit fine post-fix.
- **Recurring offenders:** release/codec/bit-depth tokens; language blends; uploader suffixes; hyphen/parenthesis chains; bilingual titles; compact tokens (e.g., `WEBRip1080p`); “by/por/per/para …”.
- **Wrong year pattern:** homonyms and generic titles; franchises; biodramas.
- **Type drift:** “Salo …” → need stricter TV guards based on explicit TV markers.

---

## 2) Objectives (v0.23)
1. **Cut Unclassified and Name problems** without title-specific hacks.
2. **Stabilize year selection** for generic/homonym titles via structural guards.
3. **Discipline TMDb querying**: fewer, stronger queries; one adult-retry round.
4. **Improve observability**: per-run counters and light autosave metrics.
5. **Keep behavior generalizable**: no per-title alias maps.

**Non-goals:** UI/CLI overhaul, deep caching strategy, vendor switch, or changing existing CLI options.

---

## 3) High-Level Strategy
- Stronger **text hygiene** before any query.
- **Priority-driven** query list (max 4–6 after dedupe).
- **Structural thresholds**: single-word titles require higher confidence.
- **Year/Type guards**: hard constraints before fuzzy scoring.
- **Metrics** to detect regressions quickly.

---

## 4) Phased Execution Plan

### Phase 1 — Preprocessing & Query Hygiene
Implement an early pre-scan and stricter pruning, then feed the rest of the pipeline.

#### 1.1 Boundary splitting (compact tokens)
Apply once before release/lang/uploader cleanup.
- Split letter↔digit and digit↔letter:  
  `r"([A-Za-z])(\d)" → "\1 \2"`, `r"(\d)([A-Za-z])" → "\1 \2"`
- Split common media transitions (non-exhaustive; extensible list):  
  `r"(?i)(x264|x265|h\.?264|h\.?265)(\s*\d{3,4}p)" → "\1 \2"`  
  `r"(?i)(webrip|web\-dl|brrip|hdrip|dvdrip)(\d{3,4}p)" → "\1 \2"`  
  `r"(?i)(dual(?:audio)?|multi(?:audio)?|vose|vost|espeng)([^\s])" → "\1 \2"`

#### 1.2 Multi-language uploader tail detection
Trim trailing uploader phrases without relying on a fixed name list.
- Pattern (case-insensitive): preposition + token run at end, bounded so it **stops before years/episode markers**.  
  Example guard regex (concept):  
  `r"(?i)\b(?:by|por|per|para)\b[ \-_\.]+(?!.*(?:\b(19|20)\d{2}\b|S\d{1,2}E\d{1,3}\b)).{1,60}$"`
- Implement as a right-trim pass; do not trim if it would remove an episode or year segment.

#### 1.3 Parenthesis / hyphen clause pruning
For candidate-title formation:
- Drop clauses that **do not contain** `[A-Za-z]{3,}` after cleanup.
- Collapse duplicate separators: `[,;|\-_/]{2,} → single`.
- **Bilingual rule:** within parentheses, **keep only** chunks with **≥2 alphabetic words**; drop the rest.

### Phase 2 — TMDb Query Strategy & Scoring
- Build a **narrow, ordered** query list (deduped, cap 4–6):
  1) Core cleaned title + nearest filename year  
  2) Core cleaned title (no year)  
  3) **Near-year segment** (title fragment adjacent to the year)  
  4) **Best Latin multi-word phrase** from the title
- **Adult fallback:** a single global retry with `include_adult=True` **only if** no candidate ≥ threshold or zero results overall.
- **Structural thresholds:**
  - Base fuzzy threshold = config value (default 80).
  - If candidate title is **single word** or length < 4 chars, require **base +10**.
- **Strong filtering before accept:**
  - If candidate year deviates by **>1** from any filename year **and** base fuzzy < **90**, reject (for generic/single-word titles).
  - If candidate title still contains release/lang/uploader tokens post-normalization, apply a **hard penalty (−15)** or reject.

### Phase 3 — Second Pass (Rescue)
- Generate **fewer, stronger** candidates only:
  - Best Latin multi-word phrase
  - Near-year right segment
  - Romanized title **only if** mostly non-Latin in filename
- Reuse Phase-2 discipline: same caps, same adult-retry rules.  
- Keep total TMDb calls per item under existing per-item limit.

### Phase 4 — Observability & Guardrails
- Counters (per run):
  - `unclassified`, `wrong_year`, `wrong_type`, `name_problems`
  - `tmdb_calls_per_file` (min/avg/max)
  - `adult_retries` (count)
  - `query_buckets` counts: `core_with_year`, `core_no_year`, `near_year`, `multi_word`
- **Autosave snapshot** with timestamp and run counter.
- Config flags (safe defaults below):
  - `metrics.enabled` (bool, default `false`)
  - `metrics.level` (`basic` | `full`, default `basic`)
  - `metadata.api.max_calls_per_run` (int, default `0` = unlimited)
  - `metadata.log_scores` (bool, default `false`) → log queries/scores **only** when `test_mode=true` or this flag true.

### Phase 5 — Validation & Tuning
- Controlled set (N≈20) representative of failure modes.
- For each run, capture:
  - `unclassified`, `name_problems`, `wrong_year`, `wrong_type`
  - avg TMDb calls per item; elapsed time per item
- Compare v0.23 vs v0.22; adjust regex list and thresholds only (no per-title aliases).

---

## 5) Acceptance Criteria (copy to `docs/acceptance_v023.md`)
1. **Boundary splitting** converts compact tokens (`WEBRip1080p`, `x2641080p`, `voseLAT`) into spaced tokens **without** altering semantic words.  
2. **Uploader tail** starting with `by|por|per|para` is trimmed only when no year or episode token would be removed.  
3. **Clause pruning** drops parenthetical/hyphen fragments lacking any 3+ letter sequence; parentheses contents with <2 alpha words are removed.  
4. **Query list** per item is **≤6** after dedupe and in the specified priority order.  
5. **Adult retry** is executed **at most once per item** and only if previous round produced zero or < threshold candidates.  
6. **Single-word titles** require `threshold + 10` to accept; otherwise rejected.  
7. **Year guard:** if filename contains year(s), candidates deviating by >1 year with base fuzzy <90 are rejected.  
8. **Penalty** (−15) applied when a candidate still contains release/lang/uploader tokens after normalization; such candidates are not selected over clean ones of equal score.  
9. **TV switch** occurs only with explicit TV markers in filename **or** if metadata confidence ≥ `threshold + 10` and no strong movie hints.  
10. **Metrics** are emitted when `metrics.enabled=true` or `test_mode=true` and include the counters listed in Phase 4.  
11. **Autosave snapshot** includes timestamp and run counter.  
12. Running the 20-item validation set shows **improvement vs v0.22** in at least 2/3 of: `unclassified`, `name_problems`, `wrong_year`, with **no increase** in `wrong_type`.

---

## 6) Minimal API/Structure Diff (sketch, not code)

_New helpers inside `emuletoplex_runner.py`:_
- `def split_compact_tokens(s: str) -> str: ...`
- `def strip_uploader_tail(s: str) -> str: ...`
- `def prune_clauses(s: str) -> str: ...`  *(includes bilingual rule)*
- `def build_tmdb_queries(cleaned: str, years: set[int], hints: dict) -> list[dict]: ...`
- `def is_single_word_title(title: str) -> bool: ...`
- `def has_tv_markers(text: str) -> bool: ...`
- `def mostly_non_latin(text: str) -> bool: ...`

_Expanded return (already in use):_  
`(type, title, year, score, tmdb_id, age, genres)`

_Config (`config.example.yaml`) additions:_
```yaml
metadata:
  fuzzy_threshold: 80
  api:
    max_calls_per_item: 8         # existing, keep
    max_calls_per_run: 0          # 0 = unlimited
  log_scores: false               # new: emit query/score lines when true or test_mode

metrics:
  enabled: false                  # off by default
  level: basic                    # basic|full
