# Acceptance Criteria v0.23

Target branch: `v0.23-hardening`
Files in scope: `emuletoplex_runner.py`, `config.example.yaml`
Out of scope: CLI/UX changes, provider swaps, per-title aliasing

---

## Phase 1 — Preprocessing & Query Hygiene

### 1.1 Boundary splitting (pre-scan)

Applied **before** release/lang/uploader stripping, in the order shown.

Replacements (case-insensitive):

```
(?i)(web|bd|hd|hdr|microhd)(\d{3,4}p)      → "$1 $2"
(?i)(\d{3,4}p)(x\d{3,4})                    → "$1 $2"
(?i)(dual)(audio)                           → "$1 $2"
(?i)(multi)(audio)                          → "$1 $2"
(?i)(vo)(se)                                → "$1 $2"
(?i)(vo)(esp)                               → "$1 $2"
(?i)(esp)(eng)                              → "$1 $2"
```

**Acceptance checks**

* [ ] The pre-scan runs once, before other cleaning passes.
* [ ] Tokens like `WEBRip1080p`, `720pX264`, `dualAudio`, `vose` are split into space-separated tokens without altering semantic words.

### 1.2 Uploader tail trimming

* Split at the **first** occurrence of:

```
(?i)\s+(?:by|por|per|para)\s+.+$
```

* Remove trailing domains using a generic domain regex `DOMAINS_ANY_RE` (TLD-agnostic).
* Must **not** trim past a year token `(19|20)\d{2}` or an episode token `S\d{1,2}E\d{1,3}`.

**Acceptance checks**

* [ ] Uploader tails introduced by `by|por|per|para` are removed only when safe.
* [ ] Years/episode markers to the right are preserved; trimming aborts if it would remove them.

### 1.3 Parenthesis / hyphen pruning

Rules when forming candidate segments:

* **Parentheses:** drop any inner clause that is **not a year** and has **no** 3+ alpha-char sequence.

  * Implementation note: if a parenthetical chunk lacks `(?:[A-Za-z]{3,})` and is not a year `(19|20)\d{2}`, remove it.
* **Hyphen/semicolon/colon/slash splits:** when splitting with `re.split(r"\s*[-–;:/]\s*")`, ignore segments that do **not** contain at least one `[A-Za-z]{3,}`.

**Acceptance checks**

* [ ] Empty or numeric-only parenthesis clauses are removed.
* [ ] Segments lacking any 3+ alpha sequence are excluded from candidates.

### 1.4 Bilingual chunks rule

* From **inner parentheses**, only keep chunks with **≥ 2** alpha words:

```
[A-Za-z]{3,}\s+[A-Za-z]{3,}+
```

* Keep them only if they **do not** contain release/lang/uploader tokens **after** cleaning.

**Acceptance checks**

* [ ] Single-word alternates or token noise inside parentheses are dropped.
* [ ] Kept alternates pass the token-noise filter.

### 1.5 “No 3+ alpha-chars ⇒ drop”

* Any candidate segment (core/parenthesis/hyphen) with **zero** matches of `[A-Za-z]{3,}` is dropped **before** guess/metadata.

**Acceptance checks**

* [ ] Such segments never reach the TMDb query builder.

---

## Phase 2 — TMDb Strategy & Scoring

### 2.1 Thresholds

* Base threshold: `metadata.fuzzy_threshold` (default **80**).
* Single-word titles (or total title length `< 4`) require score **≥ threshold + 10**.

**Acceptance checks**

* [ ] Single-word titles below the higher bar are rejected.

### 2.2 Adult retry (one-shot)

* Perform **exactly one** global retry round with `include_adult=true` **only if** the first (non-adult) round yields **no candidate with score ≥ threshold**.
* Do **not** toggle adult per media or per query.

**Acceptance checks**

* [ ] Adult retry occurs at most once per item.
* [ ] Adult retry is skipped when first round already found a ≥ threshold candidate.

### 2.3 Year deviation guard & hard rejects

* If `allowed_years` (from filename tokens) is set and `cand_year ∉ allowed_years` → **reject** candidate.
* Else if `year_hint` present and `|cand_year − year_hint| > 1` **and** base fuzzy `< 90` → **reject** candidate.
* If cleaned candidate titles still contain release/lang/uploader tokens → **reject** (or apply penalty ≤ **−15** guaranteeing it falls below accept threshold).
* Minimum base fuzzy to even consider a candidate: **≥ 60**.

**Acceptance checks**

* [ ] Candidates outside allowed years are rejected.
* [ ] “>1 year off + fuzzy < 90” are rejected for generic titles.
* [ ] Residual token noise is penalized/rejected before selection.
* [ ] No sub-60 fuzzy candidate is considered.

---

## Phase 3 — Second Pass (Rescue)

### 3.1 Candidate sources (max 5; execute at most 3)

Use only the following:

1. Core cleaned title **with** year.
2. Core cleaned title **without** year.
3. **Near-year right segment:** if name contains `... - YYYY - Title ...`, choose the segment **immediately right** of the year; keep only if it has **≥ 2** alpha words.
4. **Best Latin multi-word phrase:** longest chunk with **≥ 2** alpha words in the cleaned base name.
5. **Romanized title:** only if `is_mostly_non_latin(raw_name)` is true; romanize and keep only if it has **≥ 2** alpha words.

**Dedupe**

* Dedupe by `clean_query_text(q).lower()` before executing.
* Cap to **5** unique candidates; execute only the **first 3**.

**Adult retry discipline**

* Same as main pass: **one** adult retry round if first pass yields no candidate ≥ threshold.

**Call cap**

* Respect `metadata.tmdb_call_limit` **per file** across **both** passes.

**Acceptance checks**

* [ ] At most 3 queries are executed in second pass.
* [ ] Total TMDb calls per item does not exceed the configured per-file cap.

---

## Phase 4 — Observability & Metrics

### 4.1 INFO logs per file (once per item)

Emit a single line (or JSON) with:

```
TMDb_calls=NN; adult_retry_used=true|false; query_attempts=NN;
best_score=NN; matched_media=movie|tv|none; used_cache=true|false
```

### 4.2 Manifest metrics (root `"metrics"` block)

Add/update integer counters:

* `name_problems`
* `wrong_year`
* `wrong_type`
* `unclassified` remains reported in summary (not duplicated here).

### 4.3 Config flags (in `config.example.yaml`)

* `logging.metrics_enabled: bool` (default **true**) → enables INFO metrics per file and manifest metrics block.
* `metadata.tmdb_call_limit: int` (default **40**).
* `metadata.use_known_picks: bool` (default **true**) → allows disabling learned cache if desired.
* `logging.autosave_every: int` (default **50**) → autosaves `_manifest.autosave.json` every **N** items.

**Acceptance checks**

* [ ] With `logging.metrics_enabled=true`, per-file INFO logs and manifest `"metrics"` block are present.
* [ ] Autosave happens at the configured cadence.

---

## Phase 5 — Validation (20-item batch vs v0.22)

### 5.1 Test batch

* Prepare 20 placeholder files representative of noisy patterns (language blends, compact tokens, hyphen/parenthesis chains, short generic titles).

### 5.2 Command

```
.\.venv\Scripts\python.exe emuletoplex_runner.py ^
  --test-mode ^
  --test-source .\test_placeholders_20 ^
  --test-output-root .\test_output_v023 ^
  --once
```

### 5.3 Acceptance comparisons (v0.23 vs v0.22 on same 20)

* **Unclassified:** `≤ v0.22` (must not increase)
* **name\_problems:** `≤ v0.22` (must not increase)
* **wrong\_year:** `≤ v0.22` (must not increase)
* **Errors:** zero occurrences of

  * `"normalize_with_metadata: unexpected error"`
  * `UnboundLocalError`
  * any `Traceback` in log
* **TMDb\_calls\_per\_file:** `≤ metadata.tmdb_call_limit` for **every** item; **average** calls per file `≤ v0.22` average
* **adult\_retry\_used:** present only when first round yielded **no** candidates `≥ threshold`

---

## Minimal API / Struct Diff (signatures only; no code)

### Preprocessing

```python
def split_compact_tokens(s: str) -> str
def strip_uploader_tail(s: str) -> str  # supports by|por|per|para with guards
def clean_query_text(q: str) -> str     # delimiter cleanup + 3+ alpha rule
```

### TMDb Strategy

```python
def normalize_with_metadata(
    cfg: Dict[str, Any],
    cleaned_title: str,
    year_hint: Optional[int],
    tv_hint: bool,
    allowed_years: Optional[Set[int]]
) -> Tuple[Optional[str], Optional[str], Optional[int], int, Optional[int], Optional[int], Optional[List[str]]]
# Behavior: two rounds (adult off/on), capped by metadata.tmdb_call_limit, structural thresholds applied.


def pick_best_metadata(
    results: List[Dict[str, Any]],
    cleaned_query: str,
    year_hint: Optional[int],
    tv_hint: bool,
    allowed_years: Optional[Set[int]]
) -> Tuple[Optional[Dict[str, Any]], int]
# Behavior: hard rejects for year deviation and residual tokens; higher bar for single-word titles.
```

### Second Pass

```python
class IngestHandler:
    def second_pass_unclassified(self) -> None
# Behavior: generates up to 5 candidates (executes 3), with dedupe; one-shot adult retry; respects per-file call cap.
```

### Observability

```python
def write_manifest_and_summary(
    handler: IngestHandler,
    cfg: Dict[str, Any],
    args: argparse.Namespace,
    logger: logging.Logger
) -> None
# Adds "metrics" block (name_problems, wrong_year, wrong_type) when logging.metrics_enabled is True.
```

### Config keys (read by cfg\_get)

```
logging.metrics_enabled: bool
metadata.tmdb_call_limit: int
metadata.use_known_picks: bool
logging.autosave_every: int
```

### Helpers (existing)

```
is_mostly_non_latin(s: str, threshold: float = 0.6) -> bool
extract_filename_years(text: str) -> List[int]
```

---

## Notes

* Criteria are **general-rule based**: no special-casing of particular titles or filenames.
* Focus is on robust string hygiene, disciplined query strategy, and measurable metrics.
