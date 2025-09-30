# EmuleToPlex v_0.23 — Phase 1 (Preprocessing & Query Hygiene) — Step‑by‑Step Routemap

## Purpose
Stabilize filename → query text preprocessing so that outputs are deterministic, clean, and Phase‑2/3 ready. This phase **does not** change metadata selection policy; it ensures we always feed clean, comparable queries.

## Scope
- Boundary splitting of compact tokens (codec/resolution/release merges)
- Safe uploader‑tail trimming guarded by year/episode markers
- Parenthesis and delimiter clause pruning
- Bilingual inner‑parentheses handling
- Universal normalization order and ordered dedupe
- Determinism hygiene (ordering, seeds)
- Instrumentation and test fixtures

## Non‑Goals
- Metadata scoring thresholds or tie‑breakers (Phase‑2)
- Kids/genre routing (Phase‑4)
- LLM verification (Phase‑3)

---

## Milestone 0 — Baseline lock (1 day)
- Add config flags:
  - `phase1_only=true` (skip second‑pass rescues while testing)
  - `use_known_picks=false` during Phase‑1 tests
- Sort all file iterations: `sorted(root.rglob("*.txt"), key=str.casefold)`
- Export deterministic seed in logs: `PYTHONHASHSEED=0` (optional but recommended)

**Exit:** two consecutive dry runs over the same input yield identical *preprocessing* outputs.

---

## Milestone 1 — Boundary splitting (1 day)
**Implement**
- Extend `COMPACT_TOKEN_REPLACEMENTS`:
  - `(x264|x265|h.?264|h.?265)(\\s*\\d{3,4}p) → \\1 \\2`
  - `(webrip|web-?dl|b[dr]rip|hdrip|dvdrip)(\\d{3,4}p) → \\1 \\2`
  - Normalize `4K/UHD/HDR10+/DoVi` into a stable token (“4k” plus HDR markers as separate tokens)
- Keep exemptions for `S\\d\\dE\\d\\d`, `\\d+x\\d+`, `E\\d+`

**Exit:** test cases show compact merges are split while TV markers remain intact.

---

## Milestone 2 — Uploader tail trimming with boundary guard (1 day)
**Implement**
- `UPLOADER_TAIL_RE` trims trailing `by|por|per|para <handle>` and common group suffixes
- `UPLOADER_BOUNDARY_RE` prevents trimming when the tail contains:
  - year `\\b(19|20)\\d{2}\\b`
  - `S\\d{1,2}E\\d{1,3}`, `\\d{1,2}x\\d{1,3}`, `E\\d{1,3}`
  - `Cap(ítulo)? \\d+`, `Temporada|Season`
- Call `strip_uploader_tail` right after `split_compact_tokens`

**Exit:** uploader tags removed; no valid year/episode contexts are cut.

---

## Milestone 3 — Parenthesis and delimiter pruning (2 days)
**Implement**
- Parentheses:
  - Keep pure year `(19|20)\\d{2}`
  - Keep text with a 3+ alpha sequence after cleanup
  - Drop numeric‑only or single‑token junk
- Delimiters `; : / -`:
  - Build segments only if they have a 3+ alpha sequence or pass the leading‑numeral rule (Milestone 5)
- Early drop credit/uploader clauses:
  - credit keywords (with/starring/dirigido/by/feat…)
  - person‑like name lists (comma / “ y ” / “ & ” / “ / ”)
  - short ALL‑CAPS 2–5 char tails (not years or episode markers)
  - match against `UPLOADER_LIST_RE` if present

**Exit:** no credit/uploader fragments survive; parentheses behave; hyphen trails don’t win as titles.

---

## Milestone 4 — Bilingual inner‑parentheses (0.5 day)
**Implement**
- From inner parentheses, keep only chunks that match `\\b[A-Za-z]{3,}\\s+[A-Za-z]{3,}\\b` **after** cleanup
- Apply domain/release/uploader cleanup before the 2‑word test

**Exit:** bilingual alternates retained; single‑word soup dropped.

---

## Milestone 5 — Universal normalization order + leading numerals (1.5 days)
**Implement**
- Universal order for every segment path (both `clean_query_text` and `preprocess_name` rebuilds):
  1) `DOMAINS_ANY_RE`
  2) `BAD_TOKENS_RE`
  3) `UPLOADER_LIST_RE` (if configured)
  4) `WORD_SEPS_RE` then collapse whitespace
  5) `_strip_tail_noise`
  6) squeeze/strip stray `-_. ,`
- Segment gate:
  - keep if `3+ alpha` present
  - else keep only if `_allow_leading_numeral(seg)` is true and the next token is alphabetic (saves “12 Monos”, “2001 …”)
- Ordered dedupe (preserve insertion order; never iterate a bare `set`)

**Exit:** leading‑numeral titles preserved; token noise removed consistently.

---

## Milestone 6 — Determinism hygiene (0.5 day)
- Sort any list produced from a set or dict keys (`key=str.casefold`)
- Ensure the same cleaned segments are produced for identical inputs across repeated runs

**Exit:** hash of cleaned segments is identical in back‑to‑back runs.

---

## Milestone 7 — Instrumentation & fixtures (0.5 day)
- Log whenever the weak‑title guard would demote to Unclassified (Phase‑2 will use this)
- Log when uploader/credit drops are applied
- Add a small fixture suite of 30 representative filenames with expected cleaned outputs and bilingual picks

**Exit:** CI check passes the fixture suite; logs show guard counters and drops as expected.

---

## Milestone 8 — Non‑regression gate (0.5 day)
- Compare against v_0.23_p1_1 baseline summary:
  - Healthy count must be **≥ baseline** (or unchanged) for the same corpus
  - Unclassified must not increase
  - No “ALT” or placeholder tokens in final titles (should be impossible in Phase‑1)

**Exit:** baseline metrics preserved or improved; Phase‑1 complete.