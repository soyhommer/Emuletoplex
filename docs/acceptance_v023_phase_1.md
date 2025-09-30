acceptance = """# EmuleToPlex v_0.23 — Phase 1 Acceptance Criteria

Phase‑1 is accepted only if **all** items below pass on the standard test corpus.

## A. Boundary splitting
- Given: `WEBRip1080p`, `WEB-DL2160p`, `x2641080p`, `h2652160p`
- Then: tokens split into `WEBRip 1080p`, `WEB-DL 2160p`, `x264 1080p`, `h265 2160p`
- And: TV markers `S01E02`, `1x20`, `E05` remain unsplit

## B. Uploader tail trimming with boundary guard
- When a trailing uploader tail exists (e.g., `by Remy`, `- xusman`, `[GRP]`)
- And the tail does **not** contain year or episode markers
- Then the tail is removed
- When the tail contains `\\b(19|20)\\d{2}\\b`, `S\\d{1,2}E\\d{1,3}`, `\\d{1,2}x\\d{1,3}`, `E\\d{1,3}`, `Cap(ítulo)? \\d+`, `Temporada|Season`
- Then **no** trimming occurs

## C. Parenthesis and delimiter pruning
- Parentheses:
  - Keep `(1995)` and similar pure years
  - Keep inner text only if, post‑cleanup, it contains a 3+ alpha sequence
  - Drop numeric‑only or single‑token junk
- Delimiters `; : / -`:
  - Keep segments with a 3+ alpha sequence
  - Or keep segments that pass the leading‑numeral rule (see D)
- Credit/uploader clauses do not survive (keywords, person‑lists, short ALL‑CAPS tails, known uploader list)

## D. Leading numeral titles preserved
- Titles such as `12 Monos` and `2001 Una Odisea del Espacio` remain intact after preprocessing
- Pure numeric tokens that are resolutions (`720`, `1080`, `2160`) do not pass the leading‑numeral rule

## E. Universal normalization order & ordered dedupe
- For all segment paths (both `clean_query_text` and rebuild after `prune_clauses`), the following order is used:
  1) domains → 2) bad tokens → 3) uploader list → 4) word separators → 5) tail noise → 6) whitespace/strip
- Dedupe preserves insertion order; no bare `set` iteration in Phase‑1 paths

## F. Bilingual inner‑parentheses rule
- Only inner parentheses chunks with **≥2 alpha words** survive (after cleanup)
- Single‑word alternates are dropped

## G. Determinism
- Two consecutive runs over the same input, with `phase1_only=true` and `use_known_picks=false`, produce identical cleaned segment outputs and identical manifest entries for Phase‑1 fields
- Any list produced from a set/dict is explicitly sorted

## H. Instrumentation
- Logs contain counters for:
  - credit/uploader clause drops
  - weak‑title guard triggers (informational only in Phase‑1)
- “ALT” or any placeholder token never appears in committed titles

## I. Non‑regression vs v_0.23_p1_1 baseline
- On the standard corpus:
  - Healthy ≥ baseline (no decrease)
  - Unclassified ≤ baseline (no increase)
  - Name‑problem count does **not** increase by more than +5% (tolerance for stricter pruning), and any increase is accompanied by a decrease in Unclassified

## J. Fixture suite
- The provided 30‑file fixture passes with exact expected cleaned outputs (strings compared case‑insensitively)
- All bilingual expectations match the two‑word rule

