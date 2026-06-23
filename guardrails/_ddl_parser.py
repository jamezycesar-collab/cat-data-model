"""
Shared DDL parser for the guardrails suite.

Replaces the brittle `_CREATE_TABLE_RE` regex previously duplicated in
`validate_field_specifications.py` and `validate_cross_dialect_parity.py`.

The old regex used a non-greedy character class `[^;]+?` to capture the
CREATE TABLE body and an alternation `\\s*(?:USING|PARTITIONED|TBLPROPERTIES|
CLUSTER|COMMENT|STORED|;)` as the terminator. Two well-defined failure modes
were observed during the Tier 17 work:

  1. Catastrophic backtracking when the body contained many `(...)` pairs in
     comments (Tier 17.3 multi-leg legs table).
  2. Spurious early termination when an in-body column had a `DECIMAL(38, 18)`
     declaration immediately followed by a per-column `COMMENT 'string'`
     clause - the regex matched the close-paren of the type and treated the
     column-level COMMENT as the table-level terminator (Tier 17.4 Hive
     fact_option_order_events).

This module replaces the regex with an explicit balanced-paren walker that
tracks depth, skips string literals (`'...'`), identifier-quoted names
(`"..."`), and SQL line comments (`-- ...`). It is robust to:
  - Nested parens in column type declarations (DECIMAL(38, 18), VARCHAR(64))
  - Parens inside string literals ('two-tuple (a, b)')
  - Parens inside line comments (`-- legs[] array (row 32.n.X)`)
  - SQL escaped quotes ('it''s a string')
  - Per-column COMMENT 'string' clauses (Hive convention)
  - Inline CHECK constraints with nested predicates
"""
import re
from typing import Iterator


# Just the CREATE TABLE header up to the opening paren. Body is parsed by hand.
_HEADER_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[\w.]*?(\w+)\s*\(",
    re.IGNORECASE,
)

# Recognized table-level keywords that follow the body's close-paren.
# Reported as the trailing keyword for downstream consumers (e.g., parity
# validator uses it to find the PARTITIONED BY clause).
_TRAILING_KW_RE = re.compile(
    r"\s*(USING|PARTITIONED|TBLPROPERTIES|CLUSTER|COMMENT|STORED|;|$)",
    re.IGNORECASE,
)


def find_create_tables(text: str) -> Iterator[tuple[str, str, int, int, str]]:
    """Yield (table_name, body, body_start, body_end_after_close, trailing_kw)
    tuples for each CREATE TABLE statement in `text`.

    - `table_name` is the captured identifier (lowercase preserved as-is in
      the source; callers typically lowercase it).
    - `body` is everything between the outermost `(` and matching `)` of the
      column list.
    - `body_start` is the character index of the first character INSIDE the
      body (i.e., right after the opening `(`).
    - `body_end_after_close` is the character index right after the matching
      `)` (i.e., where the trailing keyword scan begins).
    - `trailing_kw` is the next recognized table-level keyword (USING /
      PARTITIONED / etc.) or empty if none found.
    """
    pos = 0
    n = len(text)
    while True:
        m = _HEADER_RE.search(text, pos)
        if not m:
            return
        table = m.group(1)
        body_start = m.end()
        depth = 1
        i = body_start
        while i < n and depth > 0:
            c = text[i]
            # SQL string literal
            if c == "'":
                j = i + 1
                while j < n:
                    if text[j] == "'":
                        # SQL escaped quote: ''  -> stay inside literal
                        if j + 1 < n and text[j + 1] == "'":
                            j += 2
                            continue
                        break
                    j += 1
                if j >= n:
                    # unterminated string - bail out of this table
                    pos = body_start
                    break
                i = j + 1
                continue
            # Identifier quoting (Hive backticks, ANSI double quotes)
            if c == '"':
                j = text.find('"', i + 1)
                if j == -1:
                    pos = body_start
                    break
                i = j + 1
                continue
            if c == "`":
                j = text.find("`", i + 1)
                if j == -1:
                    pos = body_start
                    break
                i = j + 1
                continue
            # SQL line comment: -- ... \n
            if c == "-" and i + 1 < n and text[i + 1] == "-":
                j = text.find("\n", i)
                if j == -1:
                    j = n
                i = j + 1
                continue
            # Block comment: /* ... */
            if c == "/" and i + 1 < n and text[i + 1] == "*":
                j = text.find("*/", i + 2)
                if j == -1:
                    pos = body_start
                    break
                i = j + 2
                continue
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
                if depth == 0:
                    body = text[body_start:i]
                    end = i + 1
                    # Find trailing keyword for downstream consumers
                    km = _TRAILING_KW_RE.match(text, end)
                    trailing_kw = km.group(1).upper() if km and km.group(1) else ""
                    yield (table, body, body_start, end, trailing_kw)
                    pos = end
                    break
            i += 1
        else:
            # Loop ended without depth=0 - unterminated CREATE TABLE.
            # Move past the opening paren so we don't loop forever.
            pos = body_start


# Convenience: collect into a list (rarely needed but handy for testing).
def list_create_tables(text: str) -> list[tuple[str, str, int, int, str]]:
    return list(find_create_tables(text))
