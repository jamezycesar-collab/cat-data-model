"""
Regression tests for the balanced-paren CREATE TABLE parser.

These cover the failure modes observed during Tier 17 development that
motivated the rewrite from the old `_CREATE_TABLE_RE` regex:

  - Tier 17.3 multi-leg legs: parens inside `--` line comments triggered
    catastrophic backtracking in the old `[^;]+?` regex.
  - Tier 17.4 Hive fact_option_order_events: per-column `COMMENT 'string'`
    after a `DECIMAL(38, 18))` close-paren caused the old regex to treat
    the column-level COMMENT as the table-level terminator.

Run: python3 guardrails/test__ddl_parser.py
"""
import sys
from pathlib import Path

# Make the sibling module importable when run directly.
sys.path.insert(0, str(Path(__file__).parent))
from _ddl_parser import find_create_tables  # noqa: E402


def _names(text):
    return [t[0] for t in find_create_tables(text)]


def _cols(body):
    """Best-effort column-name extraction from a body string."""
    out = []
    for line in body.splitlines():
        s = line.strip().rstrip(",")
        if not s or s.startswith("--"):
            continue
        if s.upper().startswith(("CONSTRAINT", "PRIMARY", "FOREIGN")):
            continue
        first = s.split()[0] if s.split() else ""
        if first and first[0].isalpha():
            out.append(first.lower())
    return out


def test_per_column_comment_string():
    """Tier 17.4 regression: per-column COMMENT 'string' after DECIMAL(...)."""
    text = """CREATE TABLE IF NOT EXISTS gold.t (
        col_a STRING COMMENT 'first column',
        col_b DECIMAL(38, 18) COMMENT 'col with parens in (foo, bar)',
        col_c TIMESTAMP
    )
    PARTITIONED BY (event_date DATE)
    STORED AS PARQUET;"""
    assert _names(text) == ["t"]
    _, body, _, _, kw = next(find_create_tables(text))
    cols = _cols(body)
    assert cols == ["col_a", "col_b", "col_c"], cols
    assert kw == "PARTITIONED", kw


def test_parens_in_line_comments():
    """Tier 17.3 regression: -- comments with (parens) caused regex
    catastrophic backtracking with the previous [^;]+? body."""
    text = """CREATE TABLE IF NOT EXISTS gold.legs (
        leg_seq INT NOT NULL, -- the n index in legs[] array (row 32.n.X)
        leg_oc STRING, -- (O/C/NULL for stock)
        leg_side STRING NOT NULL,
        CONSTRAINT chk_side CHECK (leg_side IN ('BUY','SELL'))
    )
    USING DELTA;"""
    assert _names(text) == ["legs"]
    _, body, _, _, kw = next(find_create_tables(text))
    cols = _cols(body)
    assert cols == ["leg_seq", "leg_oc", "leg_side"], cols
    assert kw == "USING", kw


def test_multiple_tables_in_one_file():
    """Tier 17.5 regression: multi-table files must iterate cleanly."""
    text = """CREATE TABLE a (x INT) USING DELTA;
    CREATE TABLE IF NOT EXISTS b (y INT, z STRING COMMENT 'note') STORED AS PARQUET;
    CREATE TABLE c (w DECIMAL(10, 4)) USING DELTA;"""
    names = _names(text)
    assert names == ["a", "b", "c"], names


def test_nested_check_with_quoted_strings():
    """Nested CHECK with multiple ()s and 'string' literals."""
    text = """CREATE TABLE foo (
        a INT,
        b INT,
        CONSTRAINT chk CHECK (a IN ('x', 'y') OR (b > 0 AND b < 100))
    );"""
    assert _names(text) == ["foo"]


def test_escaped_quotes_inside_string():
    """SQL '' escaped-quote convention inside a string literal."""
    text = """CREATE TABLE t (
        c STRING COMMENT 'it''s an example',
        d INT
    ) USING DELTA;"""
    assert _names(text) == ["t"]


def test_unterminated_table_does_not_loop():
    """A CREATE TABLE without a matching close-paren must not infinite-loop."""
    text = """CREATE TABLE good (a INT) USING DELTA;
    CREATE TABLE bad (
        b INT
    -- never closes
    """
    # Should yield 'good' and not hang.
    assert _names(text) == ["good"]


def test_block_comment_inside_body():
    """C-style /* ... */ block comment is skipped over."""
    text = """CREATE TABLE t (
        /* embedded (parens) in block comment ) */
        a INT,
        b STRING
    ) USING DELTA;"""
    assert _names(text) == ["t"]


def test_backtick_quoted_identifier():
    """Hive backtick-quoted identifiers should be skipped over."""
    text = """CREATE TABLE t (
        `tricky col)` STRING,
        b INT
    ) USING DELTA;"""
    assert _names(text) == ["t"]


def test_double_quoted_identifier():
    """ANSI double-quoted identifiers should be skipped over."""
    text = '''CREATE TABLE t (
        "weird (col)" STRING,
        b INT
    ) USING DELTA;'''
    assert _names(text) == ["t"]


def test_repo_files_discover_all_tables():
    """End-to-end: actual repo DDL files yield the expected table counts."""
    repo = Path(__file__).resolve().parent.parent
    cases = [
        (repo / "ddl/multileg/02_multileg_gold_delta.sql", 3),
        (repo / "ddl/multileg/06_multileg_gold_hive.sql", 3),
        (repo / "ddl/option/02_option_gold_delta.sql", 3),
        (repo / "ddl/option/04_option_gold_hive.sql", 3),
    ]
    for path, expected in cases:
        text = path.read_text()
        names = _names(text)
        assert len(names) == expected, f"{path.name}: got {names}"


if __name__ == "__main__":
    tests = [v for k, v in globals().items() if k.startswith("test_") and callable(v)]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"  PASS  {t.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"  FAIL  {t.__name__}: {e}")
    print()
    print(f"{len(tests) - failed}/{len(tests)} tests passed")
    sys.exit(1 if failed else 0)
