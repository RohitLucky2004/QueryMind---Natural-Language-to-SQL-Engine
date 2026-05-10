import logging
from typing import Any

try:
    import sqlglot
    import sqlglot.expressions as exp
    SQLGLOT_AVAILABLE = True
except ImportError:
    SQLGLOT_AVAILABLE = False

logger = logging.getLogger(__name__)


def _extract_references(sql: str) -> tuple[set[str], dict[str, set[str]]]:
    """
    Parse SQL with sqlglot and extract all table and column references.

    Returns:
        tables: set of table names (lowercased, alias-resolved where possible)
        columns: dict of {table_name: set of column names} (best-effort)
    """
    tables: set[str] = set()
    columns: dict[str, set[str]] = {}

    try:
        tree = sqlglot.parse_one(sql, dialect="postgres")
    except Exception as e:
        logger.warning("sqlglot parse failed: %s", e)
        return tables, columns

    # Build alias → real table name map
    alias_map: dict[str, str] = {}
    for table_node in tree.find_all(exp.Table):
        real_name = table_node.name.lower()
        tables.add(real_name)
        alias = table_node.alias
        if alias:
            alias_map[alias.lower()] = real_name

    # Walk column references
    for col_node in tree.find_all(exp.Column):
        col_name = col_node.name.lower()
        table_ref = col_node.table
        if table_ref:
            resolved = alias_map.get(table_ref.lower(), table_ref.lower())
            columns.setdefault(resolved, set()).add(col_name)

    return tables, columns


def validate_against_schema(
    sql: str,
    schema: dict[str, Any],
) -> tuple[bool, str | None, list[str]]:
    """
    Pass 2: Parse SQL into an AST and cross-reference every table/column
    reference against the schema chunks returned by Schema Service.

    Args:
        sql: The generated SQL string.
        schema: Dict of {table_name: {columns: [{name, ...}], ...}}

    Returns:
        (passed, reason, invalid_references)
    """
    if not SQLGLOT_AVAILABLE:
        logger.warning("sqlglot not installed — skipping Pass 2")
        return True, None, []

    if not schema:
        logger.warning("Empty schema provided to Pass 2 — skipping")
        return True, None, []

    schema_lower = {k.lower(): v for k, v in schema.items()}
    invalid: list[str] = []

    try:
        tables, columns = _extract_references(sql)
    except Exception as e:
        logger.error("Pass 2 extraction error: %s", e)
        return True, None, []  # Don't block on parser errors

    # Check table references
    for table in tables:
        if table not in schema_lower:
            invalid.append(f"table:{table}")

    # Check column references (only for tables we know about)
    for table, cols in columns.items():
        if table not in schema_lower:
            continue  # Already flagged above
        schema_cols = {
            c["name"].lower()
            for c in schema_lower[table].get("columns", [])
        }
        for col in cols:
            if col not in schema_cols:
                invalid.append(f"column:{table}.{col}")

    if invalid:
        reason = f"SQL references unknown schema objects: {', '.join(invalid)}"
        logger.warning("Pass 2 FAILED — %s", reason)
        return False, reason, invalid

    logger.debug("Pass 2 passed — all schema references valid")
    return True, None, []
