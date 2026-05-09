# querymind-schema-service/services/introspector.py
"""
Schema introspection using SQLAlchemy Inspector.
Reflects tables, columns, types, PKs, FKs, indexes and row-count estimates.
"""

import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import Engine, inspect, text

from models.schema_models import (
    ColumnInfo,
    DatabaseSchema,
    ForeignKeyInfo,
    IndexInfo,
    TableInfo,
)

logger = logging.getLogger(__name__)

# Map raw SQLAlchemy type strings to human-readable names
_TYPE_ALIASES: dict[str, str] = {
    "VARCHAR": "text",
    "TEXT": "text",
    "CHAR": "text",
    "INTEGER": "integer",
    "BIGINT": "bigint",
    "SMALLINT": "smallint",
    "NUMERIC": "numeric",
    "DECIMAL": "decimal",
    "FLOAT": "float",
    "DOUBLE_PRECISION": "double",
    "BOOLEAN": "boolean",
    "DATE": "date",
    "TIMESTAMP": "timestamp",
    "TIMESTAMPTZ": "timestamptz",
    "TIME": "time",
    "INTERVAL": "interval",
    "UUID": "uuid",
    "JSONB": "jsonb",
    "JSON": "json",
    "BYTEA": "bytea",
    "ARRAY": "array",
    "INET": "inet",
    "CIDR": "cidr",
    "MACADDR": "macaddr",
    "TSVECTOR": "tsvector",
    "SERIAL": "serial",
    "BIGSERIAL": "bigserial",
}


def _human_type(sa_type: Any) -> str:
    raw = type(sa_type).__name__.upper()
    return _TYPE_ALIASES.get(raw, raw.lower())


def _get_row_count_estimate(engine: Engine, table_name: str, schema: str = "public") -> int:
    """Use pg_class.reltuples for a fast row-count estimate."""
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    "SELECT reltuples::bigint FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE c.relname = :table AND n.nspname = :schema"
                ),
                {"table": table_name, "schema": schema},
            )
            row = result.fetchone()
            return max(int(row[0]), 0) if row else 0
    except Exception as exc:
        logger.warning("Row count estimate failed for %s: %s", table_name, exc)
        return 0


def _get_table_comment(engine: Engine, table_name: str, schema: str = "public") -> str | None:
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    "SELECT obj_description((quote_ident(:schema) || '.' || quote_ident(:table))::regclass)"
                ),
                {"schema": schema, "table": table_name},
            )
            row = result.fetchone()
            return row[0] if row and row[0] else None
    except Exception:
        return None


def introspect_table(engine: Engine, table_name: str, schema: str = "public") -> TableInfo:
    """Reflect a single table and return a TableInfo model."""
    insp = inspect(engine)

    # Columns
    columns = []
    try:
        raw_columns = insp.get_columns(table_name, schema=schema)
    except Exception as exc:
        logger.error("Failed to reflect columns for %s: %s", table_name, exc)
        raw_columns = []

    pk_constraint = insp.get_pk_constraint(table_name, schema=schema)
    pk_cols: list[str] = pk_constraint.get("constrained_columns", [])

    for col in raw_columns:
        columns.append(
            ColumnInfo(
                name=col["name"],
                type=_human_type(col["type"]),
                nullable=col.get("nullable", True),
                primary_key=col["name"] in pk_cols,
                default=str(col["default"]) if col.get("default") is not None else None,
                comment=col.get("comment"),
            )
        )

    # Foreign keys
    fks = []
    try:
        for fk in insp.get_foreign_keys(table_name, schema=schema):
            fks.append(
                ForeignKeyInfo(
                    constrained_columns=fk.get("constrained_columns", []),
                    referred_table=fk.get("referred_table", ""),
                    referred_columns=fk.get("referred_columns", []),
                )
            )
    except Exception as exc:
        logger.warning("FK reflection failed for %s: %s", table_name, exc)

    # Indexes
    indexes = []
    try:
        for idx in insp.get_indexes(table_name, schema=schema):
            indexes.append(
                IndexInfo(
                    name=idx.get("name", ""),
                    columns=idx.get("column_names", []),
                    unique=idx.get("unique", False),
                )
            )
    except Exception as exc:
        logger.warning("Index reflection failed for %s: %s", table_name, exc)

    row_count = _get_row_count_estimate(engine, table_name, schema)
    comment = _get_table_comment(engine, table_name, schema)

    return TableInfo(
        table_name=table_name,
        schema=schema,
        columns=columns,
        primary_keys=pk_cols,
        foreign_keys=fks,
        indexes=indexes,
        row_count_estimate=row_count,
        comment=comment,
    )


def introspect_database(engine: Engine, database_name: str) -> DatabaseSchema:
    """Reflect all tables in the 'public' schema."""
    insp = inspect(engine)
    try:
        table_names = insp.get_table_names(schema="public")
    except Exception as exc:
        logger.error("Failed to list tables: %s", exc)
        table_names = []

    tables = []
    for tname in table_names:
        try:
            table_info = introspect_table(engine, tname, schema="public")
            tables.append(table_info)
        except Exception as exc:
            logger.error("Failed to introspect table %s: %s", tname, exc)

    return DatabaseSchema(
        database_name=database_name,
        tables=tables,
        total_tables=len(tables),
        introspected_at=datetime.now(timezone.utc).isoformat(),
    )


def get_table_list_with_estimates(engine: Engine) -> list[dict]:
    """Return a lightweight [{table_name, row_count_estimate}] list."""
    insp = inspect(engine)
    try:
        table_names = insp.get_table_names(schema="public")
    except Exception as exc:
        logger.error("Failed to list tables: %s", exc)
        return []

    result = []
    for tname in table_names:
        count = _get_row_count_estimate(engine, tname, schema="public")
        result.append({"table_name": tname, "row_count_estimate": count})
    return result
