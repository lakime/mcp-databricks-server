"""Lakebase (Databricks-managed Postgres) query utilities for the MCP server.

Auth: Lakebase uses short-lived OAuth tokens as the Postgres password.
Tokens are obtained via `sdk_client.postgres.generate_database_credential()`
and cached locally with automatic refresh before expiry.

Required env vars:
    PGHOST                  Lakebase Postgres host
    PGDATABASE              Database name
    PGUSER                  Postgres username
    LAKEBASE_PROJECT        Lakebase project name  (e.g. myzerobus)
    LAKEBASE_BRANCH         Branch name            (e.g. production)
    LAKEBASE_ENDPOINT       Endpoint name          (e.g. primary)

Optional:
    PGPORT                  Default: 5432
    PG_SCHEMA               Default search_path schema (e.g. procurement)
"""
from __future__ import annotations

import os
import threading
import time
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PGHOST     = os.environ.get("PGHOST", "")
PGPORT     = int(os.environ.get("PGPORT", "5432"))
PGDATABASE = os.environ.get("PGDATABASE", "")
PGUSER     = os.environ.get("PGUSER", "")
PG_SCHEMA  = os.environ.get("PG_SCHEMA", "")

LAKEBASE_PROJECT  = os.environ.get("LAKEBASE_PROJECT", "")
LAKEBASE_BRANCH   = os.environ.get("LAKEBASE_BRANCH", "")
LAKEBASE_ENDPOINT = os.environ.get("LAKEBASE_ENDPOINT", "")

_TOKEN_REFRESH_MARGIN_S = 60
_DEFAULT_TOKEN_TTL_S    = 3600
_TOKEN_LOCK:  threading.Lock          = threading.Lock()
_TOKEN_CACHE: tuple[str, float]       = ("", 0.0)

_MISSING_MSG = (
    "Lakebase is not configured. Set PGHOST, PGDATABASE, PGUSER, "
    "LAKEBASE_PROJECT, LAKEBASE_BRANCH, and LAKEBASE_ENDPOINT."
)


def _lakebase_configured() -> bool:
    return all([PGHOST, PGDATABASE, PGUSER,
                LAKEBASE_PROJECT, LAKEBASE_BRANCH, LAKEBASE_ENDPOINT])


def _get_token() -> str:
    """Return a valid OAuth token, refreshing if close to expiry."""
    global _TOKEN_CACHE
    with _TOKEN_LOCK:
        token, exp = _TOKEN_CACHE
        if not token or exp - time.time() < _TOKEN_REFRESH_MARGIN_S:
            from databricks_sdk_utils import sdk_client
            resource = (
                f"projects/{LAKEBASE_PROJECT}"
                f"/branches/{LAKEBASE_BRANCH}"
                f"/endpoints/{LAKEBASE_ENDPOINT}"
            )
            cred = sdk_client.postgres.generate_database_credential(endpoint=resource)
            token = getattr(cred, "token", None) or ""
            if not token:
                raise RuntimeError(
                    f"generate_database_credential returned no token for {resource}"
                )
            exp_attr = getattr(cred, "expiration_time", None)
            exp = (
                exp_attr.timestamp()
                if exp_attr and hasattr(exp_attr, "timestamp")
                else time.time() + _DEFAULT_TOKEN_TTL_S
            )
            _TOKEN_CACHE = (token, exp)
        return token


def _connect(schema_override: Optional[str] = None):
    """Open a psycopg2 connection with a fresh OAuth token as password."""
    token = _get_token()
    conn = psycopg2.connect(
        host=PGHOST,
        port=PGPORT,
        dbname=PGDATABASE,
        user=PGUSER,
        password=token,
        sslmode="require",
        cursor_factory=psycopg2.extras.RealDictCursor,
    )
    search = schema_override or PG_SCHEMA
    if search:
        with conn.cursor() as cur:
            cur.execute(f"SET search_path TO {search}, public")
    return conn


def _rows_to_markdown(columns: List[str], rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return "_No rows returned._"
    header  = "| " + " | ".join(columns) + " |"
    divider = "| " + " | ".join("---" for _ in columns) + " |"
    lines   = [header, divider]
    for row in rows:
        cells = [str(row.get(c, "")) for c in columns]
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def execute_lakebase_query(sql: str, schema: Optional[str] = None) -> str:
    """Execute an arbitrary SQL query against Lakebase and return Markdown results."""
    if not _lakebase_configured():
        return _MISSING_MSG
    try:
        with _connect(schema) as conn, conn.cursor() as cur:
            cur.execute(sql)
            if cur.description is None:
                conn.commit()
                return f"Query executed successfully. Rows affected: {cur.rowcount}"
            rows = [dict(r) for r in cur.fetchall()]
            cols = [d.name for d in cur.description]
            return f"**{len(rows)} row(s)**\n\n{_rows_to_markdown(cols, rows)}"
    except Exception as e:
        return f"Lakebase query error: {str(e)}"


def list_lakebase_schemas() -> str:
    """List all user-visible schemas in Lakebase (excludes system schemas)."""
    if not _lakebase_configured():
        return _MISSING_MSG
    try:
        sql = """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('information_schema','pg_catalog','pg_toast','pg_temp_1','pg_toast_temp_1')
              AND schema_name NOT LIKE 'pg_%'
            ORDER BY schema_name
        """
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(sql)
            rows = [dict(r) for r in cur.fetchall()]
        if not rows:
            return "# Lakebase Schemas\n\nNo user schemas found."
        lines = ["# Lakebase Schemas", ""]
        for r in rows:
            lines.append(f"- `{r['schema_name']}`")
        return "\n".join(lines)
    except Exception as e:
        return f"Error listing Lakebase schemas: {str(e)}"


def list_lakebase_tables(schema_name: Optional[str] = None) -> str:
    """List tables and views in a Lakebase schema."""
    if not _lakebase_configured():
        return _MISSING_MSG
    schema = schema_name or PG_SCHEMA or "public"
    try:
        sql = """
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = %s
            ORDER BY table_type, table_name
        """
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(sql, (schema,))
            rows = [dict(r) for r in cur.fetchall()]
        if not rows:
            return f"# Tables in `{schema}`\n\nNo tables or views found."
        lines = [f"# Tables in `{schema}`", ""]
        for r in rows:
            kind = "view" if r["table_type"] == "VIEW" else "table"
            lines.append(f"- **`{r['table_name']}`** ({kind})")
        return "\n".join(lines)
    except Exception as e:
        return f"Error listing Lakebase tables: {str(e)}"


def describe_lakebase_table(table_name: str, schema_name: Optional[str] = None) -> str:
    """Return column definitions for a Lakebase table."""
    if not _lakebase_configured():
        return _MISSING_MSG
    schema = schema_name or PG_SCHEMA or "public"
    try:
        col_sql = """
            SELECT column_name, data_type, character_maximum_length,
                   is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        pk_sql = """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema    = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = %s AND tc.table_name = %s
        """
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(col_sql, (schema, table_name))
            cols = [dict(r) for r in cur.fetchall()]
            cur.execute(pk_sql, (schema, table_name))
            pk_cols = {r["column_name"] for r in cur.fetchall()}

        if not cols:
            return f"Table `{schema}.{table_name}` not found or has no columns."

        lines = [f"# Table: `{schema}.{table_name}`", "", "| Column | Type | Nullable | PK | Default |",
                 "| --- | --- | --- | --- | --- |"]
        for c in cols:
            dtype = c["data_type"]
            if c.get("character_maximum_length"):
                dtype += f"({c['character_maximum_length']})"
            pk  = "✓" if c["column_name"] in pk_cols else ""
            nullable = c["is_nullable"]
            default  = c["column_default"] or ""
            lines.append(f"| `{c['column_name']}` | `{dtype}` | {nullable} | {pk} | {default} |")
        return "\n".join(lines)
    except Exception as e:
        return f"Error describing Lakebase table: {str(e)}"
