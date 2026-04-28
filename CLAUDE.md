# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
uv sync

# Run the MCP server (stdio transport)
uv run main.py

# Run with a .env file for local development
uv run main.py  # python-dotenv loads .env automatically

# Install a new dependency
uv add <package>
```

There is no test suite and no lint configuration.

## Architecture

This is a **Model Context Protocol (MCP) server** for Databricks that exposes 34 tools over stdio transport. The server is built with [FastMCP](https://github.com/jlowin/fastmcp).

### File layout

| File | Purpose |
|------|---------|
| `main.py` | MCP server entry point — instantiates FastMCP and registers all 34 `@mcp.tool` functions |
| `databricks_sdk_utils.py` | All Databricks SDK calls: Unity Catalog, SQL execution, Lakeview dashboards, Apps |
| `lakebase_utils.py` | Postgres (Lakebase) connection, OAuth token management, query execution |
| `databricks_formatter.py` | Converts raw SDK query result dicts into Markdown tables |

### Tool groups

- **Unity Catalog** (5 tools): catalog/schema/table exploration, SQL execution via `execute_databricks_sql()`
- **Lakebase** (4 tools): Postgres queries against Databricks-managed Postgres; disabled gracefully if env vars absent
- **Lakeview Dashboards** (8 tools): CRUD + typed builders (table, counter, chart, multi-widget) that construct the Lakeview JSON spec before calling the API
- **Databricks Apps** (8 tools): list/get/deploy/start/stop/logs Databricks Apps

### Key design patterns

1. **Async/sync bridge** — MCP tools in `main.py` are `async def` and delegate to sync utility functions via `asyncio.to_thread()`. Keep utilities sync.
2. **Markdown output** — every tool returns a Markdown-formatted string; formatting logic lives in the `_utils` files, not in `main.py`.
3. **Token caching** — Lakebase OAuth tokens are cached with a 60-second expiry margin in `lakebase_utils._get_token()`.
4. **Lineage caching** — `_get_job_info_cached()` and `_get_notebook_id_cached()` use module-level dicts to avoid redundant API calls within a single tool invocation.

### Authentication

The Databricks SDK picks up credentials from environment variables automatically:

- PAT: `DATABRICKS_TOKEN`
- OAuth M2M: `DATABRICKS_CLIENT_ID` + `DATABRICKS_CLIENT_SECRET`

Required env vars: `DATABRICKS_HOST`, `DATABRICKS_SQL_WAREHOUSE_ID`, and one of the auth pairs above. Lakebase needs `PGHOST`, `PGDATABASE`, `PGUSER` (optional; server starts without them).

### Adding a new tool

1. Add the implementation function to the appropriate `*_utils.py` file (sync, returns a Markdown string).
2. Register it in `main.py` as an `async def` tool using `asyncio.to_thread()`.
3. Update the tool count in this file and in README.md.

## Delivery workflow

After every feature is complete, always:

1. Commit the changes with a descriptive message.
2. Push to `lakime main` (`git push lakime main`). Never push to `origin` (that points to `RafaelCartenet/mcp-databricks-server` — write access is denied and pushes there are forbidden).

Do this without waiting to be asked.
