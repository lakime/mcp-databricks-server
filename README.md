# Databricks MCP Server

An MCP server for Databricks workspaces. Gives LLM agents tools to explore Unity Catalog, run SQL queries, manage Lakeview dashboards, query Lakebase Postgres, and control Databricks Apps.

## Tools

### Unity Catalog

| Tool | Args | Description |
|---|---|---|
| `list_uc_catalogs` | — | List all catalogs with names, descriptions, and types |
| `describe_uc_catalog` | `catalog_name` | List all schemas in a catalog |
| `describe_uc_schema` | `catalog_name`, `schema_name`, `include_columns?` | List tables in a schema; add columns with `include_columns=True` |
| `describe_uc_table` | `full_table_name`, `include_lineage?` | Full table structure; add upstream/downstream tables + notebook/job lineage with `include_lineage=True` |
| `execute_sql_query` | `sql` | Run any SQL against the configured SQL warehouse |

All tools return Markdown.

### Lakeview Dashboards

| Tool | Args | Description |
|---|---|---|
| `list_dashboards` | — | List all dashboards with IDs and lifecycle states |
| `get_dashboard` | `dashboard_id` | Inspect datasets and page/widget structure |
| `create_dashboard` | `display_name`, `serialized_dashboard`, `warehouse_id?` | Create from a raw JSON definition |
| `create_table_dashboard` | `display_name`, `sql_query`, `warehouse_id?`, ... | SQL → table widget |
| `create_counter_dashboard` | `display_name`, `sql_query`, `value_field`, `warehouse_id?`, ... | SQL → KPI counter card |
| `create_chart_dashboard` | `display_name`, `sql_query`, `chart_type`, `x_field`, `y_field`, `warehouse_id?`, ... | SQL → chart (bar/line/area/scatter/pie) |
| `create_multi_widget_dashboard` | `display_name`, `widgets`, `warehouse_id?` | Multiple counters + charts on one page |
| `publish_dashboard` | `dashboard_id`, `warehouse_id?` | Publish a draft so viewers can access it |
| `trash_dashboard` | `dashboard_id` | Soft-delete a dashboard |

### Lakebase (Postgres)

[Lakebase](https://docs.databricks.com/en/lakebase/index.html) is Databricks-managed Postgres. Auth uses a short-lived OAuth token as the password, refreshed automatically.

| Tool | Args | Description |
|---|---|---|
| `lakebase_query` | `sql`, `schema?` | Run any SQL (SELECT / INSERT / UPDATE / DELETE) |
| `lakebase_list_schemas` | — | List all user-visible schemas |
| `lakebase_list_tables` | `schema_name?` | List tables and views in a schema |
| `lakebase_describe_table` | `table_name`, `schema_name?` | Column names, types, nullability, PK, defaults |

### Databricks Apps

| Tool | Args | Description |
|---|---|---|
| `list_databricks_apps` | — | List all apps with state and URL |
| `get_databricks_app` | `app_name` | State, URL, active deployment, pending deployment |
| `deploy_databricks_app` | `app_name`, `source_code_path`, `mode?` | Deploy from a workspace path (`SNAPSHOT` or `AUTO_SYNC`) |
| `list_databricks_app_deployments` | `app_name` | Deployment history, newest first |
| `get_databricks_app_deployment` | `app_name`, `deployment_id` | Poll a deployment until `SUCCEEDED` or `FAILED` |
| `start_databricks_app` | `app_name` | Start a stopped app |
| `stop_databricks_app` | `app_name` | Stop a running app |

## Setup

### Install

```bash
pip install -r requirements.txt
```

### Configure

Create a `.env` file or export the variables directly.

**Required:**
```env
DATABRICKS_HOST=your-workspace.azuredatabricks.net
DATABRICKS_TOKEN=your-personal-access-token
DATABRICKS_SQL_WAREHOUSE_ID=your-warehouse-id
```

**Lakebase (optional — tools degrade gracefully if unset):**
```env
PGHOST=your-lakebase-host
PGDATABASE=databricks_postgres
PGUSER=your-username
PGPORT=5432
PG_SCHEMA=your-default-schema
LAKEBASE_PROJECT=your-project
LAKEBASE_BRANCH=production
LAKEBASE_ENDPOINT=primary
```

### Permissions

The identity behind `DATABRICKS_TOKEN` needs:

- **Unity Catalog**: `USE CATALOG`, `USE SCHEMA`, `SELECT` on accessed objects
- **SQL Warehouse**: `CAN_USE` on the warehouse specified by `DATABRICKS_SQL_WAREHOUSE_ID`
- **Lakebase**: `CAN_CONNECT_AND_CREATE` on the Lakebase instance
- **Apps**: `CAN_MANAGE` to deploy/start/stop; `CAN_VIEW` to list and inspect

## Run

### Standalone

```bash
python main.py
```

Starts the server over stdio.

### Cursor

Add to `~/.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "databricks": {
      "command": "uv",
      "args": ["--directory", "/path/to/mcp-databricks-server", "run", "main.py"]
    }
  }
}
```

Or with plain `python`:

```json
{
  "mcpServers": {
    "databricks": {
      "command": "python",
      "args": ["/path/to/mcp-databricks-server/main.py"]
    }
  }
}
```

Restart Cursor after saving.

## Dependencies

- `databricks-sdk` — UC, SQL warehouse, Apps, Lakebase token generation
- `mcp[cli]` — Model Context Protocol
- `psycopg2-binary` — Postgres driver for Lakebase
- `python-dotenv` — `.env` file loading
- `httpx` — HTTP client
