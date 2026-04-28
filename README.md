[![Listed on Spark](https://spark.entire.vc/badges/listed.svg)](https://spark.entire.vc/assets/vb-databricks-smart-sql?utm_source=github&utm_medium=readme)
[![Install via Spark](https://spark.entire.vc/badges/vb-databricks-smart-sql/install.svg)](https://spark.entire.vc/assets/vb-databricks-smart-sql?utm_source=github&utm_medium=readme)

# Databricks MCP Server

An MCP server that gives LLM agents full access to your Databricks workspace: Unity Catalog metadata, SQL execution, Lakeview dashboards, Lakebase Postgres, and Databricks Apps.

- [Motivation](#motivation)
- [Overview](#overview)
- [Available Tools](#available-tools)
  - [Unity Catalog Tools](#unity-catalog-tools)
  - [Lakeview Dashboard Tools](#lakeview-dashboard-tools)
  - [Lakebase (Postgres) Tools](#lakebase-postgres-tools)
  - [Databricks Apps Tools](#databricks-apps-tools)
- [Setup](#setup)
  - [System Requirements](#system-requirements)
  - [Installation](#installation)
  - [Environment Variables](#environment-variables)
- [Permissions](#permissions)
- [Running the Server](#running-the-server)
  - [Standalone Mode](#standalone-mode)
  - [Using with Cursor](#using-with-cursor)
- [Example Agent Workflow](#example-agent-workflow)
- [Dependencies](#dependencies)

## Motivation

Databricks Unity Catalog allows detailed documentation of your data assets — catalogs, schemas, tables, and columns. Documenting these assets thoroughly requires an investment of time, and a common question is: what are the practical benefits?

This MCP server provides a strong answer. It enables LLMs to directly access and use that Unity Catalog metadata. The more comprehensively your data is described in UC, the more effectively an agent can understand your Databricks environment and construct accurate SQL queries to answer data requests autonomously.

## Overview

This Model Context Protocol (MCP) server connects an AI agent to your Databricks workspace. Its primary goal is to equip the agent with everything it needs to answer questions about your data independently — browsing UC metadata, exploring data lineage, executing SQL, managing dashboards, querying Lakebase Postgres, and controlling Databricks Apps.

Beyond traditional catalog browsing, the server lets agents discover and analyze the code that processes your data. Through lineage capabilities, agents can identify notebooks and jobs that read from or write to tables, then examine the actual transformation logic, business rules, and data quality checks. This creates a feedback loop where agents understand not just *what* data exists, but *how* it's produced.

## Available Tools

### Unity Catalog Tools

Five tools for navigating and understanding your Unity Catalog assets. They return Markdown optimized for LLM consumption.

| Tool | Description |
|---|---|
| `list_uc_catalogs()` | List all accessible catalogs with names, descriptions, and types |
| `describe_uc_catalog(catalog_name)` | List all schemas in a catalog with names and descriptions |
| `describe_uc_schema(catalog_name, schema_name, include_columns?)` | List all tables in a schema; set `include_columns=True` to include column details |
| `describe_uc_table(full_table_name, include_lineage?)` | Full table structure; set `include_lineage=True` for upstream/downstream tables, notebooks, and jobs |
| `execute_sql_query(sql)` | Run any SQL against the Databricks SQL warehouse |

**UC metadata in practice:**

Well-documented UC metadata lets agents operate with richer context. Schema-level descriptions help identify relevant data sources:

![Schema Description in Unity Catalog](assets/schema_description.png)
*Fig 1: A schema in Unity Catalog with user-provided descriptions — accessible to an LLM via this server.*

Column-level comments clarify the semantics of each field, enabling precise SQL conditions and selections:

![Table Column Descriptions in Unity Catalog](assets/table_columns_description.png)
*Fig 2: Column-level descriptions in Unity Catalog, passed to the LLM for accurate query generation.*

**Lineage capabilities** (`include_lineage=True` on `describe_uc_table`):
- Upstream and downstream table dependencies
- Notebooks that read from or write to the table (with workspace paths and job associations)
- Code discovery: notebook paths the agent can read to analyze actual transformation logic

### Lakeview Dashboard Tools

Create, publish, and manage **Lakeview (AI/BI) dashboards** directly from conversation context.

| Tool | Description |
|---|---|
| `list_dashboards()` | List all dashboards with IDs and lifecycle states |
| `get_dashboard(dashboard_id)` | Inspect a dashboard's datasets and page structure |
| `create_dashboard(display_name, serialized_dashboard, warehouse_id?)` | Create a dashboard from a raw JSON definition |
| `create_table_dashboard(display_name, sql_query, ...)` | SQL query → table widget dashboard |
| `create_counter_dashboard(display_name, sql_query, value_field, ...)` | SQL query → KPI metric card |
| `create_chart_dashboard(display_name, sql_query, chart_type, x_field, y_field, ...)` | SQL query → bar / line / area / scatter / pie chart |
| `create_multi_widget_dashboard(display_name, widgets, warehouse_id?)` | Compose multiple counters and charts on a single page |
| `publish_dashboard(dashboard_id, warehouse_id?)` | Publish a draft dashboard so viewers can access it |
| `trash_dashboard(dashboard_id)` | Soft-delete a dashboard |

**Counter widget note:** Lakeview counter (KPI) widgets require spec `version: 2` and a real aggregation expression (`SUM(\`field\`)`, `COUNT(\`*\`)`, etc.) with `disaggregated: false` — plain column references do not render. All `create_counter_dashboard` and `create_multi_widget_dashboard` calls apply `SUM` by default (configurable via `agg_fn`). Since the dataset SQL is pre-aggregated, `SUM` of a single row returns the same value.

### Lakebase (Postgres) Tools

[Lakebase](https://docs.databricks.com/en/lakebase/index.html) is Databricks' managed Postgres service. It stores both synced Gold tables (read-only replicas from Delta Lake) and native agent-state tables.

Auth uses a **short-lived OAuth token as the Postgres password**, obtained via `sdk_client.postgres.generate_database_credential()`. Tokens are cached and refreshed automatically 60 seconds before expiry.

| Tool | Description |
|---|---|
| `lakebase_query(sql, schema?)` | Run any SQL (SELECT / INSERT / UPDATE / DELETE) against Lakebase Postgres |
| `lakebase_list_schemas()` | List all user-visible schemas (excludes system schemas) |
| `lakebase_list_tables(schema_name?)` | List tables and views in a schema |
| `lakebase_describe_table(table_name, schema_name?)` | Get column definitions, nullability, PK, and defaults |

**Example:**

```
lakebase_list_schemas()
→ - `procurement`  - `public`

lakebase_list_tables(schema_name="procurement")
→ - email_inbox (table)  - po_drafts (table)  - budget_ledger (table) ...

lakebase_describe_table("po_drafts", schema_name="procurement")
→ | Column | Type | Nullable | PK | Default |
  | po_id  | uuid | NO       | ✓  |         |
  | ...

lakebase_query("SELECT status, COUNT(*) FROM procurement.po_drafts GROUP BY status")
→ | status   | count |
  | DRAFT    | 12    |
  | APPROVED | 4     |
```

### Databricks Apps Tools

List, inspect, deploy, start, and stop **Databricks Apps** directly from conversation context.

| Tool | Description |
|---|---|
| `list_databricks_apps()` | List all apps with state (`app=`, `compute=`) and URL |
| `get_databricks_app(app_name)` | Full details: state, URL, active/pending deployment |
| `deploy_databricks_app(app_name, source_code_path, mode?)` | Deploy an app from a workspace source path |
| `list_databricks_app_deployments(app_name)` | Deployment history for an app |
| `get_databricks_app_deployment(app_name, deployment_id)` | Poll a specific deployment's status |
| `start_databricks_app(app_name)` | Start a stopped app |
| `stop_databricks_app(app_name)` | Stop a running app |

**Deployment modes:**

| Mode | Behaviour |
|---|---|
| `SNAPSHOT` (default) | Point-in-time copy of the source path at deploy time |
| `AUTO_SYNC` | Continuously syncs the app with changes to the source path |

**State transitions:**
- Deployment: `IN_PROGRESS` → `SUCCEEDED` (or `FAILED` / `CANCELLED`)
- Compute: `STARTING` → `ACTIVE` → `STOPPED` / `ERROR`
- Application: `DEPLOYING` → `RUNNING` / `CRASHED` / `UNAVAILABLE`

**Example:**

```
list_databricks_apps()
→ - **livezerobus** — app=RUNNING, compute=ACTIVE
    URL: https://....azuredatabricks.net/driver-proxy/o/.../livezerobus/...

get_databricks_app("livezerobus")
→ Active deployment: 01abc... | Source: /Repos/main/LiveZerobus/backend

deploy_databricks_app("livezerobus", "/Repos/main/LiveZerobus/backend")
→ Deployment started — ID: 01def...

get_databricks_app_deployment("livezerobus", "01def...")
→ State: `IN_PROGRESS`   (poll again)
→ State: `SUCCEEDED`
```

## Lakeview Dashboard Tools

This server can create, publish, and manage **Lakeview (AI/BI) dashboards** directly from conversation context.

| Tool | Description |
|---|---|
| `list_dashboards()` | List all Lakeview dashboards with IDs and lifecycle states |
| `get_dashboard(dashboard_id)` | Inspect a dashboard's datasets and page structure |
| `create_dashboard(display_name, serialized_dashboard, warehouse_id?)` | Create from a raw JSON definition |
| `create_table_dashboard(display_name, sql_query, ...)` | One-liner: SQL query → table widget dashboard |
| `create_counter_dashboard(display_name, sql_query, value_field, ...)` | One-liner: SQL query → KPI metric card |
| `create_chart_dashboard(display_name, sql_query, chart_type, x_field, y_field, ...)` | One-liner: SQL query → bar / line / area / scatter / pie chart |
| `create_multi_widget_dashboard(display_name, widgets, warehouse_id?)` | Compose multiple counters + charts on a single page |
| `publish_dashboard(dashboard_id, warehouse_id?)` | Publish a draft dashboard so viewers can access it |
| `trash_dashboard(dashboard_id)` | Soft-delete a dashboard |

### Counter widget note

Lakeview counter (KPI) widgets require spec `version: 2` and a real aggregation expression
(`SUM(\`field\`)`, `COUNT(\`*\`)`, etc.) with `disaggregated: false` in the widget query —
plain column references do not render. All `create_counter_dashboard` / `create_multi_widget_dashboard`
calls apply `SUM` by default (configurable via `agg_fn`). Since the dataset SQL is already
pre-aggregated, `SUM` of a single row returns the same value.

## Lakebase (Postgres) Tools

[Lakebase](https://docs.databricks.com/en/lakebase/index.html) is Databricks' managed Postgres
service. It stores both synced Gold tables (read-only replicas from Delta Lake) and native
agent-state tables (email threads, PO drafts, budget ledger, etc.).

Auth uses a **short-lived OAuth token as the Postgres password**, obtained via the
`sdk_client.postgres.generate_database_credential()` SDK call. Tokens are cached and
refreshed automatically 60 seconds before expiry.

### Additional environment variables

```env
# Postgres connection
PGHOST=<lakebase-host>
PGDATABASE=<database-name>
PGUSER=<username>
PGPORT=5432                    # optional, default 5432
PG_SCHEMA=<default-schema>     # optional, sets search_path

# Lakebase resource path
LAKEBASE_PROJECT=<project-name>    # e.g. myzerobus
LAKEBASE_BRANCH=<branch-name>      # e.g. production
LAKEBASE_ENDPOINT=<endpoint-name>  # e.g. primary
```

All six Lakebase variables are **optional** — if any are missing the tools return an informative
message rather than crashing the server.

### Tools

| Tool | Description |
|---|---|
| `lakebase_query(sql, schema?)` | Run any SQL (SELECT / INSERT / UPDATE / DELETE) against Lakebase Postgres |
| `lakebase_list_schemas()` | List all user-visible schemas (excludes system schemas) |
| `lakebase_list_tables(schema_name?)` | List tables and views in a schema |
| `lakebase_describe_table(table_name, schema_name?)` | Get column definitions, nullability, PK, and defaults |

### Example

```
lakebase_list_schemas()
→ - `procurement`  - `public`

lakebase_list_tables(schema_name="procurement")
→ - email_inbox (table)  - po_drafts (table)  - budget_ledger (table) ...

lakebase_describe_table("po_drafts", schema_name="procurement")
→ | Column | Type | Nullable | PK | Default |
  | po_id  | uuid | NO       | ✓  |         |
  | ...

lakebase_query("SELECT status, COUNT(*) FROM procurement.po_drafts GROUP BY status")
→ | status   | count |
  | DRAFT    | 12    |
  | APPROVED | 4     |
```

## Databricks Apps Tools

This server can list, inspect, deploy, start, and stop **Databricks Apps** directly from conversation context.

| Tool | Description |
|---|---|
| `list_databricks_apps()` | List all apps with state (`app=`, `compute=`) and URL |
| `get_databricks_app(app_name)` | Full details: state, URL, active/pending deployment |
| `deploy_databricks_app(app_name, source_code_path, mode?)` | Deploy an app from a workspace source path |
| `list_databricks_app_deployments(app_name)` | Deployment history for an app |
| `get_databricks_app_deployment(app_name, deployment_id)` | Poll a specific deployment status |
| `start_databricks_app(app_name)` | Start a stopped app |
| `stop_databricks_app(app_name)` | Stop a running app |

### Deployment modes

| Mode | Behaviour |
|---|---|
| `SNAPSHOT` (default) | Takes a point-in-time copy of the source path at deploy time |
| `AUTO_SYNC` | Keeps the app continuously in sync with changes to the source path |

### Deployment states

A deployment progresses through: `IN_PROGRESS` → `SUCCEEDED` (or `FAILED` / `CANCELLED`).
App compute states: `STARTING` → `ACTIVE` → `STOPPED` / `ERROR`.
App application states: `DEPLOYING` → `RUNNING` / `CRASHED` / `UNAVAILABLE`.

### Example workflow

```
list_databricks_apps()
→ - **livezerobus** — app=RUNNING, compute=ACTIVE
    URL: https://....azuredatabricks.net/driver-proxy/o/.../livezerobus/...

get_databricks_app("livezerobus")
→ Active deployment: 01abc... | Source: /Repos/main/LiveZerobus/backend

deploy_databricks_app("livezerobus", "/Repos/main/LiveZerobus/backend")
→ Deployment started — ID: 01def...

get_databricks_app_deployment("livezerobus", "01def...")
→ State: `IN_PROGRESS`   (poll again)
→ State: `SUCCEEDED`
```

## Setup

### System Requirements

- Python 3.10+
- `uv` (optional but recommended): [installation guide](https://docs.astral.sh/uv/getting-started/installation/)

### Installation

```bash
pip install -r requirements.txt
```

Or with `uv`:

```bash
uv pip install -r requirements.txt
```

### Environment Variables

**Required — Databricks connection:**

```env
DATABRICKS_HOST="your-databricks-instance.cloud.databricks.com"
DATABRICKS_TOKEN="your-databricks-personal-access-token"
DATABRICKS_SQL_WAREHOUSE_ID="your-sql-warehouse-id"
```

The `DATABRICKS_SQL_WAREHOUSE_ID` is used for SQL execution and lineage fetching. Metadata browsing (listing catalogs, schemas, tables) does not require it unless lineage is requested.

**Optional — Lakebase (Postgres):**

All six variables are optional. If any are missing the Lakebase tools return an informative message rather than crashing the server.

```env
PGHOST=<lakebase-host>
PGDATABASE=<database-name>          # default: databricks_postgres
PGUSER=<username>
PGPORT=5432                         # default: 5432
PG_SCHEMA=<default-schema>          # sets search_path
LAKEBASE_PROJECT=<project-name>     # e.g. myzerobus
LAKEBASE_BRANCH=<branch-name>       # e.g. production
LAKEBASE_ENDPOINT=<endpoint-name>   # e.g. primary
```

You can use a `.env` file in the project root — `python-dotenv` loads it automatically.

## Permissions

The identity associated with `DATABRICKS_TOKEN` needs:

1. **Unity Catalog**: `USE CATALOG` on accessed catalogs, `USE SCHEMA` on accessed schemas, `SELECT` on queried tables.
2. **SQL Warehouse**: `CAN_USE` on the warehouse specified by `DATABRICKS_SQL_WAREHOUSE_ID`.
3. **Lakebase**: `CAN_CONNECT_AND_CREATE` on the Lakebase instance (for token generation and queries).
4. **Apps**: `CAN_MANAGE` on apps to deploy/start/stop; `CAN_VIEW` to list and inspect.

For production use, prefer a service principal with narrowly scoped permissions. Rotate tokens regularly and audit UC and query history logs to monitor usage.

## Running the Server

### Standalone Mode

```bash
python main.py
```

This starts the MCP server over stdio, compatible with Agent Composer and other MCP clients.

### Using with Cursor

Configure the server in `~/.cursor/mcp.json`:

```bash
mkdir -p ~/.cursor
touch ~/.cursor/mcp.json
```

Add the following, replacing the path with your actual installation directory:

```json
{
    "mcpServers": {
        "databricks": {
            "command": "uv",
            "args": [
                "--directory",
                "/path/to/your/mcp-databricks-server",
                "run",
                "main.py"
            ]
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
            "args": [
                "/path/to/your/mcp-databricks-server/main.py"
            ]
        }
    }
}
```

Restart Cursor to apply the changes.

## Example Agent Workflow

This MCP server empowers an agent to autonomously navigate your Databricks environment. A typical interaction might look like:

![Agent actively using MCP tools to find data](assets/agent_usage.png)
*Fig 3: An LLM agent using the Databricks MCP tools, demonstrating iterative exploration and query refinement.*

1. **Discover catalogs**: `list_uc_catalogs()` — agent identifies `prod_catalog`
2. **Explore the catalog**: `describe_uc_catalog(catalog_name="prod_catalog")` — sees `sales_schema`, `inventory_schema`
3. **Browse a schema**: `describe_uc_schema(catalog_name="prod_catalog", schema_name="sales_schema")` — sees `orders`, `customers`
4. **Inspect a table**: `describe_uc_table(full_table_name="prod_catalog.sales_schema.orders")` — gets column types
5. **Trace lineage**: `describe_uc_table(..., include_lineage=True)` — discovers that `/Repos/production/etl/sales_processing.py` writes to the table
6. **Read the transformation code**: agent reads the notebook file directly in the IDE
7. **Execute a query**: `execute_sql_query(sql="SELECT customer_id, SUM(order_total) FROM prod_catalog.sales_schema.orders WHERE order_date > '2023-01-01' GROUP BY customer_id")`

## Dependencies

<<<<<<< HEAD
-   `databricks-sdk`: For interacting with the Databricks REST APIs and Unity Catalog.
-   `python-dotenv`: For loading environment variables from a `.env` file.
-   `mcp[cli]`: The Model Context Protocol library.
-   `asyncio`: For asynchronous operations within the MCP server.
-   `httpx` (typically a sub-dependency of `databricks-sdk` or `mcp`): For making HTTP requests.
-   `psycopg2-binary`: For connecting to Lakebase (Databricks-managed Postgres).
=======
- `databricks-sdk` — Databricks REST API, Unity Catalog, Apps, and Lakebase token generation
- `mcp[cli]` — Model Context Protocol library
- `python-dotenv` — loads `.env` files
- `httpx` — HTTP client
- `psycopg2-binary` — Postgres driver for Lakebase queries
>>>>>>> 03a061f (Rewrite README as a single coherent document)
