from typing import Optional
import asyncio
from mcp.server.fastmcp import FastMCP
from databricks_formatter import format_query_results
from lakebase_utils import (
    execute_lakebase_query,
    list_lakebase_schemas,
    list_lakebase_tables,
    describe_lakebase_table,
)
from databricks_sdk_utils import (
    get_uc_table_details,
    get_uc_catalog_details,
    get_uc_schema_details,
    execute_databricks_sql,
    get_uc_all_catalogs_summary,
    list_lakeview_dashboards,
    get_lakeview_dashboard,
    create_lakeview_dashboard,
    create_table_dashboard,
    create_counter_dashboard,
    create_chart_dashboard,
    create_multi_widget_dashboard,
    publish_lakeview_dashboard,
    trash_lakeview_dashboard,
    VALID_CHART_TYPES,
)


mcp = FastMCP("databricks")

@mcp.tool()
async def execute_sql_query(sql: str) -> str:
    """
    Executes a given SQL query against the Databricks SQL warehouse and returns the formatted results.
    
    Use this tool when you need to run specific SQL queries, such as SELECT, SHOW, or other DQL statements.
    This is ideal for targeted data retrieval or for queries that are too complex for the structured description tools.
    The results are returned in a human-readable, Markdown-like table format.

    Args:
        sql: The complete SQL query string to execute.
    """
    try:
        sdk_result = await asyncio.to_thread(execute_databricks_sql, sql_query=sql)
        
        status = sdk_result.get("status")
        if status == "failed":
            error_message = sdk_result.get("error", "Unknown query execution error.")
            details = sdk_result.get("details", "No additional details provided.")
            return f"SQL Query Failed: {error_message}\nDetails: {details}"
        elif status == "error":
            error_message = sdk_result.get("error", "Unknown error during SQL execution.")
            details = sdk_result.get("details", "No additional details provided.")
            return f"Error during SQL Execution: {error_message}\nDetails: {details}"
        elif status == "success":
            return format_query_results(sdk_result)
        else:
            return f"Received an unexpected status from query execution: {status}. Result: {sdk_result}"
            
    except Exception as e:
        return f"An unexpected error occurred while executing SQL query: {str(e)}"


@mcp.tool()
async def describe_uc_table(full_table_name: str, include_lineage: Optional[bool] = False) -> str:
    """
    Provides a detailed description of a specific Unity Catalog table.
    
    Use this tool to understand the structure (columns, data types, partitioning) of a single table.
    This is essential before constructing SQL queries against the table.
    
    Optionally, it can include comprehensive lineage information that goes beyond traditional 
    table-to-table dependencies:

    **Table Lineage:**
    - Upstream tables (tables this table reads from)
    - Downstream tables (tables that read from this table)
    
    **Notebook & Job Lineage:**
    - Notebooks that read from this table, including:
      * Notebook name and workspace path
      * Associated Databricks job information (job name, ID, task details)
    - Notebooks that write to this table with the same detailed context
    
    **Use Cases:**
    - Data impact analysis: understand what breaks if you modify this table
    - Code discovery: find notebooks that process this data for further analysis
    - Debugging: trace data flow issues by examining both table dependencies and processing code
    - Documentation: understand the complete data ecosystem around a table

    The lineage information allows LLMs and tools to subsequently fetch the actual notebook 
    code content for deeper analysis of data transformations and business logic.

    The output is formatted in Markdown.

    Args:
        full_table_name: The fully qualified three-part name of the table (e.g., `catalog.schema.table`).
        include_lineage: Set to True to fetch and include comprehensive lineage (tables, notebooks, jobs). 
                         Defaults to False. May take longer to retrieve but provides rich context for 
                         understanding data dependencies and enabling code exploration.
    """
    try:
        details_markdown = await asyncio.to_thread(
            get_uc_table_details,
            full_table_name=full_table_name,
            include_lineage=include_lineage
        )
        return details_markdown
    except ImportError as e:
        return f"Error initializing Databricks SDK utilities: {str(e)}. Please ensure DATABRICKS_HOST and DATABRICKS_TOKEN are set."
    except Exception as e:
        return f"Error getting detailed table description for '{full_table_name}': {str(e)}"

@mcp.tool()
async def describe_uc_catalog(catalog_name: str) -> str:
    """
    Provides a summary of a specific Unity Catalog, listing all its schemas with their names and descriptions.
    
    Use this tool when you know the catalog name and need to discover the schemas within it.
    This is often a precursor to describing a specific schema or table.
    The output is formatted in Markdown.

    Args:
        catalog_name: The name of the Unity Catalog to describe (e.g., `prod`, `dev`, `system`).
    """
    try:
        summary_markdown = await asyncio.to_thread(
            get_uc_catalog_details,
            catalog_name=catalog_name
        )
        return summary_markdown
    except ImportError as e:
        return f"Error initializing Databricks SDK utilities: {str(e)}. Please ensure DATABRICKS_HOST and DATABRICKS_TOKEN are set."
    except Exception as e:
        return f"Error getting catalog summary for '{catalog_name}': {str(e)}"

@mcp.tool()
async def describe_uc_schema(catalog_name: str, schema_name: str, include_columns: Optional[bool] = False) -> str:
    """
    Provides detailed information about a specific schema within a Unity Catalog.
    
    Use this tool to understand the contents of a schema, primarily its tables.
    Optionally, it can list all tables within the schema and their column details.
    Set `include_columns=True` to get column information, which is crucial for query construction but makes the output longer.
    If `include_columns=False`, only table names and descriptions are shown, useful for a quicker overview.
    The output is formatted in Markdown.

    Args:
        catalog_name: The name of the catalog containing the schema.
        schema_name: The name of the schema to describe.
        include_columns: If True, lists tables with their columns. Defaults to False for a briefer summary.
    """
    try:
        details_markdown = await asyncio.to_thread(
            get_uc_schema_details,
            catalog_name=catalog_name,
            schema_name=schema_name,
            include_columns=include_columns
        )
        return details_markdown
    except ImportError as e:
        return f"Error initializing Databricks SDK utilities: {str(e)}. Please ensure DATABRICKS_HOST and DATABRICKS_TOKEN are set."
    except Exception as e:
        return f"Error getting detailed schema description for '{catalog_name}.{schema_name}': {str(e)}"

@mcp.tool()
async def list_uc_catalogs() -> str:
    """
    Lists all available Unity Catalogs with their names, descriptions, and types.
    
    Use this tool as a starting point to discover available data sources when you don't know specific catalog names.
    It provides a high-level overview of all accessible catalogs in the workspace.
    The output is formatted in Markdown.
    """
    try:
        summary_markdown = await asyncio.to_thread(get_uc_all_catalogs_summary)
        return summary_markdown
    except ImportError as e:
        return f"Error initializing Databricks SDK utilities: {str(e)}. Please ensure DATABRICKS_HOST and DATABRICKS_TOKEN are set."
    except Exception as e:
        return f"Error listing catalogs: {str(e)}"

@mcp.tool()
async def lakebase_query(sql: str, schema: Optional[str] = None) -> str:
    """
    Executes a SQL query against Lakebase (Databricks-managed Postgres) and returns
    the results as a Markdown table.

    Lakebase stores agent state and synced Gold tables as native Postgres tables.
    Use this tool for INSERT / UPDATE / DELETE / SELECT against those tables.

    Args:
        sql: Any valid Postgres SQL statement.
        schema: Optional schema to set as search_path before running the query.
                Defaults to the PG_SCHEMA env var if set.
    """
    try:
        return await asyncio.to_thread(execute_lakebase_query, sql, schema)
    except Exception as e:
        return f"Error executing Lakebase query: {str(e)}"


@mcp.tool()
async def lakebase_list_schemas() -> str:
    """
    Lists all user-visible schemas in Lakebase Postgres (excludes system schemas).

    Use this as a starting point to discover what data is available in Lakebase.
    """
    try:
        return await asyncio.to_thread(list_lakebase_schemas)
    except Exception as e:
        return f"Error listing Lakebase schemas: {str(e)}"


@mcp.tool()
async def lakebase_list_tables(schema_name: Optional[str] = None) -> str:
    """
    Lists tables and views in a Lakebase Postgres schema.

    Args:
        schema_name: Schema to inspect. Defaults to the PG_SCHEMA env var, then 'public'.
    """
    try:
        return await asyncio.to_thread(list_lakebase_tables, schema_name)
    except Exception as e:
        return f"Error listing Lakebase tables: {str(e)}"


@mcp.tool()
async def lakebase_describe_table(table_name: str, schema_name: Optional[str] = None) -> str:
    """
    Returns column definitions (name, type, nullability, primary key, default) for a
    Lakebase Postgres table.

    Args:
        table_name: Table name (without schema prefix).
        schema_name: Schema containing the table. Defaults to PG_SCHEMA env var, then 'public'.
    """
    try:
        return await asyncio.to_thread(describe_lakebase_table, table_name, schema_name)
    except Exception as e:
        return f"Error describing Lakebase table: {str(e)}"


@mcp.tool()
async def list_dashboards() -> str:
    """
    Lists all Lakeview dashboards in the Databricks workspace.

    Use this tool to discover existing dashboards, their names, IDs, and lifecycle states.
    The output is formatted in Markdown.
    """
    try:
        return await asyncio.to_thread(list_lakeview_dashboards)
    except Exception as e:
        return f"Error listing dashboards: {str(e)}"


@mcp.tool()
async def get_dashboard(dashboard_id: str) -> str:
    """
    Gets details of a specific Lakeview dashboard, including its datasets and page structure.

    Args:
        dashboard_id: The dashboard ID (e.g. `01efd...`).
    """
    try:
        return await asyncio.to_thread(get_lakeview_dashboard, dashboard_id)
    except Exception as e:
        return f"Error getting dashboard: {str(e)}"


@mcp.tool()
async def create_dashboard(
    display_name: str,
    serialized_dashboard: str,
    warehouse_id: Optional[str] = None,
) -> str:
    """
    Creates a Lakeview dashboard from a fully specified serialized dashboard JSON string.

    Use this when you have already constructed the complete dashboard definition JSON.
    For a quick single-table dashboard from a SQL query, use `create_table_dashboard` instead.

    The serialized_dashboard must be a JSON string with this structure:
    {
      "datasets": [{"name": "ds_id", "displayName": "...", "query": "SELECT ..."}],
      "pages": [{"name": "page_id", "displayName": "...", "layout": [...widgets...]}]
    }

    Args:
        display_name: Human-readable name for the dashboard.
        serialized_dashboard: JSON string defining datasets, pages, and widgets.
        warehouse_id: Optional SQL warehouse ID to attach. Uses the default warehouse if omitted.
    """
    try:
        return await asyncio.to_thread(create_lakeview_dashboard, display_name, serialized_dashboard, warehouse_id)
    except Exception as e:
        return f"Error creating dashboard: {str(e)}"


@mcp.tool()
async def create_table_dashboard(
    display_name: str,
    sql_query: str,
    warehouse_id: Optional[str] = None,
    dataset_display_name: Optional[str] = None,
    table_title: Optional[str] = None,
) -> str:
    """
    Creates a Lakeview dashboard with a single table visualization from a SQL query.

    This is the quickest way to turn a SQL query into a dashboard. It auto-generates
    the dashboard JSON with one dataset and one table widget.

    Args:
        display_name: Human-readable name for the dashboard.
        sql_query: The SQL query that populates the table (e.g. `SELECT * FROM livezerobus.procurement.inventory_snapshot LIMIT 100`).
        warehouse_id: Optional SQL warehouse ID to attach.
        dataset_display_name: Optional label for the dataset (defaults to display_name).
        table_title: Optional title shown above the table widget (defaults to display_name).
    """
    try:
        return await asyncio.to_thread(
            create_table_dashboard,
            display_name, sql_query, warehouse_id, dataset_display_name, table_title
        )
    except Exception as e:
        return f"Error creating table dashboard: {str(e)}"


@mcp.tool()
async def create_counter_dashboard(
    display_name: str,
    sql_query: str,
    value_field: str,
    warehouse_id: Optional[str] = None,
    dataset_display_name: Optional[str] = None,
    title: Optional[str] = None,
    description: Optional[str] = None,
) -> str:
    """
    Creates a Lakeview dashboard with a single counter (KPI / metric card) widget.

    The SQL query should return a single row with a numeric column that becomes
    the big displayed number. Typical use: `SELECT COUNT(*) AS total FROM ...`
    or `SELECT SUM(amount) AS revenue FROM ...`.

    Args:
        display_name: Human-readable name for the dashboard.
        sql_query: SQL that returns the metric value (should yield one row).
        value_field: Column name from the query to display as the metric (e.g. `total`, `revenue`).
        warehouse_id: Optional SQL warehouse ID to attach.
        dataset_display_name: Optional label for the dataset (defaults to display_name).
        title: Optional title shown above the counter widget (defaults to display_name).
        description: Optional subtitle shown below the counter value.
    """
    try:
        return await asyncio.to_thread(
            create_counter_dashboard,
            display_name, sql_query, value_field, warehouse_id,
            dataset_display_name, title, description,
        )
    except Exception as e:
        return f"Error creating counter dashboard: {str(e)}"


@mcp.tool()
async def create_chart_dashboard(
    display_name: str,
    sql_query: str,
    chart_type: str,
    x_field: str,
    y_field: str,
    warehouse_id: Optional[str] = None,
    dataset_display_name: Optional[str] = None,
    title: Optional[str] = None,
    color_field: Optional[str] = None,
) -> str:
    """
    Creates a Lakeview dashboard with a single chart visualization.

    Supported chart_type values: bar, line, area, scatter, pie.

    Args:
        display_name: Human-readable name for the dashboard.
        sql_query: SQL that returns the data to visualize.
        chart_type: One of: bar, line, area, scatter, pie.
        x_field: Column name to use for the X axis (or pie category).
        y_field: Column name to use for the Y axis (or pie value).
        warehouse_id: Optional SQL warehouse ID to attach.
        dataset_display_name: Optional label for the dataset (defaults to display_name).
        title: Optional title shown above the chart (defaults to display_name).
        color_field: Optional column to use for color grouping / series splitting.
    """
    valid = sorted(VALID_CHART_TYPES)
    if chart_type not in VALID_CHART_TYPES:
        return f"Invalid chart_type '{chart_type}'. Must be one of: {valid}"
    try:
        return await asyncio.to_thread(
            create_chart_dashboard,
            display_name, sql_query, chart_type, x_field, y_field,
            warehouse_id, dataset_display_name, title, color_field,
        )
    except Exception as e:
        return f"Error creating chart dashboard: {str(e)}"


@mcp.tool()
async def publish_dashboard(dashboard_id: str, warehouse_id: Optional[str] = None) -> str:
    """
    Publishes a Lakeview dashboard draft so it is accessible to viewers.

    Newly created dashboards are in draft state. Call this after `create_dashboard`
    or `create_table_dashboard` to make the dashboard publicly visible.

    Args:
        dashboard_id: The dashboard ID returned by create_dashboard.
        warehouse_id: Optional SQL warehouse ID to use for the published version.
    """
    try:
        return await asyncio.to_thread(publish_lakeview_dashboard, dashboard_id, warehouse_id)
    except Exception as e:
        return f"Error publishing dashboard: {str(e)}"


@mcp.tool()
async def create_multi_widget_dashboard(
    display_name: str,
    widgets: list,
    warehouse_id: Optional[str] = None,
) -> str:
    """
    Creates a Lakeview dashboard with multiple widgets (counters + charts) on one page.

    Each item in `widgets` is an object with:
      type        : "counter" | "bar" | "line" | "area" | "scatter" | "pie"
      sql_query   : SQL for this widget's data
      title       : Widget title
      value_field : (counter) column to show as the big number
      x_field     : (chart) column for x axis
      y_field     : (chart) column for y axis
      description : (counter, optional) subtitle text
      x, y, w, h  : grid position/size (grid is 6 wide; counters default h=3, charts h=6)

    Example widgets list:
    [
      {"type": "counter", "sql_query": "SELECT COUNT(*) AS n FROM t", "value_field": "n",
       "title": "Total", "x": 0, "y": 0, "w": 2, "h": 3},
      {"type": "bar", "sql_query": "SELECT cat, SUM(v) AS total FROM t GROUP BY cat",
       "x_field": "cat", "y_field": "total", "title": "By Category", "x": 0, "y": 3, "w": 6, "h": 6}
    ]
    """
    try:
        return await asyncio.to_thread(create_multi_widget_dashboard, display_name, widgets, warehouse_id)
    except Exception as e:
        return f"Error creating multi-widget dashboard: {str(e)}"


@mcp.tool()
async def trash_dashboard(dashboard_id: str) -> str:
    """
    Moves a Lakeview dashboard to trash (soft delete).

    Args:
        dashboard_id: The dashboard ID to trash.
    """
    try:
        return await asyncio.to_thread(trash_lakeview_dashboard, dashboard_id)
    except Exception as e:
        return f"Error trashing dashboard: {str(e)}"


if __name__ == "__main__":
    mcp.run(transport='stdio')
