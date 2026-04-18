import streamlit as st
import yaml
import re
import pandas as pd
import snowflake.connector
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, datetime, timedelta

# ---------------------------------------------------------------------------
# Environment detection: Snowsight (Snowpark) vs Local
# ---------------------------------------------------------------------------
_IS_SNOWSIGHT = False
_SNOWPARK_SESSION = None
try:
    from snowflake.snowpark.context import get_active_session
    _SNOWPARK_SESSION = get_active_session()
    _IS_SNOWSIGHT = True
except Exception:
    pass

# ---------------------------------------------------------------------------
# Page config (Snowsight may ignore or error on this)
# ---------------------------------------------------------------------------
try:
    st.set_page_config(page_title="Self-Service Report Mart", layout="wide")
except Exception:
    pass
st.title("Self-Service Report Mart")

# ---------------------------------------------------------------------------
# Snowflake connection — auto-detect environment
# ---------------------------------------------------------------------------
@st.cache_resource
def get_connection():
    if _IS_SNOWSIGHT:
        return _SNOWPARK_SESSION.connection
    return snowflake.connector.connect(connection_name="migration")


def _reconnect():
    """Clear cached connection and create a new one."""
    if _IS_SNOWSIGHT:
        return get_connection()  # session-managed, no reconnect needed
    get_connection.clear()
    return get_connection()


# Validate connection on startup (fail fast with guidance if local creds missing)
if not _IS_SNOWSIGHT:
    try:
        get_connection()
    except Exception as _conn_err:
        st.error(
            "**Local connection failed.** "
            "Create `~/.snowflake/connections.toml` with:\n\n"
            "```toml\n"
            "[migration]\n"
            "account = \"your_account\"\n"
            "user = \"your_user\"\n"
            "password = \"your_password\"\n"
            "warehouse = \"your_warehouse\"\n"
            "database = \"your_database\"\n"
            "schema = \"your_schema\"\n"
            "```"
        )
        st.stop()


# ---------------------------------------------------------------------------
# Helper: run a query and return a dataframe
# ---------------------------------------------------------------------------
@st.cache_data(ttl=60)
def run_query(sql: str):
    return _exec_query(sql)


@st.cache_data(ttl=600)
def run_query_cached(sql: str):
    """Longer-TTL cache (10 min) for VQR-matched queries."""
    return _exec_query(sql)


def _exec_query(sql: str):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(sql)
        cols = [desc[0] for desc in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    except snowflake.connector.errors.ProgrammingError as e:
        if "390114" in str(e):
            conn = _reconnect()
            cur = conn.cursor()
            try:
                cur.execute(sql)
                cols = [desc[0] for desc in cur.description]
                return pd.DataFrame(cur.fetchall(), columns=cols)
            finally:
                cur.close()
        raise
    finally:
        cur.close()


# ---------------------------------------------------------------------------
# Helper: call Cortex Analyst via REST API
# ---------------------------------------------------------------------------
def send_analyst_message(semantic_view_fqn: str, messages: list[dict]) -> dict:
    """Send a message to Cortex Analyst and return the response dict."""
    conn = get_connection()
    parts = semantic_view_fqn.split(".")
    body = {
        "messages": messages,
        "semantic_view": {
            "database": parts[0],
            "schema": parts[1],
            "name": parts[2],
        },
    }
    try:
        resp = conn.rest.request(
            "/api/v2/cortex/analyst/message",
            body=body,
            method="post",
            client="rest",
        )
        return resp
    except snowflake.connector.errors.ProgrammingError as e:
        if "390114" in str(e):
            conn = _reconnect()
            resp = conn.rest.request(
                "/api/v2/cortex/analyst/message",
                body=body,
                method="post",
                client="rest",
            )
            return resp
        raise


# ---------------------------------------------------------------------------
# Helper: save report config to per-user REPORT_MART_CONFIG_<USERNAME> table
# ---------------------------------------------------------------------------
def _get_current_user() -> str:
    df = run_query("SELECT CURRENT_USER() AS USR")
    return df.iloc[0]["USR"]


def _ensure_user_config_table(cur, table_name: str):
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            REPORT_ID NUMBER(38,0) DEFAULT <database>.<schema>.REPORT_MART_CONFIG_SEQ.NEXTVAL,
            REPORT_NAME VARCHAR(500),
            REQUEST_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
            REQUEST_USER VARCHAR(255) DEFAULT CURRENT_USER(),
            SEMANTIC_VIEW VARCHAR(500),
            TABLE_NAME VARCHAR(500),
            TIME_DIMENSION VARCHAR(255),
            START_DATE VARCHAR(20),
            END_DATE VARCHAR(20),
            AGG_WINDOW VARCHAR(20),
            GROUP_COL VARCHAR(255),
            EXCLUDE_VALS VARCHAR(2000),
            MEASURES VARCHAR(2000),
            AGGREGATION VARCHAR(50),
            INCLUDE_NULLS BOOLEAN,
            CHART_TYPE VARCHAR(50),
            SHOW_ALL_AVAILABLE BOOLEAN,
            ADD_ACCUMULATIVE BOOLEAN,
            GENERATED_SQL TEXT
        )
    """)


_MASTER_HISTORY_TABLE = "<database>.<schema>.REPORT_MART_MASTER_CONFIG_HISTORY"


def _ensure_master_history_table(cur):
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {_MASTER_HISTORY_TABLE} (
            REPORT_ID NUMBER(38,0),
            REPORT_NAME VARCHAR(500),
            REQUEST_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
            REQUEST_USER VARCHAR(255) DEFAULT CURRENT_USER(),
            SEMANTIC_VIEW VARCHAR(500),
            TABLE_NAME VARCHAR(500),
            TIME_DIMENSION VARCHAR(255),
            START_DATE VARCHAR(20),
            END_DATE VARCHAR(20),
            AGG_WINDOW VARCHAR(20),
            GROUP_COL VARCHAR(255),
            EXCLUDE_VALS VARCHAR(2000),
            MEASURES VARCHAR(2000),
            AGGREGATION VARCHAR(50),
            INCLUDE_NULLS BOOLEAN,
            CHART_TYPE VARCHAR(50),
            SHOW_ALL_AVAILABLE BOOLEAN,
            ADD_ACCUMULATIVE BOOLEAN,
            GENERATED_SQL TEXT,
            ACTION VARCHAR(10)
        )
    """)


def _log_master_history(cur, action: str, report_id, report_name: str,
                        sv_fqn: str = None, table_name: str = None,
                        time_dim: str = None, start_date: str = None,
                        end_date: str = None, agg_window: str = None,
                        group_col: str = None, exclude_vals: str = None,
                        measures: str = None, aggregation: str = None,
                        include_nulls: bool = None, chart_type: str = None,
                        show_all: bool = None, add_accum: bool = None,
                        generated_sql: str = None):
    """Insert a row into the master history table."""
    _ensure_master_history_table(cur)
    cur.execute(
        f"""INSERT INTO {_MASTER_HISTORY_TABLE}
           (REPORT_ID, REPORT_NAME, SEMANTIC_VIEW, TABLE_NAME, TIME_DIMENSION,
            START_DATE, END_DATE, AGG_WINDOW, GROUP_COL, EXCLUDE_VALS, MEASURES,
            AGGREGATION, INCLUDE_NULLS, CHART_TYPE, SHOW_ALL_AVAILABLE,
            ADD_ACCUMULATIVE, GENERATED_SQL, ACTION)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (
            report_id, report_name, sv_fqn, table_name, time_dim,
            start_date, end_date, agg_window, group_col, exclude_vals,
            measures, aggregation, include_nulls, chart_type, show_all,
            add_accum, generated_sql, action,
        ),
    )


def save_to_report_mart(params: dict, sql: str, sv_fqn: str, report_name: str):
    conn = get_connection()
    cur = conn.cursor()
    try:
        username = _get_current_user()
        table_name = f"<database>.<schema>.REPORT_MART_CONFIG_{username}"
        _ensure_user_config_table(cur, table_name)
        cur.execute(
            f"""INSERT INTO {table_name}
               (REPORT_NAME, SEMANTIC_VIEW, TABLE_NAME, TIME_DIMENSION, START_DATE,
                END_DATE, AGG_WINDOW, GROUP_COL, EXCLUDE_VALS, MEASURES, AGGREGATION,
                INCLUDE_NULLS, CHART_TYPE, SHOW_ALL_AVAILABLE, ADD_ACCUMULATIVE,
                GENERATED_SQL)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                report_name,
                sv_fqn,
                params.get("table"),
                params.get("date_col"),
                params.get("start_date"),
                params.get("end_date"),
                params.get("agg_window"),
                params.get("group_col"),
                ", ".join(params.get("exclude_vals", [])) or None,
                ", ".join(params.get("measures", [])),
                ", ".join(params.get("aggs", [])),
                params.get("include_nulls", False),
                params.get("chart_type"),
                params.get("show_all_available", False),
                params.get("add_accumulative", False),
                sql,
            ),
        )
        # Fetch the REPORT_ID just inserted and log to master history
        cur.execute(f"SELECT MAX(REPORT_ID) AS RID FROM {table_name}")
        rid_row = cur.fetchone()
        rid = rid_row[0] if rid_row else None
        _log_master_history(
            cur, action="CREATE", report_id=rid, report_name=report_name,
            sv_fqn=sv_fqn, table_name=params.get("table"),
            time_dim=params.get("date_col"),
            start_date=params.get("start_date"),
            end_date=params.get("end_date"),
            agg_window=params.get("agg_window"),
            group_col=params.get("group_col"),
            exclude_vals=", ".join(params.get("exclude_vals", [])) or None,
            measures=", ".join(params.get("measures", [])),
            aggregation=", ".join(params.get("aggs", [])),
            include_nulls=params.get("include_nulls", False),
            chart_type=params.get("chart_type"),
            show_all=params.get("show_all_available", False),
            add_accum=params.get("add_accumulative", False),
            generated_sql=sql,
        )
        return table_name
    finally:
        cur.close()


# ---------------------------------------------------------------------------
# Helper: get current session database and schema
# ---------------------------------------------------------------------------
@st.cache_data(ttl=120)
def get_current_context() -> tuple[str, str]:
    df = run_query("SELECT CURRENT_DATABASE() AS DB, CURRENT_SCHEMA() AS SCH")
    if df.empty:
        return None, None
    return df.iloc[0]["DB"], df.iloc[0]["SCH"]


# ---------------------------------------------------------------------------
# Helper: list accessible databases
# ---------------------------------------------------------------------------
@st.cache_data(ttl=120)
def list_databases() -> list[str]:
    df = run_query("SHOW DATABASES")
    if df.empty:
        return []
    col = "name" if "name" in df.columns else df.columns[1]
    return df[col].tolist()


# ---------------------------------------------------------------------------
# Helper: list schemas in a database
# ---------------------------------------------------------------------------
@st.cache_data(ttl=120)
def list_schemas(database: str) -> list[str]:
    df = run_query(f"SHOW SCHEMAS IN DATABASE {database}")
    if df.empty:
        return []
    col = "name" if "name" in df.columns else df.columns[1]
    return df[col].tolist()


# ---------------------------------------------------------------------------
# Helper: list semantic views in a schema
# ---------------------------------------------------------------------------
@st.cache_data(ttl=120)
def list_semantic_views(database: str, schema: str) -> list[str]:
    df = run_query(f"SHOW SEMANTIC VIEWS IN SCHEMA {database}.{schema}")
    if df.empty:
        return []
    col = "name" if "name" in df.columns else df.columns[1]
    return df[col].tolist()


# ---------------------------------------------------------------------------
# Helper: read YAML from a semantic view
# ---------------------------------------------------------------------------
@st.cache_data(ttl=120)
def read_semantic_yaml(fqn: str) -> str:
    df = run_query(f"SELECT SYSTEM$READ_YAML_FROM_SEMANTIC_VIEW('{fqn}') AS YAML_CONTENT")
    return df.iloc[0]["YAML_CONTENT"]


# ---------------------------------------------------------------------------
# Helper: parse YAML string -> dict
# ---------------------------------------------------------------------------
def parse_yaml(yaml_str: str) -> dict:
    return yaml.safe_load(yaml_str)


# ---------------------------------------------------------------------------
# Helper: extract elements from parsed model
# ---------------------------------------------------------------------------
def get_table_fqn(model: dict) -> str:
    t = model["tables"][0]["base_table"]
    return f"{t['database']}.{t['schema']}.{t['table']}"


def get_table_name(model: dict) -> str:
    return model["tables"][0]["name"]


def get_dimensions(model: dict) -> list[str]:
    return [d["name"] for d in model["tables"][0].get("dimensions", [])]


def get_time_dimensions(model: dict) -> list[str]:
    return [d["name"] for d in model["tables"][0].get("time_dimensions", [])]


def get_time_dimension_types(model: dict) -> dict[str, str]:
    return {d["name"]: d.get("data_type", "").upper() for d in model["tables"][0].get("time_dimensions", [])}


def get_facts(model: dict) -> list[str]:
    return [f["name"] for f in model["tables"][0].get("facts", [])]


def get_vqr_sqls(model: dict) -> set[str]:
    """Return a set of normalised VQR SQL strings for fast lookup."""
    return {vq.get("sql", "").strip().upper() for vq in model.get("verified_queries", []) if vq.get("sql")}


# ---------------------------------------------------------------------------
# Helper: build query from parameter selections
# ---------------------------------------------------------------------------
_AGG_WINDOW_MAP = {"Daily": "DAY", "Weekly": "WEEK", "Monthly": "MONTH"}


def build_query(
    table_fqn: str,
    date_col: str | None,
    date_col_type: str,
    start_date,
    end_date,
    group_col: str | None,
    exclude_vals: list[str],
    selected_facts: list[str],
    selected_aggs: list[str],
    agg_window: str,
    include_nulls: bool,
) -> str:
    select_parts = []
    group_positions = []
    pos = 1

    # Time dimension with truncation — convert to date if needed
    if date_col:
        trunc = _AGG_WINDOW_MAP.get(agg_window, "DAY")
        if "NUMBER" in date_col_type or "INT" in date_col_type or "FLOAT" in date_col_type:
            date_expr = f"TO_DATE(TO_VARCHAR({date_col}), 'YYYYMMDD')"
        elif "VARCHAR" in date_col_type or "STRING" in date_col_type or "TEXT" in date_col_type:
            date_expr = f"TO_DATE({date_col})"
        else:
            date_expr = date_col
        select_parts.append(f"DATE_TRUNC('{trunc}', {date_expr}) AS TIME_PERIOD")
        group_positions.append(str(pos))
        pos += 1

    # Group criteria
    if group_col:
        select_parts.append(group_col)
        group_positions.append(str(pos))
        pos += 1

    # Aggregated measures
    for fact in selected_facts:
        for agg in selected_aggs:
            if agg == "COUNT DISTINCT":
                expr = f"COUNT(DISTINCT {fact})"
                alias = f"{fact}_COUNT_DISTINCT"
            else:
                expr = f"{agg}({fact})"
                alias = f"{fact}_{agg}"
            select_parts.append(f"{expr} AS {alias}")

    # FROM
    sep = ",\n    "
    sql = f"SELECT\n    {sep.join(select_parts)}\nFROM {table_fqn}"

    # WHERE
    where_clauses = []
    if date_col and start_date and end_date:
        if "NUMBER" in date_col_type or "INT" in date_col_type or "FLOAT" in date_col_type:
            where_date = f"TO_DATE(TO_VARCHAR({date_col}), 'YYYYMMDD')"
        elif "VARCHAR" in date_col_type or "STRING" in date_col_type or "TEXT" in date_col_type:
            where_date = f"TO_DATE({date_col})"
        else:
            where_date = date_col
        where_clauses.append(f"{where_date} BETWEEN '{start_date}' AND '{end_date}'")
    if group_col and exclude_vals:
        escaped = "', '".join(v.replace("'", "''") for v in exclude_vals)
        where_clauses.append(f"{group_col} NOT IN ('{escaped}')")
    if not include_nulls and selected_facts:
        null_filters = [f"{f} IS NOT NULL" for f in selected_facts]
        where_clauses.append(" AND ".join(null_filters))

    if where_clauses:
        sql += f"\nWHERE {' AND '.join(where_clauses)}"

    # GROUP BY
    if group_positions:
        sql += f"\nGROUP BY {', '.join(group_positions)}"

    # ORDER BY
    sql += "\nORDER BY 1"

    return sql


# ---------------------------------------------------------------------------
# Helper: render a plotly chart from query results
# ---------------------------------------------------------------------------
def render_chart(result_df, chart_type, group_col, title=None, height=None):
    """Build a plotly figure from a query result dataframe and chart config.

    Returns a plotly figure or None if the data can't be charted.
    """
    if result_df.empty:
        return None

    x_col = None
    color_col = None
    if "TIME_PERIOD" in result_df.columns:
        x_col = "TIME_PERIOD"
        if group_col and group_col in result_df.columns:
            color_col = group_col
    elif group_col and group_col in result_df.columns:
        x_col = group_col

    # If one measure, no time dimension, and group-by: color each category
    y_cols_check = [c for c in result_df.columns if c not in {x_col, color_col}]
    if x_col != "TIME_PERIOD" and x_col == group_col and len(y_cols_check) == 1:
        color_col = group_col

    is_time = x_col == "TIME_PERIOD"
    if is_time:
        result_df = result_df.sort_values(x_col, ascending=True)

    exclude_from_y = {x_col, color_col}
    y_cols = [c for c in result_df.columns if c not in exclude_from_y]

    for yc in y_cols:
        result_df[yc] = pd.to_numeric(result_df[yc], errors="coerce")

    if is_time:
        sorted_x = result_df[x_col].astype(str).unique().tolist()
        x_axis_cfg = dict(xaxis_title=x_col, xaxis_type="category",
                          xaxis_categoryorder="array",
                          xaxis_categoryarray=sorted_x)
    else:
        x_axis_cfg = dict(xaxis_title=x_col, xaxis_type="category")

    if not x_col or not y_cols:
        return None

    fig = None
    if color_col:
        measure_col = y_cols[0]
        if chart_type == "Barchart":
            fig = px.bar(result_df, x=x_col, y=measure_col, color=color_col, barmode="group")
            fig.update_layout(yaxis_title=measure_col, **x_axis_cfg)
            fig.update_yaxes(tickformat=",", type="linear")
        elif chart_type == "Linechart":
            fig = px.line(result_df, x=x_col, y=measure_col, color=color_col)
            fig.update_layout(yaxis_title=measure_col, **x_axis_cfg)
            fig.update_yaxes(tickformat=",", type="linear")
        elif chart_type == "Boxplot":
            fig = px.box(result_df, x=x_col, y=measure_col, color=color_col)
            fig.update_layout(**x_axis_cfg)
            fig.update_yaxes(tickformat=",", type="linear")
        elif chart_type == "Waterfall":
            labels = result_df[x_col].astype(str).tolist()
            values = result_df[measure_col].tolist()
            fig = go.Figure(go.Waterfall(
                x=labels, y=values,
                measure=["relative"] * len(values),
                textposition="outside",
            ))
            fig.update_layout(title=measure_col, showlegend=False, **x_axis_cfg)
    else:
        if chart_type == "Barchart":
            fig = px.bar(result_df, x=x_col, y=y_cols, barmode="group",
                         labels={"value": "Value", "variable": "Measure"})
            fig.update_layout(yaxis_title="Value", **x_axis_cfg)
            fig.update_yaxes(tickformat=",", type="linear")
        elif chart_type == "Linechart":
            fig = px.line(result_df, x=x_col, y=y_cols,
                          labels={"value": "Value", "variable": "Measure"})
            fig.update_layout(yaxis_title="Value", **x_axis_cfg)
            fig.update_yaxes(tickformat=",", type="linear")
        elif chart_type == "Boxplot":
            melted = result_df.melt(id_vars=[x_col], value_vars=y_cols,
                                    var_name="Measure", value_name="Value")
            fig = px.box(melted, x=x_col, y="Value", color="Measure")
            fig.update_layout(**x_axis_cfg)
            fig.update_yaxes(tickformat=",", type="linear")
        elif chart_type == "Waterfall":
            measure_col = y_cols[0]
            labels = result_df[x_col].astype(str).tolist()
            values = result_df[measure_col].tolist()
            fig = go.Figure(go.Waterfall(
                x=labels, y=values,
                measure=["relative"] * len(values),
                textposition="outside",
            ))
            fig.update_layout(title=measure_col, showlegend=False, **x_axis_cfg)

    if fig:
        if title:
            fig.update_layout(title=title)
        if height:
            fig.update_layout(height=height)

    return fig


# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab_self_service, tab_data_mart, tab_cortex = st.tabs(
    ["Self-Service", "Report Mart", "Cortex Agent"]
)

# ===== Self-Service tab =====
with tab_self_service:

    # Top-level 4:1 layout — left (Semantic View + Parameters), right (Configurations)
    current_db, current_sch = get_current_context()
    col_left, col_right = st.columns([4, 1])

    with col_left:
        # --- Semantic View Initializer ---
        with st.container(border=True):
            st.subheader("Semantic View")

            # Row 1: Database, Schema, Semantic View, Table Name
            col_db, col_sch, col_sv, col_tbl = st.columns(
                [1, 1, 1, 1], vertical_alignment="bottom"
            )

            with col_db:
                db_list = list_databases()
                default_db_idx = (db_list.index(current_db) + 1) if current_db and current_db in db_list else 0
                selected_db = st.selectbox(
                    "Database",
                    options=[None] + db_list,
                    index=default_db_idx,
                    format_func=lambda x: "— Select —" if x is None else x,
                )

            db_selected = selected_db is not None

            with col_sch:
                if db_selected:
                    sch_list = list_schemas(selected_db)
                    default_sch_idx = (sch_list.index(current_sch) + 1) if current_sch and selected_db == current_db and current_sch in sch_list else 0
                    selected_sch = st.selectbox(
                        "Schema",
                        options=[None] + sch_list,
                        index=default_sch_idx,
                        format_func=lambda x: "— Select —" if x is None else x,
                    )
                else:
                    selected_sch = st.selectbox(
                        "Schema",
                        options=["— Select —"],
                        disabled=True,
                    )
                    selected_sch = None

            sch_selected = selected_sch is not None

            with col_sv:
                if db_selected and sch_selected:
                    sv_list = list_semantic_views(selected_db, selected_sch)
                    selected_sv = st.selectbox(
                        "Semantic View",
                        options=sv_list if sv_list else [],
                        index=0,
                        placeholder="Select a semantic view",
                        disabled=not sv_list,
                    )
                else:
                    selected_sv = st.selectbox(
                        "Semantic View",
                        options=["— Select —"],
                        disabled=True,
                    )
                    selected_sv = None

            # Parse YAML on selection
            yaml_str = None
            model = None
            if selected_sv and selected_db and selected_sch:
                fqn = f"{selected_db}.{selected_sch}.{selected_sv}"
                yaml_str = read_semantic_yaml(fqn)
                model = parse_yaml(yaml_str)

            with col_tbl:
                tbl_name = get_table_fqn(model) if model else ""
                st.text_input("Table Name", value=tbl_name, disabled=True)

            # Row 2: Expander + Download button
            col_exp, col_dl = st.columns([3, 1], vertical_alignment="bottom")

            with col_exp:
                with st.expander("View Semantic Model YAML"):
                    if yaml_str:
                        st.code(yaml_str, language="yaml")
                    else:
                        st.caption("Select a semantic view to display its YAML.")

            with col_dl:
                if yaml_str:
                    st.download_button(
                        "Download YAML",
                        data=yaml_str,
                        file_name=f"{selected_sv.lower()}_semantic_model.yaml",
                        mime="text/yaml",
                    )
                else:
                    st.download_button("Download YAML", data="", disabled=True)

        # --- Parameters ---
        if model:
            table_fqn = get_table_fqn(model)
            time_dims = get_time_dimensions(model)
            time_dim_types = get_time_dimension_types(model)
            dims = get_dimensions(model)
            facts = get_facts(model)

            with st.container(border=True):
                st.subheader("Parameters")

                # ---- Row 1: Date Parameters ----
                col_date, col_range, col_start, col_end = st.columns(
                    [1, 1, 1, 1], vertical_alignment="bottom"
                )

                with col_date:
                    date_col = st.selectbox(
                        "Choose Time Dimension",
                        options=[None] + time_dims,
                        index=0,
                        format_func=lambda x: "— Select —" if x is None else x,
                    )

                date_enabled = date_col is not None

                # Query min/max for the selected date column
                available_min = None
                available_max = None
                if date_enabled:
                    try:
                        range_df = run_query(
                            f"SELECT MIN({date_col}) AS MIN_DT, MAX({date_col}) AS MAX_DT FROM {table_fqn}"
                        )
                        if not range_df.empty:
                            available_min = range_df.iloc[0]["MIN_DT"]
                            available_max = range_df.iloc[0]["MAX_DT"]
                    except Exception:
                        pass

                with col_range:
                    range_text = f"{available_min} - {available_max}" if available_min is not None else ""
                    st.text_input("Available Range", value=range_text, disabled=True)

                # Compute default dates
                default_start = date.today() - timedelta(days=7)
                default_end = date.today()

                with col_start:
                    start_date = st.date_input(
                        "Start Date", value=default_start, disabled=not date_enabled,
                        key="start_date",
                    )

                with col_end:
                    end_date = st.date_input(
                        "End Date", value=default_end, disabled=not date_enabled,
                        key="end_date",
                    )

                # ---- Row 2: Group-By + Values & Aggregation ----
                col_grp, col_excl, col_facts, col_agg_methods = st.columns(
                    [1, 1, 1, 1], vertical_alignment="bottom"
                )

                with col_grp:
                    group_col = st.selectbox(
                        "Choose Group Criteria",
                        options=[None] + dims,
                        index=0,
                        format_func=lambda x: "— Select —" if x is None else x,
                    )

                group_enabled = group_col is not None

                with col_excl:
                    if group_enabled:
                        try:
                            distinct_df = run_query(
                                f"SELECT DISTINCT {group_col} AS VAL FROM {table_fqn} ORDER BY 1"
                            )
                            distinct_vals = distinct_df["VAL"].dropna().astype(str).tolist()
                        except Exception:
                            distinct_vals = []
                        exclude_vals = st.multiselect(
                            "Exclude Values",
                            options=distinct_vals,
                        )
                    else:
                        st.multiselect(
                            "Exclude Values", options=[], disabled=True
                        )

                with col_facts:
                    selected_facts = st.multiselect(
                        "Choose Measures",
                        options=facts,
                    )

                facts_enabled = len(selected_facts) > 0

                with col_agg_methods:
                    selected_agg = st.selectbox(
                        "Aggregation Method",
                        options=["SUM", "AVG", "COUNT", "COUNT DISTINCT", "MAX", "MEDIAN", "MIN", "MODE"],
                        disabled=not facts_enabled,
                    )
                    selected_aggs = [selected_agg] if selected_agg else []

    with col_right:
        with st.container(border=True):
            st.subheader("Configurations")

            if model:
                agg_window = st.radio(
                    "Aggregation Window",
                    options=["Daily", "Weekly", "Monthly"],
                    disabled=not date_enabled,
                )

                chart_default_idx = 1 if date_enabled and not group_enabled else 0
                chart_type = st.radio(
                    "Chart Type",
                    options=["Barchart", "Linechart", "Boxplot", "Waterfall"],
                    index=chart_default_idx,
                    disabled=not facts_enabled,
                )

                show_all = st.checkbox(
                    "Show All Available", value=False, disabled=not date_enabled
                )

                # If Show All Available, override Start/End Date to available min/max
                if date_enabled and show_all and available_min is not None:
                    try:
                        min_dt = pd.Timestamp(str(available_min)).date()
                        max_dt = pd.Timestamp(str(available_max)).date()
                        if st.session_state.get("start_date") != min_dt or st.session_state.get("end_date") != max_dt:
                            st.session_state["start_date"] = min_dt
                            st.session_state["end_date"] = max_dt
                            st.rerun()
                    except Exception:
                        pass

                add_accum = st.checkbox(
                    "Add Accumulative",
                    disabled=not facts_enabled,
                )

                include_nulls = st.checkbox(
                    "Include Null Values", value=False,
                    disabled=not facts_enabled,
                )
            else:
                st.caption("Select a semantic view to configure.")

    # ---- Generate Report Button ----
    if model:
        if st.button("Generate Report", type="primary", use_container_width=True):
            if not selected_facts or not selected_aggs:
                st.warning("Please select at least one measure and one aggregation method.")
            else:
                sql = build_query(
                    table_fqn=table_fqn,
                    date_col=date_col,
                    date_col_type=time_dim_types.get(date_col, "") if date_col else "",
                    start_date=start_date if date_enabled else None,
                    end_date=end_date if date_enabled else None,
                    group_col=group_col,
                    exclude_vals=exclude_vals if group_enabled else [],
                    selected_facts=selected_facts,
                    selected_aggs=selected_aggs,
                    agg_window=agg_window if date_enabled else "Daily",
                    include_nulls=include_nulls,
                )
                st.session_state["generated_sql"] = sql
                st.session_state["report_chart_type"] = chart_type
                st.session_state["report_group_col"] = group_col
                st.session_state["report_params"] = {
                    "table": table_fqn,
                    "measures": selected_facts,
                    "aggs": selected_aggs,
                    "date_col": date_col,
                    "start_date": str(start_date) if date_enabled else None,
                    "end_date": str(end_date) if date_enabled else None,
                    "agg_window": agg_window if date_enabled else None,
                    "group_col": group_col,
                    "exclude_vals": exclude_vals if group_enabled else [],
                    "include_nulls": include_nulls,
                    "chart_type": chart_type,
                    "show_all_available": show_all if date_enabled else False,
                    "add_accumulative": add_accum if facts_enabled else False,
                }

        # Render report from session state
        if "generated_sql" in st.session_state:
            sql = st.session_state["generated_sql"]
            with st.expander("Generated SQL", expanded=True):
                st.code(sql, language="sql")
            try:
                result_df = run_query(sql)
                st.dataframe(result_df, use_container_width=True)

                if not result_df.empty:
                    st.download_button(
                        "Download Data as CSV",
                        data=result_df.to_csv(index=False),
                        file_name="report_data.csv",
                        mime="text/csv",
                        use_container_width=True,
                        type="primary",
                    )

                # --- Chart visualization ---
                if not result_df.empty:
                    rpt_chart = st.session_state.get("report_chart_type", "Barchart")
                    rpt_group = st.session_state.get("report_group_col")
                    params = st.session_state.get("report_params", {})

                    # Build chart description
                    desc_parts = []
                    desc_parts.append(f"**{params.get('chart_type', 'Chart')}** of "
                                      f"**{', '.join(params.get('aggs', []))}** on "
                                      f"**{', '.join(params.get('measures', []))}**")
                    if params.get("group_col"):
                        desc_parts.append(f"grouped by **{params['group_col']}**")
                    if params.get("date_col"):
                        desc_parts.append(f"over **{params['date_col']}** "
                                          f"({params.get('agg_window', 'Daily')}, "
                                          f"{params.get('start_date')} to {params.get('end_date')})")
                    if params.get("exclude_vals"):
                        desc_parts.append(f"excluding **{', '.join(params['exclude_vals'])}**")
                    desc_parts.append(f"from `{params.get('table', '')}`")
                    st.markdown(" ".join(desc_parts))

                    fig = render_chart(result_df, rpt_chart, rpt_group)
                    if fig:
                        st.plotly_chart(fig, use_container_width=True)

                    # Add to Report Mart dialog
                    @st.dialog("Save Visual to Report Mart")
                    def _save_dialog():
                        params = st.session_state.get("report_params", {})
                        agg_lbl = ", ".join(params.get("aggs", [])).title()
                        meas_lbl = ", ".join(params.get("measures", [])).replace("_", " ").title()
                        default_name = f"{agg_lbl} of {meas_lbl}"
                        report_name = st.text_input("Report Name", value=default_name)
                        if st.button("Save", type="primary", use_container_width=True):
                            cleaned = re.sub(r'\s+', ' ', report_name.strip())
                            if not cleaned:
                                st.warning("Please enter a report name.")
                                return
                            try:
                                sv_fqn = f"{selected_db}.{selected_sch}.{selected_sv}"
                                params = st.session_state.get("report_params", {})
                                gen_sql = st.session_state.get("generated_sql", "")
                                user_table = save_to_report_mart(params, gen_sql, sv_fqn, cleaned)
                                st.success("Visual saved to Report Mart.")
                            except Exception as save_err:
                                st.error(f"Failed to save: {save_err}")

                    if st.button("Add Visual to Report Mart", use_container_width=True, type="primary"):
                        _save_dialog()

            except Exception as e:
                st.error(f"Query failed: {e}")

# ===== Report Mart tab =====
with tab_data_mart:
    # Show removal success/error messages
    if "rm_removed_msg" in st.session_state:
        st.success(st.session_state.pop("rm_removed_msg"))
    if "rm_removed_err" in st.session_state:
        st.error(st.session_state.pop("rm_removed_err"))

    # Load saved reports for current user
    try:
        _rm_user = _get_current_user()
        _rm_table = f"<database>.<schema>.REPORT_MART_CONFIG_{_rm_user}"
        _rm_reports = run_query(f"SELECT * FROM {_rm_table} ORDER BY REPORT_ID")
    except Exception:
        _rm_reports = pd.DataFrame()

    if _rm_reports.empty:
        st.info("No saved reports yet. Use the Self-Service tab to create and save visuals.")
    else:
        rows = list(_rm_reports.iterrows())
        for i in range(0, len(rows), 3):
            batch = rows[i:i + 3]
            cols = st.columns(3)
            for col_idx, (_, row) in enumerate(batch):
                with cols[col_idx]:
                    with st.container(border=True):
                        rpt_name = row.get("REPORT_NAME", f"Report {row['REPORT_ID']}")
                        rpt_sql = row.get("GENERATED_SQL", "")
                        rpt_chart_type = row.get("CHART_TYPE", "Barchart")
                        rpt_grp = row.get("GROUP_COL") if pd.notna(row.get("GROUP_COL")) else None

                        # Run the saved query and render chart
                        try:
                            rpt_df = run_query(rpt_sql)
                            fig = render_chart(
                                rpt_df,
                                chart_type=rpt_chart_type,
                                group_col=rpt_grp,
                                title=rpt_name,
                                height=300,
                            )
                            if fig:
                                st.plotly_chart(fig, use_container_width=True, key=f"rm_chart_{row['REPORT_ID']}")
                            else:
                                st.caption(f"**{rpt_name}**")
                                st.warning("Unable to render chart.")
                        except Exception as chart_err:
                            st.caption(f"**{rpt_name}**")
                            st.error(f"Query failed: {chart_err}")

                        # Action buttons
                        btn_cols = st.columns(4)
                        rid = row["REPORT_ID"]
                        with btn_cols[0]:
                            if st.button("Refresh", key=f"rm_refresh_{rid}", use_container_width=True):
                                # Clear cached query so next rerun fetches fresh data
                                run_query.clear()
                                st.rerun()
                        with btn_cols[1]:
                            try:
                                csv_data = rpt_df.to_csv(index=False) if not rpt_df.empty else ""
                            except Exception:
                                csv_data = ""
                            st.download_button(
                                "Download",
                                data=csv_data,
                                file_name=f"{rpt_name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                mime="text/csv",
                                key=f"rm_dl_{rid}",
                                use_container_width=True,
                            )
                        with btn_cols[2]:
                            if st.button("Query", key=f"rm_sql_{rid}", use_container_width=True):
                                st.session_state[f"rm_show_sql_{rid}"] = not st.session_state.get(f"rm_show_sql_{rid}", False)
                        with btn_cols[3]:
                            if st.button("Remove", key=f"rm_rm_{rid}", use_container_width=True):
                                st.session_state["rm_delete_rid"] = int(rid)
                                st.session_state["rm_delete_name"] = rpt_name
                                st.session_state["rm_delete_table"] = _rm_table
                                st.rerun()

                        # Show SQL toggle
                        if st.session_state.get(f"rm_show_sql_{rid}", False):
                            st.code(rpt_sql, language="sql")

        # --- Remove confirmation dialog ---
        @st.dialog("Remove Report")
        def _confirm_remove():
            del_name = st.session_state.get("rm_delete_name", "this report")
            st.warning(
                f"**\"{del_name}\"** is about to be deleted from your personal Report Mart. "
                "This visual will no longer show in this dashboard in the future."
            )
            st.markdown("Are you sure you want to proceed?")
            btn_row = st.columns(2)
            with btn_row[0]:
                if st.button("Yes, Remove", type="primary", use_container_width=True):
                    try:
                        del_rid = st.session_state["rm_delete_rid"]
                        del_table = st.session_state["rm_delete_table"]
                        _del_conn = get_connection()
                        with _del_conn.cursor() as cur:
                            # Capture row data before deleting for history log
                            cur.execute(
                                f"SELECT * FROM {del_table} WHERE REPORT_ID = %s",
                                (del_rid,),
                            )
                            _del_row = cur.fetchone()
                            _del_cols = [d[0] for d in cur.description] if cur.description else []
                            _del_data = dict(zip(_del_cols, _del_row)) if _del_row else {}
                            cur.execute(
                                f"DELETE FROM {del_table} WHERE REPORT_ID = %s",
                                (del_rid,),
                            )
                            # Log DELETE to master history
                            _log_master_history(
                                cur, action="DELETE",
                                report_id=del_rid,
                                report_name=_del_data.get("REPORT_NAME", del_name),
                                sv_fqn=_del_data.get("SEMANTIC_VIEW"),
                                table_name=_del_data.get("TABLE_NAME"),
                                time_dim=_del_data.get("TIME_DIMENSION"),
                                start_date=_del_data.get("START_DATE"),
                                end_date=_del_data.get("END_DATE"),
                                agg_window=_del_data.get("AGG_WINDOW"),
                                group_col=_del_data.get("GROUP_COL"),
                                exclude_vals=_del_data.get("EXCLUDE_VALS"),
                                measures=_del_data.get("MEASURES"),
                                aggregation=_del_data.get("AGGREGATION"),
                                include_nulls=_del_data.get("INCLUDE_NULLS"),
                                chart_type=_del_data.get("CHART_TYPE"),
                                show_all=_del_data.get("SHOW_ALL_AVAILABLE"),
                                add_accum=_del_data.get("ADD_ACCUMULATIVE"),
                                generated_sql=_del_data.get("GENERATED_SQL"),
                            )
                        run_query.clear()
                        st.session_state["rm_removed_msg"] = (
                            f"Report **\"{del_name}\"** has been removed from your personal Report Mart."
                        )
                    except Exception as del_err:
                        st.session_state["rm_removed_err"] = f"Failed to remove: {del_err}"
                    finally:
                        st.session_state.pop("rm_delete_rid", None)
                        st.session_state.pop("rm_delete_name", None)
                        st.session_state.pop("rm_delete_table", None)
                        st.rerun()
            with btn_row[1]:
                if st.button("Cancel", use_container_width=True):
                    st.session_state.pop("rm_delete_rid", None)
                    st.session_state.pop("rm_delete_name", None)
                    st.session_state.pop("rm_delete_table", None)
                    st.rerun()

        if "rm_delete_rid" in st.session_state:
            _confirm_remove()

# ===== Cortex Agent tab =====
with tab_cortex:

    # --- Header ---
    st.subheader(":material/smart_toy: Cortex Agent")
    st.caption(
        "Ask questions about your data in natural language. "
        "Powered by Cortex Analyst using the semantic view you selected above."
    )

    # --- Semantic view FQN for this tab ---
    agent_sv_fqn = None
    try:
        if selected_sv and selected_db and selected_sch:
            agent_sv_fqn = f"{selected_db}.{selected_sch}.{selected_sv}"
    except NameError:
        pass

    # Clear chat history when semantic view changes
    if agent_sv_fqn != st.session_state.get("agent_sv_fqn_prev"):
        st.session_state["agent_messages"] = []
        st.session_state["agent_display"] = []
        st.session_state["agent_sv_fqn_prev"] = agent_sv_fqn

    if agent_sv_fqn:
        st.caption(f"Using semantic view: `{agent_sv_fqn}`")

    # --- Session state for chat ---
    if "agent_messages" not in st.session_state:
        st.session_state["agent_messages"] = []  # API message history
    if "agent_display" not in st.session_state:
        st.session_state["agent_display"] = []   # UI display history

    # --- Example questions as suggestion pills (from verified queries) ---
    _PILL_STYLES = [
        ":blue[:material/analytics:]",
        ":green[:material/trending_up:]",
        ":orange[:material/calendar_month:]",
        ":red[:material/bar_chart:]",
        ":violet[:material/compare:]",
    ]

    EXAMPLE_QUESTIONS = {}
    try:
        if model:
            vqrs = model.get("verified_queries", [])
            # Only show onboarding-flagged VQRs (fastest responses)
            onboarding = [vq for vq in vqrs if vq.get("use_as_onboarding_question")]
            if not onboarding:
                onboarding = vqrs[:5]
            for i, vq in enumerate(onboarding):
                q = vq.get("question", "")
                name = vq.get("name", f"vqr_{i}")
                if q:
                    icon = _PILL_STYLES[i % len(_PILL_STYLES)]
                    label = name.replace("_", " ").title()
                    EXAMPLE_QUESTIONS[f"{icon} {label}"] = q
    except NameError:
        pass

    if not EXAMPLE_QUESTIONS:
        EXAMPLE_QUESTIONS = {
            ":blue[:material/analytics:] Summarize by category":
                "What are the totals for each category?",
            ":green[:material/trending_up:] Top 10 records":
                "What are the top 10 records by the primary measure?",
            ":orange[:material/calendar_month:] Trends over time":
                "How have the key metrics changed over time by month?",
        }

    if not st.session_state["agent_display"]:
        selected_pill = st.pills(
            "Try asking:",
            list(EXAMPLE_QUESTIONS.keys()),
            label_visibility="collapsed",
        )
        if selected_pill and agent_sv_fqn:
            prompt = EXAMPLE_QUESTIONS[selected_pill]
            st.session_state["agent_messages"].append({"role": "user", "content": [{"type": "text", "text": prompt}]})
            st.session_state["agent_display"].append({"role": "user", "content": prompt})
            st.rerun()

    # --- Display chat history ---
    for entry in st.session_state["agent_display"]:
        with st.chat_message(entry["role"]):
            st.markdown(entry["content"])
            if entry.get("sql"):
                with st.expander("Generated SQL", expanded=False):
                    st.code(entry["sql"], language="sql")
            if entry.get("df") is not None and not entry["df"].empty:
                st.dataframe(entry["df"], use_container_width=True)
                # Auto-chart: if 2+ columns, plot first string col as x, first numeric as y
                df = entry["df"]
                str_cols = [c for c in df.columns if df[c].dtype == "object"]
                num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
                if str_cols and num_cols:
                    x_c = str_cols[0]
                    y_c = num_cols[0]
                    chart_df = df.head(30)  # limit for readability
                    for nc in num_cols:
                        chart_df[nc] = pd.to_numeric(chart_df[nc], errors="coerce")
                    fig = px.bar(chart_df, x=x_c, y=y_c, title=f"{y_c} by {x_c}")
                    fig.update_layout(xaxis_type="category")
                    fig.update_yaxes(tickformat=",", type="linear")
                    st.plotly_chart(fig, use_container_width=True)

    # --- Chat input ---
    if not agent_sv_fqn:
        st.info("Select a semantic view in the Self-Service tab to enable the agent.")
    else:
        # Check if there's a pending message that needs a response
        # (e.g. from auto-explore or pill selection)
        _pending_prompt = None
        if (st.session_state["agent_messages"]
                and st.session_state["agent_messages"][-1].get("role") == "user"
                and (not st.session_state["agent_display"]
                     or st.session_state["agent_display"][-1].get("role") == "user")):
            _pending_prompt = True

        if user_input := st.chat_input("Ask a question about your data..."):
            # Append user message
            st.session_state["agent_messages"].append({"role": "user", "content": [{"type": "text", "text": user_input}]})
            st.session_state["agent_display"].append({"role": "user", "content": user_input})
            _pending_prompt = True

            with st.chat_message("user"):
                st.markdown(user_input)

        if _pending_prompt:

            # Call Cortex Analyst
            with st.chat_message("assistant"):
                _stopped = False
                try:
                    # --- Phase 1: call Analyst (show status while waiting) ---
                    with st.status("Generating answer...", expanded=False) as _status:
                        _recent = st.session_state["agent_messages"][-10:]
                        resp = send_analyst_message(agent_sv_fqn, _recent)

                        message = resp.get("message", {})
                        content_items = message.get("content", [])

                        display_text = ""
                        result_sql = None
                        for item in content_items:
                            if item.get("type") == "text":
                                display_text += item.get("text", "") + "\n"
                            elif item.get("type") == "sql":
                                result_sql = item.get("statement", "")
                        _status.update(label="Answer ready", state="complete")

                    # --- Stop button: let user cancel before query execution ---
                    if result_sql:
                        if st.button("Stop", key="agent_stop", icon=":material/stop:", type="secondary"):
                            _stopped = True
                            display_text = display_text or ""
                            display_text += "\n\n*Stopped by user before query execution.*"

                    # --- Phase 2: stream text to screen word-by-word ---
                    if display_text.strip():
                        _words = display_text.strip().split()
                        def _word_stream():
                            for w in _words:
                                yield w + " "
                        st.write_stream(_word_stream())

                    # --- Phase 3: execute SQL and render results (skip if stopped) ---
                    result_df = None
                    if result_sql and not _stopped:
                        with st.expander("Generated SQL", expanded=False):
                            st.code(result_sql, language="sql")
                        # Use longer cache if SQL matches a verified query
                        _vqr_sqls = set()
                        try:
                            if model:
                                _vqr_sqls = get_vqr_sqls(model)
                        except NameError:
                            pass
                        _is_vqr = result_sql.strip().upper() in _vqr_sqls
                        _query_fn = run_query_cached if _is_vqr else run_query
                        with st.spinner("Running query..."):
                            try:
                                result_df = _query_fn(result_sql)
                                st.dataframe(result_df, use_container_width=True)

                                # Auto-chart
                                str_cols = [c for c in result_df.columns if result_df[c].dtype == "object"]
                                num_cols = [c for c in result_df.columns if pd.api.types.is_numeric_dtype(result_df[c])]
                                if str_cols and num_cols:
                                    x_c = str_cols[0]
                                    y_c = num_cols[0]
                                    chart_df = result_df.head(30)
                                    for nc in num_cols:
                                        chart_df[nc] = pd.to_numeric(chart_df[nc], errors="coerce")
                                    fig = px.bar(chart_df, x=x_c, y=y_c, title=f"{y_c} by {x_c}")
                                    fig.update_layout(xaxis_type="category")
                                    fig.update_yaxes(tickformat=",", type="linear")
                                    st.plotly_chart(fig, use_container_width=True)
                            except Exception as qe:
                                st.error(f"Query execution failed: {qe}")

                    if not display_text.strip() and not result_sql:
                        display_text = "I wasn't able to generate a response. Please try rephrasing your question."
                        st.markdown(display_text)

                    # Save to display history
                    st.session_state["agent_display"].append({
                        "role": "assistant",
                        "content": display_text.strip() or "Here are the results:",
                        "sql": result_sql,
                        "df": result_df,
                    })

                    # Save to API history
                    st.session_state["agent_messages"].append(message)

                except Exception as e:
                    error_msg = f"Cortex Analyst error: {e}"
                    st.error(error_msg)
                    st.session_state["agent_display"].append({
                        "role": "assistant",
                        "content": error_msg,
                    })

    # --- Clear chat button ---
    if st.session_state["agent_display"]:
        if st.button("Clear conversation", icon=":material/delete:", use_container_width=True):
            st.session_state["agent_messages"] = []
            st.session_state["agent_display"] = []
            st.rerun()

