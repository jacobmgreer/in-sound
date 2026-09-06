# Updated app module: delayed shiny import + syntax fixes
# Usage:
# - In Pyodide / shinylive: await pyodide.runPythonAsync("import app; APP = await app.build_app()")
# - In a normal Python environment (server with shiny installed): import asyncio; APP = asyncio.run(build_app())

import os
import sys
import pathlib
import urllib.request
import duckdb
import pandas as pd
import asyncio

# =============================================================================
# CONFIGURATION & PATHS
# =============================================================================

IS_SHINYLIVE = "emscripten" in sys.platform.lower() or bool(os.getenv("IN_SHINYLIVE"))
DATA_DIR = "data" if not IS_SHINYLIVE else "."

N_PARQUET_FILES = 10
PARQUET_BASE_URL = "https://huggingface.co/datasets/jacobmgreer/in-sound/resolve/main"

TYPE_CONTENT = 1
TYPE_CREATOR = 2
FILTER_DEBOUNCE_MS = 400
MACRO_DIR = "macros"

# Global engine state containers
DECADE_BITS = {}
ORIGIN_BITS = {}
GRAPH_BITS = {}
SOURCE_ID_TO_NAME = {}
ENTITY_SCHEMA = {}
FILTER_CHOICES = {
    "ok": False,
    "decades": [],
    "sources": {},
    "source_ids": [],
    "origins": [],
}
NC_CON = None
FETCH_LOG = []


def log_fetch(msg: str):
    FETCH_LOG.append(msg)
    print(f"[nitrate-dash] {msg}")


def get_parquet_paths():
    return [
        os.path.join(DATA_DIR, f"big_sound_{i}.parquet")
        for i in range(N_PARQUET_FILES)
    ]


def check_missing_files():
    return [p for p in get_parquet_paths() if not os.path.exists(p)]


def download_shards():
    os.makedirs(DATA_DIR, exist_ok=True)
    for i in range(N_PARQUET_FILES):
        filename = f"big_sound_{i}.parquet"
        dest = os.path.join(DATA_DIR, filename)
        if os.path.exists(dest):
            continue
        url = f"{PARQUET_BASE_URL}/{filename}"
        log_fetch(f"Fetching {filename}...")
        try:
            urllib.request.urlretrieve(url, dest)
        except Exception as e:
            log_fetch(f"Error fetching {filename}: {e}")


# =============================================================================
# DUCKDB ENGINE INITIALIZATION
# =============================================================================


def open_con():
    con = duckdb.connect(database=":memory:", read_only=False)
    if not IS_SHINYLIVE:
        try:
            con.execute("PRAGMA threads=6;")
            con.execute("PRAGMA memory_limit='4GB';")
        except Exception:
            # Older DuckDB builds may not support PRAGMA memory_limit; ignore if not supported
            pass
    con.execute("SET preserve_insertion_order = false;")
    return con


def register_all_macros(con):
    if not os.path.exists(MACRO_DIR):
        return
    for macro_file in pathlib.Path(MACRO_DIR).glob("*.sql"):
        sql_text = macro_file.read_text(encoding="utf-8")
        if sql_text.strip():
            con.execute(sql_text)


def load_bits_from_macro(con, macro_name):
    # macro_name should be a macro returning (bit, value)
    df = con.execute(f"SELECT bit, value FROM {macro_name}() ORDER BY bit DESC").fetchdf()
    if df.empty:
        return {}
    return {str(row.value): int(row.bit) for row in df.itertuples(index=False)}


def resolve_export_schema(schema_df):
    cols = schema_df["column_name"].tolist()

    def pick_col(candidates, label):
        for c in candidates:
            if c in cols:
                return c
        raise RuntimeError(
            f"Could not find {label}. Tried: {', '.join(candidates)}."
        )

    source_col = pick_col(["source"], "entity source column")
    source_type = schema_df.loc[
        schema_df["column_name"] == source_col, "column_type"
    ].values[0]

    return {
        "id": pick_col(["big"], "entity ID column"),
        "type": pick_col(["type"], "entity type column"),
        "decades": pick_col(["decades"], "entity decade bitmask"),
        "source": source_col,
        "source_is_int": any(
            t in source_type.lower() for t in ["int", "serial"]
        ),
        "origins": pick_col(["origins"], "entity origin bitmask"),
        "graph_col": pick_col(["comp"], "entity graph bitmask"),
    }


def create_dimension_tables(con):
    # Create dim_decade
    if DECADE_BITS:
        decade_vals = [f"({bit}, {val})" for val, bit in DECADE_BITS.items()]
        con.execute(
            f"CREATE OR REPLACE TABLE dim_decade AS SELECT bit, value FROM (VALUES {', '.join(decade_vals)}) AS t(bit, value)"
        )
    else:
        # empty table with same column names
        con.execute(
            "CREATE OR REPLACE TABLE dim_decade AS SELECT CAST(NULL AS BIGINT) AS bit, CAST(NULL AS VARCHAR) AS value LIMIT 0"
        )

    # Origin dimension table
    if ORIGIN_BITS:
        origin_vals = [f"({bit}, '{val}')" for val, bit in ORIGIN_BITS.items()]
        con.execute(
            f"CREATE OR REPLACE TABLE dim_origin AS SELECT bit, value FROM (VALUES {', '.join(origin_vals)}) AS t(bit, value)"
        )
    else:
        con.execute(
            "CREATE OR REPLACE TABLE dim_origin AS SELECT CAST(NULL AS BIGINT) AS bit, CAST(NULL AS VARCHAR) AS value LIMIT 0"
        )

    # Source dimension table
    if SOURCE_ID_TO_NAME:
        source_vals = [f"({bit}, '{val}')" for val, bit in SOURCE_ID_TO_NAME.items()]
        con.execute(
            f"CREATE OR REPLACE TABLE dim_source AS SELECT bit, value FROM (VALUES {', '.join(source_vals)}) AS t(bit, value)"
        )
    else:
        con.execute(
            "CREATE OR REPLACE TABLE dim_source AS SELECT CAST(NULL AS BIGINT) AS bit, CAST(NULL AS VARCHAR) AS value LIMIT 0"
        )


def bit_is_set_join_sql(bitmask_col, alias=None):
    col = f"{alias}.{bitmask_col}" if alias else bitmask_col
    return f"(COALESCE({col}, 0)::BIGINT & (1::BIGINT << d.bit)) <> 0"


def load_filter_choices(con):
    if len(check_missing_files()) > 0:
        return {
            "ok": False,
            "decades": [],
            "sources": {},
            "source_ids": [],
            "origins": [],
        }

    decades_df = con.execute(
        f"SELECT DISTINCT d.value FROM nc_data p JOIN dim_origin d ON {bit_is_set_join_sql(ENTITY_SCHEMA['decades'], 'p')} ORDER BY value"
    ).fetchdf()

    origins_df = con.execute(
        f"SELECT DISTINCT d.value FROM nc_data p JOIN dim_origin d ON {bit_is_set_join_sql(ENTITY_SCHEMA['origins'], 'p')} ORDER BY value"
    ).fetchdf()

    source_col = ENTITY_SCHEMA["source"]
    sources_df = con.execute(
        f"""
        SELECT DISTINCT p.{source_col} AS source_id,
               COALESCE(d.value, 'Source ' || CAST(p.{source_col} AS VARCHAR)) AS source
        FROM nc_data p
        LEFT JOIN dim_source d ON p.{source_col} = d.bit
        WHERE p.{source_col} IS NOT NULL
        ORDER BY source
    """
    ).fetchdf()

    source_ids = (
        sources_df["source_id"].astype(int).tolist()
        if not sources_df.empty
        else []
    )
    source_labels = (
        sources_df["source"].astype(str).tolist() if not sources_df.empty else []
    )
    sources_dict = dict(zip(source_labels, source_ids))

    return {
        "ok": True,
        "decades": (
            sorted(decades_df["value"].astype(float).tolist())
            if not decades_df.empty
            else list(DECADE_BITS.values())
        ),
        "sources": sources_dict,
        "source_ids": source_ids,
        "origins": (
            origins_df["value"].astype(str).tolist()
            if not origins_df.empty
            else list(ORIGIN_BITS.keys())
        ),
    }


def init_engine():
    global DECADE_BITS, ORIGIN_BITS, GRAPH_BITS, SOURCE_ID_TO_NAME, ENTITY_SCHEMA, FILTER_CHOICES, NC_CON

    schema_con = open_con()
    register_all_macros(schema_con)

    DECADE_BITS = load_bits_from_macro(schema_con, "get_decade_mapping")
    ORIGIN_BITS = load_bits_from_macro(schema_con, "get_origin_mapping")
    GRAPH_BITS = load_bits_from_macro(schema_con, "get_comp_mapping")
    SOURCE_ID_TO_NAME = load_bits_from_macro(schema_con, "get_source_mapping")

    first_file = get_parquet_paths()[0]
    schema_df = schema_con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{first_file}')"
    ).fetchdf()
    ENTITY_SCHEMA = resolve_export_schema(schema_df)
    schema_con.close()

    con = open_con()
    create_dimension_tables(con)

    file_list = ", ".join([f"'{f}'" for f in get_parquet_paths()])
    log_fetch(f"Loading {N_PARQUET_FILES} parquet shards into DuckDB cache...")
    con.execute(
        f"CREATE TABLE nc_data AS SELECT * FROM read_parquet([{file_list}])"
    )

    FILTER_CHOICES = load_filter_choices(con)

    if IS_SHINYLIVE:
        for f in get_parquet_paths():
            if os.path.exists(f):
                os.remove(f)

    NC_CON = con
    return True


# =============================================================================
# UI + SERVER BUILD (DEFERRED until shiny is available)
# =============================================================================


async def build_app():
    """
    Ensure 'shiny' is available in the runtime, then build and return the App instance.
    This is async because installing via micropip in Pyodide is async.
    """
    try:
        # Try importing directly first (normal Python server where shiny is installed)
        import shiny  # type: ignore
    except ModuleNotFoundError:
        # Try to install using micropip if available (Pyodide)
        try:
            import micropip  # type: ignore
            await micropip.install("shiny")
        except Exception as e:
            raise RuntimeError(
                "shiny is not installed and automatic installation via micropip failed."
                f" Install shiny in the environment or ensure pyodide.loadPackage('shiny') was called. ({e})"
            )
    # Now import the actual shiny symbols
    from shiny import App, reactive, render, ui

    # --- UI LAYOUT ---
    navbar_css = """
    .navbar {
      background-color: #c3cdd7 !important;
      border: none !important;
      border-bottom: 2px solid !important;
      box-shadow: none !important;
      padding: 20px;
    }
    """

    app_ui = ui.page_navbar(
        ui.nav_spacer(),
        title="Connected Components",
        theme=ui.Theme(bootswatch="brite"),
        header=ui.tags.head(ui.tags.style(navbar_css)),
        sidebar=ui.sidebar(
            ui.output_ui("sidebar_filters"),
            width=290,
        ),
        ui.nav_panel(
            "By Source",
            ui.output_ui("config_warning"),
            ui.h6(
                "Content",
                class_="text-muted",
                style="font-size:2em; margin:4px 0 0 0;",
            ),
            ui.hr(style="margin:16px;border:0;"),
            ui.output_ui("content_overview_metrics"),
            ui.output_ui("by_content_source_header"),
            ui.output_data_frame("tbl_by_content_source"),
            ui.hr(style="margin:16px;border:0;"),
            ui.h6(
                "Creator",
                class_="text-muted",
                style="font-size:2em; margin:4px 0 0 0;",
            ),
            ui.hr(style="margin:16px;border:0;"),
            ui.output_ui("creator_overview_metrics"),
            ui.output_ui("by_creator_source_header"),
            ui.output_data_frame("tbl_by_creator_source"),
        ),
        ui.nav_panel(
            "By Decade",
            ui.h6(
                "Content",
                class_="text-muted",
                style="font-size:2em; margin:4px 0 0 0;",
            ),
            ui.hr(style="margin:16px;border:0;"),
            ui.h5("Content Records by Decade"),
            ui.output_data_frame("tbl_content_decade"),
            ui.h6(
                "Creator",
                class_="text-muted",
                style="font-size:2em; margin:20px 0 0 0;",
            ),
            ui.hr(style="margin:16px;border:0;"),
            ui.h5("Creator Records by Associated Content Decade(s)"),
            ui.output_data_frame("tbl_creator_decade"),
        ),
        ui.nav_panel(
            "By Origin",
            ui.h6(
                "Content",
                class_="text-muted",
                style="font-size:2em; margin:4px 0 0 0;",
            ),
            ui.hr(style="margin:16px;border:0;"),
            ui.h5("Content Records by Origin(s)"),
            ui.output_data_frame("tbl_content_origin"),
            ui.h6(
                "Creator",
                class_="text-muted",
                style="font-size:2em; margin:20px 0 0 0;",
            ),
            ui.hr(style="margin:16px;border:0;"),
            ui.h5("Creator Records by Associated Content Origin(s)"),
            ui.output_data_frame("tbl_creator_origin"),
        ),
    )

    # --- SERVER LOGIC ---
    def server(input, output, session):
        data_status = reactive.Value("pending")

        @reactive.effect
        def _initialize_data():
            if len(check_missing_files()) > 0 and IS_SHINYLIVE:
                try:
                    download_shards()
                except Exception as e:
                    log_fetch(f"Download error: {e}")

            if len(check_missing_files()) == 0:
                try:
                    init_engine()
                    data_status.set("ok")
                except Exception as e:
                    log_fetch(f"Engine init failed: {e}")
                    data_status.set("failed")
            else:
                data_status.set("failed")

        @output
        @ui.render.ui
        def sidebar_file_controls():
            return ui.div()  # placeholder; implement file controls if desired

        @output
        @ui.render.ui
        def sidebar_filters():
            req_status = data_status() == "ok"
            if not req_status:
                return ui.div()

            decades_choices = {"": "All Decades"}
            for d in FILTER_CHOICES["decades"]:
                decades_choices[str(int(d))] = f"{int(d)}s"

            return ui.tagList(
                ui.h6("Cross-Filters"),
                ui.hr(style="margin:6px 0"),
                ui.input_select(
                    "filter_decade_from", "From Decade", choices=decades_choices, selected=""
                ),
                ui.input_select(
                    "filter_decade_to", "To Decade", choices=decades_choices, selected=""
                ),
                ui.input_selectize(
                    "filter_origins",
                    "Origin(s)",
                    choices={o: o for o in FILTER_CHOICES["origins"]},
                    selected=None,
                    multiple=True,
                    options={"placeholder": "All"},
                ),
                ui.hr(style="margin:6px 0"),
                ui.h6("Source"),
                ui.input_checkbox_group(
                    "filter_sources",
                    label=None,
                    choices={str(v): k for k, v in FILTER_CHOICES["sources"].items()},
                    selected=[str(x) for x in FILTER_CHOICES["source_ids"]],
                ),
            )

        @output
        @ui.render.ui
        def config_warning():
            status = data_status()
            if status == "pending":
                return ui.div(
                    {"class": "alert alert-info"},
                    ui.tags.strong("Loading parquet data... "),
                    "this only happens once per visit.",
                )
            if status == "ok":
                return None
            missing = check_missing_files()
            return ui.div(
                {"class": "alert alert-warning"},
                ui.tags.strong("Missing parquet files: "),
                ui.tags.code(", ".join(missing)),
            )

        def selected_bitmask_sql(values, bits_map):
            valid = [v for v in values if v is not None and str(v) != ""]
            if not valid:
                return None
            positions = [bits_map[str(v)] for v in valid if str(v) in bits_map]
            if not positions:
                return None
            total = sum(2 ** int(p) for p in positions)
            return f"{total:.0f}"

        def build_bitmask_clause(values, bitmask_col, bits_map):
            mask_sql = selected_bitmask_sql(values, bits_map)
            if not mask_sql:
                return ""
            return f"AND ((COALESCE({bitmask_col}, 0)::BIGINT & {mask_sql}::BIGINT) <> 0)"

        def get_decade_values():
            f_from = input.filter_decade_from()
            f_to = input.filter_decade_to()
            all_dec = FILTER_CHOICES["decades"]
            if not f_from or not f_to:
                return all_dec
            try:
                frm, to = int(f_from), int(f_to)
                if frm > to:
                    frm, to = to, frm
                return [d for d in all_dec if frm <= d <= to]
            except ValueError:
                return all_dec

        def shared_filter_clauses():
            clauses = []
            # Source clause
            selected_src = input.filter_sources()
            if selected_src is not None:
                if not selected_src:
                    clauses.append("AND 1=0")
                else:
                    src_ints = [str(int(s)) for s in selected_src]
                    if len(src_ints) < len(FILTER_CHOICES["source_ids"]):
                        clauses.append(
                            f"AND {ENTITY_SCHEMA['source']} IN ({', '.join(src_ints)})"
                        )

            # Decade clause
            dec_vals = get_decade_values()
            dec_mask = selected_bitmask_sql(dec_vals, DECADE_BITS)
            if dec_mask:
                clauses.append(
                    f"AND ((COALESCE({ENTITY_SCHEMA['decades']}, 0)::BIGINT & {dec_mask}::BIGINT) <> 0)"
                )

            # Origin clause
            orig_vals = input.filter_origins()
            if orig_vals:
                orig_mask = selected_bitmask_sql(orig_vals, ORIGIN_BITS)
                if orig_mask:
                    clauses.append(
                        f"AND ((COALESCE({ENTITY_SCHEMA['origins']}, 0)::BIGINT & {orig_mask}::BIGINT) <> 0)"
                    )

            return clauses

        @reactive.Calc
        def inp_snap():
            return shared_filter_clauses()

        def run_query(sql):
            if data_status() != "ok" or NC_CON is None:
                return pd.DataFrame()
            try:
                return NC_CON.execute(sql).fetchdf()
            except Exception as e:
                print(f"[nitrate-dash query error] {e}")
                return pd.DataFrame()

        def graph_match_expr(graph_name, alias=None):
            prefix = f"{alias}." if alias else ""
            bit_idx = GRAPH_BITS[graph_name]
            return f"((COALESCE({prefix}{ENTITY_SCHEMA['graph_col']}, 0)::BIGINT & (1::BIGINT << {bit_idx})) <> 0)"

        @reactive.Calc
        def overview_data():
            clauses = " ".join(inp_snap())
            id_col = ENTITY_SCHEMA["id"]
            type_col = ENTITY_SCHEMA["type"]
            base_expr = graph_match_expr("base")
            clean_expr = graph_match_expr("clean")
            disc_expr = graph_match_expr("discovery")

            sql = f"""
                WITH filtered AS (
                    SELECT {id_col}, {type_col}, {ENTITY_SCHEMA['graph_col']}
                    FROM nc_data
                    WHERE 1=1 {clauses}
                )
                SELECT
                    COUNT(DISTINCT {id_col}) FILTER (WHERE {type_col} = 1) AS total_content,
                    COUNT(DISTINCT {id_col}) FILTER (WHERE {type_col} = 1 AND {base_expr}) AS base_content,
                    COUNT(DISTINCT {id_col}) FILTER (WHERE {type_col} = 1 AND {clean_expr}) AS clean_content,
                    COUNT(DISTINCT {id_col}) FILTER (WHERE {type_col} = 1 AND {disc_expr}) AS disc_content,
                    COUNT(DISTINCT {id_col}) FILTER (WHERE {type_col} = 2) AS total_creator,
                    COUNT(DISTINCT {id_col}) FILTER (WHERE {type_col} = 2 AND {base_expr}) AS base_creator,
                    COUNT(DISTINCT {id_col}) FILTER (WHERE {type_col} = 2 AND {clean_expr}) AS clean_creator,
                    COUNT(DISTINCT {id_col}) FILTER (WHERE {type_col} = 2 AND {disc_expr}) AS disc_creator
                FROM filtered
            """
            return run_query(sql)

        def ov_card(swatch_color, title, subtitle, matched_n, matched_denom, unmatched_n):
            pct_str = (
                f"{100 * matched_n / matched_denom:.1f}%"
                if matched_denom and matched_denom > 0
                else "—"
            )
            return ui.div(
                {"class": "card h-100"},
                ui.div(
                    {"class": "card-body py-2 px-3"},
                    ui.div(
                        {"class": "d-flex align-items-center mb-1"},
                        ui.div(
                            {
                                "style": f"width:12px;height:12px;border-radius:2px;background:{swatch_color};margin-right:6px;flex-shrink:0;"
                            }
                        ),
                        ui.strong(title),
                    ),
                    ui.div(
                        {"class": "text-muted", "style": "font-size:0.75rem; margin-bottom:4px;"},
                        subtitle,
                    ),
                    ui.h4({"class": "mb-1"}, pct_str),
                    ui.div({"style": "font-size:0.82rem;"}, f"{matched_n:,} connected"),
                    ui.div(
                        {"style": "font-size:0.82rem;color:#000000;"},
                        f"{unmatched_n:,} unmatched",
                    ),
                ),
            )

        @output
        @ui.render.ui
        def content_overview_metrics():
            df = overview_data()
            if df.empty or df.isna().all().all():
                return ui.p({"class": "text-muted"}, "No data for selected filters.")
            r = df.iloc[0]
            return ui.layout_column_wrap(
                1,
                3,
                ov_card("#db3f7d", "Base", "Source Graph", r.base_content, r.total_content, r.total_content - r.base_content),
                ov_card("#0ec56a", "Clean", "Cleaned Graph", r.clean_content, r.total_content, r.total_content - r.clean_content),
                ov_card("#3885d3", "Discovery", "Proposed Graph", r.disc_content, r.total_content, r.total_content - r.disc_content),
            )

        @output
        @ui.render.ui
        def creator_overview_metrics():
            df = overview_data()
            if df.empty or df.isna().all().all():
                return ui.p({"class": "text-muted"}, "No data for selected filters.")
            r = df.iloc[0]
            return ui.layout_column_wrap(
                1,
                3,
                ov_card("#db3f7d", "Base", "Source Graph", r.base_creator, r.total_creator, r.total_creator - r.base_creator),
                ov_card("#0ec56a", "Clean", "Cleaned Graph", r.clean_creator, r.total_creator, r.total_creator - r.clean_creator),
                ov_card("#3885d3", "Discovery", "Proposed Graph", r.disc_creator, r.total_creator, r.total_creator - r.disc_creator),
            )

        def query_by_source_func(entity_type_val, entity_name):
            clauses = " ".join(inp_snap())
            id_col = ENTITY_SCHEMA["id"]
            type_col = ENTITY_SCHEMA["type"]
            total_col = f"total_{entity_name}"
            source_col = ENTITY_SCHEMA["source"]

            base_expr = graph_match_expr("base", "f")
            clean_expr = graph_match_expr("clean", "f")
            disc_expr = graph_match_expr("discovery", "f")

            sql = f"""
                WITH filtered_{entity_name} AS (
                    SELECT {id_col}, {type_col}, {source_col}, {ENTITY_SCHEMA['graph_col']}
                    FROM nc_data
                    WHERE {type_col} = {entity_type_val} {clauses}
                )
                SELECT
                    COALESCE(s.value, 'Source ' || CAST(f.{source_col} AS VARCHAR)) AS source,
                    COUNT(DISTINCT f.{id_col}) AS {total_col},
                    COUNT(DISTINCT CASE WHEN {base_expr} THEN f.{id_col} END) AS base_matched,
                    COUNT(DISTINCT CASE WHEN {clean_expr} THEN f.{id_col} END) AS clean_matched,
                    COUNT(DISTINCT CASE WHEN {disc_expr} THEN f.{id_col} END) AS disc_matched,
                    COUNT(DISTINCT CASE WHEN NOT ({disc_expr}) THEN f.{id_col} END) AS unmatched
                FROM filtered_{entity_name} f
                LEFT JOIN dim_source s ON f.{source_col} = s.bit
                GROUP BY 1
                ORDER BY {total_col} DESC
            """
            df = run_query(sql)
            if not df.empty:
                df["pct_base"] = (df["base_matched"] / df[total_col]).apply(
                    lambda x: f"{100*x:.1f}%" if pd.notnull(x) else "—"
                )
                df["pct_clean"] = (df["clean_matched"] / df[total_col]).apply(
                    lambda x: f"{100*x:.1f}%" if pd.notnull(x) else "—"
                )
                df["pct_disc"] = (df["disc_matched"] / df[total_col]).apply(
                    lambda x: f"{100*x:.1f}%" if pd.notnull(x) else "—"
                )
            return df

        @output
        @ui.render.ui
        def by_content_source_header():
            df = query_by_source_func(TYPE_CONTENT, "content")
            if df.empty:
                return None
            return ui.tagList(ui.br(), ui.h5("By Content ID Source"))

        @output
        @ui.render.data_frame
        def tbl_by_content_source():
            df = query_by_source_func(TYPE_CONTENT, "content")
            return render.DataGrid(df, filters=False)

        @output
        @ui.render.ui
        def by_creator_source_header():
            df = query_by_source_func(TYPE_CREATOR, "creator")
            if df.empty:
                return None
            return ui.tagList(ui.br(), ui.h5("By Creator ID Source"))

        @output
        @ui.render.data_frame
        def tbl_by_creator_source():
            df = query_by_source_func(TYPE_CREATOR, "creator")
            return render.DataGrid(df, filters=False)

        def query_by_decade_func(entity_type_val, entity_name):
            clauses = " ".join(inp_snap())
            id_col = ENTITY_SCHEMA["id"]
            type_col = ENTITY_SCHEMA["type"]
            total_col = f"total_{entity_name}"
            dec_vals = [str(int(d)) for d in get_decade_values()]
            if not dec_vals:
                return pd.DataFrame()

            values_clause = ", ".join([f"({d})" for d in dec_vals])
            base_expr = graph_match_expr("base", "f")
            clean_expr = graph_match_expr("clean", "f")
            disc_expr = graph_match_expr("discovery", "f")

            sql = f"""
                WITH filtered_{entity_name} AS (
                    SELECT {id_col}, {type_col}, {ENTITY_SCHEMA['decades']}, {ENTITY_SCHEMA['graph_col']}
                    FROM nc_data
                    WHERE {type_col} = {entity_type_val} {clauses}
                ),
                all_dec AS (SELECT decade::INTEGER FROM (VALUES {values_clause}) t(decade),
                dec_exp AS (
                    SELECT
                        d.value::INTEGER AS decade,
                        f.{id_col} AS entity_id,
                        {base_expr} AS is_base,
                        {clean_expr} AS is_clean,
                        {disc_expr} AS is_discovery
                    FROM filtered_{entity_name} f
                    JOIN dim_decade d ON {bit_is_set_join_sql(ENTITY_SCHEMA['decades'], 'f')}
                )
                SELECT
                    ad.decade,
                    COUNT(DISTINCT de.entity_id) AS {total_col},
                    COUNT(DISTINCT CASE WHEN de.is_base THEN de.entity_id END) AS base_matched,
                    COUNT(DISTINCT CASE WHEN de.is_clean THEN de.entity_id END) AS clean_matched,
                    COUNT(DISTINCT CASE WHEN de.is_discovery THEN de.entity_id END) AS disc_matched,
                    COUNT(DISTINCT CASE WHEN NOT de.is_discovery THEN de.entity_id END) AS unmatched
                FROM all_dec ad
                LEFT JOIN dec_exp de USING (decade)
                GROUP BY ad.decade
                ORDER BY ad.decade
            """
            df = run_query(sql)
            if not df.empty:
                df["decade"] = df["decade"].astype(str) + "s"
                df["pct_base"] = (df["base_matched"] / df[total_col]).apply(
                    lambda x: f"{100*x:.1f}%" if pd.notnull(x) and total_col else "—"
                )
                df["pct_clean"] = (df["clean_matched"] / df[total_col]).apply(
                    lambda x: f"{100*x:.1f}%" if pd.notnull(x) else "—"
                )
                df["pct_disc"] = (df["disc_matched"] / df[total_col]).apply(
                    lambda x: f"{100*x:.1f}%" if pd.notnull(x) else "—"
                )
            return df

        @output
        @ui.render.data_frame
        def tbl_content_decade():
            return render.DataGrid(query_by_decade_func(TYPE_CONTENT, "content"), filters=False)

        @output
        @ui.render.data_frame
        def tbl_creator_decade():
            return render.DataGrid(query_by_decade_func(TYPE_CREATOR, "creator"), filters=False)

        def query_by_origin_func(entity_type_val, entity_name):
            clauses = " ".join(inp_snap())
            id_col = ENTITY_SCHEMA["id"]
            type_col = ENTITY_SCHEMA["type"]
            total_col = f"total_{entity_name}"
            base_expr = graph_match_expr("base", "f")
            clean_expr = graph_match_expr("clean", "f")
            disc_expr = graph_match_expr("discovery", "f")

            sql = f"""
                WITH filtered_{entity_name} AS (
                    SELECT {id_col}, {type_col}, {ENTITY_SCHEMA['origins']}, {ENTITY_SCHEMA['graph_col']}
                    FROM nc_data
                    WHERE {type_col} = {entity_type_val} {clauses}
                )
                SELECT
                    d.value AS grouping,
                    COUNT(DISTINCT f.{id_col}) AS {total_col},
                    COUNT(DISTINCT CASE WHEN {base_expr} THEN f.{id_col} END) AS base_matched,
                    COUNT(DISTINCT CASE WHEN {clean_expr} THEN f.{id_col} END) AS clean_matched,
                    COUNT(DISTINCT CASE WHEN {disc_expr} THEN f.{id_col} END) AS disc_matched,
                    COUNT(DISTINCT CASE WHEN NOT ({disc_expr}) THEN f.{id_col} END) AS unmatched
                FROM filtered_{entity_name} f
                JOIN dim_origin d ON {bit_is_set_join_sql(ENTITY_SCHEMA['origins'], 'f')}
                GROUP BY d.value
                ORDER BY {total_col} DESC
            """
            df = run_query(sql)
            if not df.empty:
                df["pct_base"] = (df["base_matched"] / df[total_col]).apply(
                    lambda x: f"{100*x:.1f}%" if pd.notnull(x) else "—"
                )
                df["pct_clean"] = (df["clean_matched"] / df[total_col]).apply(
                    lambda x: f"{100*x:.1f}%" if pd.notnull(x) else "—"
                )
                df["pct_disc"] = (df["disc_matched"] / df[total_col]).apply(
                    lambda x: f"{100*x:.1f}%" if pd.notnull(x) else "—"
                )
            return df

        @output
        @ui.render.data_frame
        def tbl_content_origin():
            return render.DataGrid(query_by_origin_func(TYPE_CONTENT, "content"), filters=False)

        @output
        @ui.render.data_frame
        def tbl_creator_origin():
            return render.DataGrid(query_by_origin_func(TYPE_CREATOR, "creator"), filters=False)

    # Build and return the App object
    return App(app_ui, server)