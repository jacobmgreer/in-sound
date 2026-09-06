library(shiny)
library(bslib)
library(DT)
library(duckdb)
library(dplyr)
library(scales)
library(htmltools)


# =============================================================================
# CONFIGURATION & PATHS
# =============================================================================

options(bslib.precompiled = TRUE)

`%||%` <- function(a, b) if (is.null(a)) b else a

# Detect the Shinylive/webR runtime. IN_SHINYLIVE is set by the shinylive
# runtime; Emscripten is webR's sysname. A local R session hits neither.
IS_SHINYLIVE <- nzchar(Sys.getenv("IN_SHINYLIVE")) ||
  identical(Sys.info()[["sysname"]], "Emscripten")

# Mutable data location. Local dev: "data" (repo root) or "../data" (dashboard/).
# In Shinylive the app folder is the working directory, so keep it relative;
# the parquets are downloaded into the webR virtual filesystem at runtime.
DATA_DIR <- if (IS_SHINYLIVE) "" else "../data"

# ── Sharded layout ────────────────────────────────────────────────────────────
# Content and creators now live in the SAME files — identical columns and
# bitmasks — discriminated by the `type` column:
#     type = 1  → content
#     type = 2  → creator
# The export is split into N_PARQUET_FILES shards named big_sight_0.parquet,
# big_sight_1.parquet, … Local dev reads them straight from DATA_DIR; Shinylive
# fetches each shard and unions them into a single DuckDB table (nc_data).
N_PARQUET_FILES <- 10
PARQUET_FILES <- function() {
  file.path(DATA_DIR, sprintf("big_sound_%d.parquet", seq_len(N_PARQUET_FILES) - 1))
}

# Where Shinylive fetches the 10 shards.
#
# DEFAULT: same-origin
# OVERRIDE: set NITRATE_PARQUET_BASE_URL
# Local dev ignores all of this entirely and reads DATA_DIR from disk.
# PARQUET_BASE_URL_ENV <- "NITRATE_PARQUET_BASE_URL"
# Point to your Hugging Face repository raw URL path
NITRATE_PARQUET_BASE_URL = "https://huggingface.co/jacobmgreer/in-sound")


TYPE_CONTENT <- 1L
TYPE_CREATOR <- 2L

FILTER_DEBOUNCE_MS <- 400

MACRO_DIR <- "macros"

# =============================================================================
# ENGINE STATE (populated by init_engine() once data is available)
# =============================================================================

DECADE_BITS       <- integer()
ORIGIN_BITS       <- integer()
GRAPH_BITS        <- integer()
SOURCE_ID_TO_NAME <- integer()
ENTITY_SCHEMA     <- NULL
FILTER_CHOICES    <- list(
  ok = FALSE, decades = numeric(),
  sources = setNames(integer(0), character(0)),
  source_ids = integer(),
  origins = character()
)
NC_CON <- NULL

# =============================================================================
# LOW-LEVEL HELPERS
# =============================================================================

sql_quote <- function(x) sprintf("'%s'", gsub("'", "''", as.character(x), fixed = TRUE))
sql_int_in <- function(v) paste0("(", paste(as.integer(v), collapse = ", "), ")")

open_con <- function() {
  con <- dbConnect(duckdb(), dbdir = ":memory:", read_only = FALSE)
  # DuckDB-Wasm is single-threaded by default (multithreading needs COOP/CEOP
  # headers, which GitHub Pages does not send), so only set this natively.
  if (!IS_SHINYLIVE) dbExecute(con, "PRAGMA threads=6;")
  dbExecute(con, "SET preserve_insertion_order = false;")
  con
}

safe_query <- function(con, sql, fallback = data.frame()) {
  tryCatch({
    df <- dbGetQuery(con, sql)
    for (col in names(df)) {
      if (inherits(df[[col]], "integer64")) df[[col]] <- as.numeric(df[[col]])
    }
    df
  }, error = function(e) {
    message("[nitrate-dash] ", conditionMessage(e))
    fallback
  })
}

run_sql <- function(con, sql) {
  safe_query(con, sql)
}

check_files <- function() {
  files <- PARQUET_FILES()
  basename(files[!file.exists(files)])
}

# =============================================================================
# SHINYLIVE DATA FETCH — pull the shard parquets into the webR filesystem
# =============================================================================

# Diagnostics from the last fetch attempt, surfaced in the UI on failure so a
# broken download shows its real cause instead of masquerading as
# "missing files / update DATA_DIR".
FETCH_LOG <- character()
log_fetch <- function(...) {
  msg <- paste0(...)
  FETCH_LOG[[length(FETCH_LOG) + 1]] <<- msg
  message("[nitrate-dash] ", msg)
}

fetch_headers <- function() {
  # Optional "Name: value" lines (one per line) for proxies needing an API key,
  # e.g. NITRATE_PARQUET_HEADERS="x-corsfix-key: cfx_12345678"
  h <- Sys.getenv("NITRATE_PARQUET_HEADERS", "")
  if (!nzchar(h)) return(NULL)
  lines <- trimws(strsplit(h, "\n")[[1]])
  lines <- lines[nzchar(lines)]
  if (!length(lines)) return(NULL)
  kv <- strsplit(lines, ":", fixed = TRUE)
  stats::setNames(
    vapply(kv, function(x) trimws(paste(x[-1], collapse = ":")), character(1)),
    vapply(kv, function(x) trimws(x[[1]]), character(1))
  )
}

is_parquet_file <- function(path) {
  if (!file.exists(path) || file.size(path) < 8) return(FALSE)
  con <- file(path, open = "rb")
  on.exit(close(con))
  identical(readBin(con, "raw", 4), charToRaw("PAR1"))
}

sniff_text <- function(path, n = 200) {
  con <- file(path, open = "rb")
  on.exit(close(con))
  gsub("[^[:print:]\t]", "?", rawToChar(readBin(con, "raw", n)))
}

fetch_one <- function(url, dest) {
  args <- list(url = url, destfile = dest, mode = "wb", quiet = TRUE)
  hdrs <- fetch_headers()
  if (!is.null(hdrs)) args$headers <- hdrs
  tryCatch({
    do.call(utils::download.file, args)
    TRUE
  }, error = function(e) {
    log_fetch("fetch errored — ", url, " — ", conditionMessage(e))
    FALSE
  })
}

fetch_binary <- function(url, dest) {
  if (!fetch_one(url, dest)) return(FALSE)

  if (!file.exists(dest) || file.size(dest) == 0) {
    log_fetch("fetch returned no data — ", url)
    unlink(dest)
    return(FALSE)
  }
  if (!is_parquet_file(dest)) {
    # A CORS proxy / 404 / "origin not allowed" response is usually a tiny
    # JSON or HTML body — log its beginning so the UI can show the cause.
    log_fetch("response was not parquet (", file.size(dest), " B) — ", url,
              " — first bytes: ", sniff_text(dest))
    unlink(dest)
    return(FALSE)
  }
  TRUE
}

# Resolve the shard base URL. Explicit env var wins; otherwise derive a
# same-origin "<page>/data" URL from the Shinylive page location so no CORS
# proxy is needed when the parquets ship on the same GitHub Pages site.
parquet_base_url <- function(session) {
  env <- Sys.getenv(PARQUET_BASE_URL_ENV, "")
  if (nzchar(env)) return(sub("/$", "", env))

  cd   <- session$clientData
  path <- cd$url_pathname %||% "/"
  path <- sub("index\\.html$", "", path)
  if (!grepl("/$", path)) path <- paste0(path, "/")

  port <- cd$url_port %||% ""
  host <- cd$url_hostname %||% ""
  if (nzchar(port) && !port %in% c("80", "443")) host <- paste0(host, ":", port)

  paste0(cd$url_protocol %||% "https:", "//", host, path, "data")
}

download_parquets <- function(base_url) {
  dir.create(DATA_DIR, showWarnings = FALSE, recursive = TRUE)
  ok <- TRUE
  for (f in PARQUET_FILES()) {
    if (file.exists(f)) next
    src <- paste0(base_url, "/", basename(f))
    log_fetch("fetching ", basename(f), "…")
    ok <- fetch_binary(src, f) && ok
  }
  ok
}

pick_col <- function(cols, candidates, label) {
  hit <- candidates[candidates %in% cols]
  if (!length(hit)) {
    stop(sprintf("Could not find %s. Tried: %s.", label, paste(candidates, collapse = ", ")), call. = FALSE)
  }
  hit[[1]]
}

parquet_schema <- function(con, path) {
  dbGetQuery(con, sprintf("DESCRIBE SELECT * FROM read_parquet(%s)", sql_quote(path)))
}

resolve_export_schema <- function(info, entity_label) {
  cols <- info$column_name
  source_col <- pick_col(cols, "source", sprintf("%s source column", entity_label))
  source_type <- info$column_type[info$column_name == source_col][[1]]

  list(
    id        = pick_col(cols, "big", sprintf("%s ID column", entity_label)),
    type      = pick_col(cols, "type", sprintf("%s type column", entity_label)),
    decades   = pick_col(cols, "decades", sprintf("%s decade bitmask", entity_label)),
    source    = source_col,
    source_is_int = grepl("int|serial", source_type, ignore.case = TRUE),
    origins   = pick_col(cols, "origins", sprintf("%s origin bitmask", entity_label)),
    graph_col = pick_col(cols, "comp", sprintf("%s graph bitmask", entity_label))
  )
}

# =============================================================================
# MACRO REGISTRATION & DYNAMIC BIT LOADING
# =============================================================================

register_all_macros <- function(con) {
  macro_files <- list.files(MACRO_DIR, pattern = "\\.sql$", full.names = TRUE)
  for (f in macro_files) {
    sql_text <- paste(readLines(f, warn = FALSE), collapse = "\n")
    if (nzchar(trimws(sql_text))) {
      dbExecute(con, sql_text)
    }
  }
}

load_bits_from_macro <- function(con, macro_name) {
  df <- dbGetQuery(con, sprintf("SELECT bit, value FROM %s() ORDER BY bit DESC", macro_name))
  setNames(as.integer(df$bit), as.character(df$value))
}

dim_rows_sql <- function(bits, numeric_values = FALSE) {
  vapply(seq_along(bits), function(i) {
    value <- names(bits)[[i]]
    value_sql <- if (numeric_values) as.character(value) else sql_quote(value)
    sprintf("(%d, %s)", as.integer(bits[[i]]), value_sql)
  }, character(1), USE.NAMES = FALSE) |> paste(collapse = ", ")
}

create_dimension_table <- function(con, table_name, bits, numeric_values = FALSE) {
  dbExecute(con, sprintf(
    "CREATE OR REPLACE TABLE %s AS SELECT bit, value FROM (VALUES %s) AS t(bit, value)",
    table_name, dim_rows_sql(bits, numeric_values)
  ))
}

register_dimension_tables <- function(con) {
  create_dimension_table(con, "dim_decade", DECADE_BITS, numeric_values = TRUE)
  create_dimension_table(con, "dim_origin", ORIGIN_BITS)

  source_rows <- vapply(seq_along(SOURCE_ID_TO_NAME), function(i) {
    sprintf("(%d, %s)", as.integer(SOURCE_ID_TO_NAME[[i]]), sql_quote(names(SOURCE_ID_TO_NAME)[[i]]))
  }, character(1), USE.NAMES = FALSE) |> paste(collapse = ", ")

  dbExecute(con, sprintf(
    "CREATE OR REPLACE TABLE dim_source AS SELECT bit, value FROM (VALUES %s) AS t(bit, value)",
    source_rows
  ))
}

bit_is_set_join_sql <- function(bitmask_col, alias = NULL) {
  col <- if (is.null(alias)) bitmask_col else paste0(alias, ".", bitmask_col)
  sprintf("(COALESCE(%s, 0)::BIGINT & (1::BIGINT << d.bit)) <> 0", col)
}

# =============================================================================
# STARTUP — filter dropdown choices (requires nc_data table built)
# =============================================================================

distinct_bitmap_values <- function(con, bitmask_col, dim_table) {
  sql <- sprintf(
    r"(
      SELECT DISTINCT d.value AS value
      FROM nc_data p
      JOIN %s d ON %s
      ORDER BY value
    )",
    dim_table, bit_is_set_join_sql(bitmask_col, "p")
  )
  safe_query(con, sql)
}

load_filter_choices <- function(con) {
  empty_sources <- setNames(integer(0), character(0))
  if (length(check_files()) > 0) {
    return(list(
      ok = FALSE, decades = numeric(), sources = empty_sources,
      source_ids = integer(), langs = character(), origins = character(), roles = character()
    ))
  }

  ddf <- distinct_bitmap_values(con, ENTITY_SCHEMA$decades, "dim_decade")
  odf <- distinct_bitmap_values(con, ENTITY_SCHEMA$origins, "dim_origin")

  source_col <- ENTITY_SCHEMA$source
  sdf_sql <- sprintf(
    r"(
      SELECT DISTINCT p.%s AS source_id,
             COALESCE(d.value, 'Source ' || CAST(p.%s AS VARCHAR)) AS source
      FROM nc_data p
      LEFT JOIN dim_source d ON p.%s = d.bit
      WHERE p.%s IS NOT NULL
      ORDER BY source
    )",
    source_col, source_col, source_col, source_col
  )
  sdf <- safe_query(con, sdf_sql)

  source_ids <- if (nrow(sdf) > 0) as.integer(sdf$source_id) else integer()
  source_labels <- if (nrow(sdf) > 0) as.character(sdf$source) else character()
  sources <- setNames(source_ids, source_labels)

  list(
    ok         = TRUE,
    decades    = if (nrow(ddf) > 0) sort(as.numeric(ddf$value)) else as.numeric(names(DECADE_BITS)),
    sources    = sources,
    source_ids = source_ids,
    origins    = if (nrow(odf) > 0) as.character(odf$value) else names(ORIGIN_BITS)
  )
}

build_decade_choices <- function() {
  ch <- c("All Decades" = "")
  if (length(FILTER_CHOICES$decades) > 0) {
    ch <- c(
      ch,
      setNames(as.character(FILTER_CHOICES$decades), paste0(FILTER_CHOICES$decades, "s"))
    )
  }
  ch
}

# =============================================================================
# ENGINE INITIALIZATION — called once data files are guaranteed present.
# Loads all shards into ONE DuckDB table (nc_data); content vs. creator views
# are separated at query time by the `type` column (1 = content, 2 = creator).
# =============================================================================

init_engine <- function() {
  schema_con <- open_con()
  register_all_macros(schema_con)

  DECADE_BITS       <<- load_bits_from_macro(schema_con, "get_decade_mapping")
  ORIGIN_BITS       <<- load_bits_from_macro(schema_con, "get_origin_mapping")
  GRAPH_BITS        <<- load_bits_from_macro(schema_con, "get_comp_mapping")
  SOURCE_ID_TO_NAME <<- load_bits_from_macro(schema_con, "get_source_mapping")

  ENTITY_SCHEMA <<- resolve_export_schema(parquet_schema(schema_con, PARQUET_FILES()[[1]]), "entity")

  dbDisconnect(schema_con, shutdown = TRUE)

  con <- open_con()
  register_dimension_tables(con)

  message("[nitrate-dash] Loading ", N_PARQUET_FILES, " parquet shards into DuckDB cache…")
  file_list <- paste(vapply(PARQUET_FILES(), sql_quote, character(1)), collapse = ", ")
  dbExecute(con, sprintf("CREATE TABLE nc_data AS SELECT * FROM read_parquet([%s])", file_list))
  message("[nitrate-dash] Unified table ready (content type=", TYPE_CONTENT,
          ", creator type=", TYPE_CREATOR, ").")

  FILTER_CHOICES <<- load_filter_choices(con)

  if (IS_SHINYLIVE) {
    # The parquet bytes are now fully materialized as a DuckDB table, so drop
    # the raw-file copies from the webR virtual filesystem to free memory
    # (DuckDB-Wasm is capped around 4 GB total).
    unlink(PARQUET_FILES())
  }

  NC_CON <<- con
  invisible(TRUE)
}

# =============================================================================
# BITMASK FILTER BUILDERS
# =============================================================================

selected_bitmask_sql <- function(values, bits) {
  values <- values[!is.na(values) & as.character(values) != ""]
  if (!length(values)) return(NULL)

  positions <- bits[as.character(values)]
  if (length(positions) == 0L || any(is.na(positions))) return(NULL)

  sprintf("%.0f", sum(2^as.numeric(positions)))
}

build_bitmask_clause <- function(values, bitmask_col, bits) {
  mask_sql <- selected_bitmask_sql(values, bits)
  if (is.null(mask_sql)) return("")
  sprintf("AND ((COALESCE(%s, 0)::BIGINT & %s::BIGINT) <> 0)", bitmask_col, mask_sql)
}

get_decade_values <- function(inp) {
  if (inp$filter_decade_from == "" || inp$filter_decade_to == "") {
    return(FILTER_CHOICES$decades)
  }

  from <- suppressWarnings(as.integer(inp$filter_decade_from))
  to   <- suppressWarnings(as.integer(inp$filter_decade_to))
  if (is.na(from) || is.na(to)) return(FILTER_CHOICES$decades)
  if (from > to) { tmp <- from; from <- to; to <- tmp }
  seq(from, to, by = 10)
}

get_decade_values_sql <- function(inp) {
  paste(sprintf("(%d)", get_decade_values(inp)), collapse = ", ")
}

build_decade_clause <- function(inp) {
  build_bitmask_clause(get_decade_values(inp), ENTITY_SCHEMA$decades, DECADE_BITS)
}

build_origin_clause <- function(inp) { build_bitmask_clause(inp$filter_origins, ENTITY_SCHEMA$origins, ORIGIN_BITS) }

build_source_clause <- function(inp) {
  selected <- inp$filter_sources
  if (!length(selected)) return("AND 1=0")

  selected <- as.integer(selected)
  selected <- selected[!is.na(selected)]
  if (!length(selected)) return("AND 1=0")
  if (length(selected) >= length(FILTER_CHOICES$source_ids)) return("")

  sprintf("AND %s IN %s", ENTITY_SCHEMA$source, sql_int_in(selected))
}

# =============================================================================
# CTE BUILDERS — one table, separated by `type`
# =============================================================================

entity_cte <- function(cte_name, type_value, clauses) {
  cols <- unique(c(
    ENTITY_SCHEMA$id, ENTITY_SCHEMA$type, ENTITY_SCHEMA$source,
    ENTITY_SCHEMA$decades, ENTITY_SCHEMA$langs,
    ENTITY_SCHEMA$origins, ENTITY_SCHEMA$roles, ENTITY_SCHEMA$graph_col
  ))
  sprintf(
    "filtered_%s AS (SELECT %s FROM nc_data WHERE %s = %d %s)",
    cte_name, paste(cols, collapse = ", "), ENTITY_SCHEMA$type,
    as.integer(type_value), paste(clauses, collapse = " ")
  )
}

shared_filter_clauses <- function(inp) {
  c(
    build_source_clause(inp),
    build_decade_clause(inp),
    build_origin_clause(inp)
  )
}

build_content_cte <- function(inp) {
  entity_cte("content", TYPE_CONTENT, shared_filter_clauses(inp))
}

build_creator_cte <- function(inp) {
  entity_cte("creator", TYPE_CREATOR, shared_filter_clauses(inp))
}

# =============================================================================
# QUERY FUNCTIONS
# =============================================================================

graph_match_expr <- function(graph_name, alias = NULL) {
  prefix <- if (is.null(alias)) "" else paste0(alias, ".")
  sprintf(
    "((COALESCE(%s%s, 0)::BIGINT & (1::BIGINT << %d)) <> 0)",
    prefix, ENTITY_SCHEMA$graph_col, GRAPH_BITS[[graph_name]]
  )
}

query_overview <- function(con, inp) {
  base_expr  <- graph_match_expr("base")
  clean_expr <- graph_match_expr("clean")
  disc_expr  <- graph_match_expr("discovery")
  id_col     <- ENTITY_SCHEMA$id
  type_col   <- ENTITY_SCHEMA$type

  sql <- sprintf(
    r"(
      WITH filtered AS (
        SELECT %s, %s, %s
        FROM nc_data
        WHERE 1=1 %s
      )
      SELECT
        COUNT(DISTINCT %s) FILTER (WHERE %s = 1) AS total_content,
        COUNT(DISTINCT %s) FILTER (WHERE %s = 1 AND %s) AS base_content,
        COUNT(DISTINCT %s) FILTER (WHERE %s = 1 AND %s) AS clean_content,
        COUNT(DISTINCT %s) FILTER (WHERE %s = 1 AND %s) AS disc_content,
        COUNT(DISTINCT %s) FILTER (WHERE %s = 2) AS total_creator,
        COUNT(DISTINCT %s) FILTER (WHERE %s = 2 AND %s) AS base_creator,
        COUNT(DISTINCT %s) FILTER (WHERE %s = 2 AND %s) AS clean_creator,
        COUNT(DISTINCT %s) FILTER (WHERE %s = 2 AND %s) AS disc_creator
      FROM filtered
    )",
    id_col, type_col, ENTITY_SCHEMA$graph_col,
    paste(shared_filter_clauses(inp), collapse = " "),
    id_col, type_col,
    id_col, type_col, base_expr,
    id_col, type_col, clean_expr,
    id_col, type_col, disc_expr,
    id_col, type_col,
    id_col, type_col, base_expr,
    id_col, type_col, clean_expr,
    id_col, type_col, disc_expr
  )

  run_sql(con, sql)
}

query_by_bit_dimension <- function(con, inp, entity, dim_table, bitmask_col, order_by) {
  id_col <- ENTITY_SCHEMA$id
  total_col <- paste0("total_", entity)
  table <- paste0("filtered_", entity)
  base_expr <- graph_match_expr("base", "f")
  clean_expr <- graph_match_expr("clean", "f")
  disc_expr <- graph_match_expr("discovery", "f")
  cte <- if (entity == "content") build_content_cte(inp) else build_creator_cte(inp)

  sql <- sprintf(
    r"(
      WITH %s
      SELECT
        d.value AS grouping,
        COUNT(DISTINCT f.%s) AS %s,
        COUNT(DISTINCT CASE WHEN %s THEN f.%s END) AS base_matched,
        COUNT(DISTINCT CASE WHEN %s THEN f.%s END) AS clean_matched,
        COUNT(DISTINCT CASE WHEN %s THEN f.%s END) AS disc_matched,
        COUNT(DISTINCT CASE WHEN NOT (%s) THEN f.%s END) AS unmatched
      FROM %s f
      JOIN %s d ON %s
      GROUP BY d.value
      ORDER BY %s
    )",
    cte,
    id_col, total_col,
    base_expr, id_col,
    clean_expr, id_col,
    disc_expr, id_col,
    disc_expr, id_col,
    table,
    dim_table, bit_is_set_join_sql(bitmask_col, "f"),
    order_by
  )

  run_sql(con, sql)
}

query_content_by_origin <- function(con, inp) query_by_bit_dimension(con, inp, "content", "dim_origin", ENTITY_SCHEMA$origins, "total_content DESC")
query_creator_by_origin <- function(con, inp) query_by_bit_dimension(con, inp, "creator", "dim_origin", ENTITY_SCHEMA$origins, "total_creator DESC")

query_by_decade <- function(con, inp, entity) {
  id_col <- ENTITY_SCHEMA$id
  total_col <- paste0("total_", entity)
  table <- paste0("filtered_", entity)
  base_expr <- graph_match_expr("base", "f")
  clean_expr <- graph_match_expr("clean", "f")
  disc_expr <- graph_match_expr("discovery", "f")
  cte <- if (entity == "content") build_content_cte(inp) else build_creator_cte(inp)
  dec_values <- get_decade_values_sql(inp)

  sql <- sprintf(
    r"(
      WITH %s,
           all_dec AS (SELECT decade FROM (VALUES %s) t(decade)),
           dec_exp AS (
             SELECT
               d.value AS decade,
               f.%s AS entity_id,
               %s AS is_base,
               %s AS is_clean,
               %s AS is_discovery
             FROM %s f
             JOIN dim_decade d ON %s
           )
      SELECT
        ad.decade,
        COUNT(DISTINCT de.entity_id) AS %s,
        COUNT(DISTINCT CASE WHEN de.is_base THEN de.entity_id END) AS base_matched,
        COUNT(DISTINCT CASE WHEN de.is_clean THEN de.entity_id END) AS clean_matched,
        COUNT(DISTINCT CASE WHEN de.is_discovery THEN de.entity_id END) AS disc_matched,
        COUNT(DISTINCT CASE WHEN NOT de.is_discovery THEN de.entity_id END) AS unmatched
      FROM all_dec ad
      LEFT JOIN dec_exp de USING (decade)
      GROUP BY ad.decade
      ORDER BY ad.decade
    )",
    cte,
    dec_values,
    id_col,
    base_expr, clean_expr, disc_expr,
    table, bit_is_set_join_sql(ENTITY_SCHEMA$decades, "f"),
    total_col
  )

  run_sql(con, sql)
}

query_content_by_decade <- function(con, inp) query_by_decade(con, inp, "content")
query_creator_by_decade <- function(con, inp) query_by_decade(con, inp, "creator")

query_by_source <- function(con, inp, entity) {
  id_col <- ENTITY_SCHEMA$id
  total_col <- paste0("total_", entity)
  table <- paste0("filtered_", entity)
  base_expr <- graph_match_expr("base", "f")
  clean_expr <- graph_match_expr("clean", "f")
  disc_expr <- graph_match_expr("discovery", "f")
  cte <- if (entity == "content") build_content_cte(inp) else build_creator_cte(inp)

  source_ref <- paste0("f.", ENTITY_SCHEMA$source)
  source_select <- sprintf("COALESCE(s.value, 'Source ' || CAST(%s AS VARCHAR)) AS source", source_ref)
  source_join <- sprintf("LEFT JOIN dim_source s ON %s = s.bit", source_ref)
  group_by_expr <- sprintf("COALESCE(s.value, 'Source ' || CAST(%s AS VARCHAR))", source_ref)

  sql <- sprintf(
    r"(
      WITH %s
      SELECT
        %s,
        COUNT(DISTINCT f.%s) AS %s,
        COUNT(DISTINCT CASE WHEN %s THEN f.%s END) AS base_matched,
        COUNT(DISTINCT CASE WHEN %s THEN f.%s END) AS clean_matched,
        COUNT(DISTINCT CASE WHEN %s THEN f.%s END) AS disc_matched,
        COUNT(DISTINCT CASE WHEN NOT (%s) THEN f.%s END) AS unmatched
      FROM %s f
      %s
      GROUP BY %s
      ORDER BY %s DESC
    )",
    cte,
    source_select,
    id_col, total_col,
    base_expr, id_col,
    clean_expr, id_col,
    disc_expr, id_col,
    disc_expr, id_col,
    table,
    source_join,
    group_by_expr,
    total_col
  )

  run_sql(con, sql)
}

query_content_by_source <- function(con, inp) query_by_source(con, inp, "content")
query_creator_by_source <- function(con, inp) query_by_source(con, inp, "creator")

pct_fmt <- function(num, denom) {
  ifelse(is.na(denom) | denom == 0, "—", sprintf("%.1f%%", 100 * num / denom))
}

# ── Global DT Formatter ───────────────────────────────────────────────────────
fmt_dt <- function(df, first_col_name = "Grouping", caption = NULL) {
  if (nrow(df) == 0) return(DT::datatable(df))

  sketch <- htmltools::withTags(table(
    class = "display",
    thead(
      tr(
        th(rowspan = 2, first_col_name),
        th(rowspan = 2, "Total", style = "text-align: right;"),
        th(colspan = 2, "Base", style = "text-align: center;"),
        th(colspan = 2, "Clean", style = "text-align: center;"),
        th(colspan = 2, "Discovery", style = "text-align: center;"),
        th(rowspan = 2, "Unmatched", style = "text-align: right;")
      ),
      tr(
        lapply(c("%", "Matched", "%", "Matched", "%", "Matched"), function(x) {
          th(x, style = "text-align: right;")
        })
      )
    )
  ))

  single_page <- nrow(df) <= 30

  dt_obj <- DT::datatable(
    df,
    caption = caption,
    container = sketch,
    rownames = FALSE,
    class = "compact stripe hover",
    options = list(
      pageLength = 30,
      scrollX = TRUE,
      paging = !single_page,
      searching = !single_page,
      info = !single_page,
      dom = (if (single_page) "t" else "frtip"),
      columnDefs = list(
        list(className = "dt-body-right", targets = c(1, 2, 3, 4, 5, 6, 7, 8)),
        list(className = "dt-body-left", targets = c(0))
      )
    )
  )

  cols_to_format <- c("total_content", "total_creator",
                      "base_matched", "clean_matched", "disc_matched",
                      "unmatched")
  target_cols <- intersect(cols_to_format, colnames(df))
  if (length(target_cols) > 0) {
    dt_obj <- DT::formatCurrency(dt_obj, columns = target_cols, currency = "", digits = 0)
  }

  dt_obj
}

ov_card <- function(swatch_color, title, subtitle, total_val,
                    matched_n = NULL, matched_denom = NULL,
                    unmatched_n = NULL, extra_note = NULL,
                    muted = FALSE) {
  opacity_swatch <- if (muted) "opacity:0.35;" else ""
  div(class = "card h-100",
    div(class = "card-body py-2 px-3",
      div(class = "d-flex align-items-center mb-1",
        div(style = sprintf(
          "width:12px;height:12px;border-radius:2px;background:%s;margin-right:6px;flex-shrink:0;%s",
          swatch_color, opacity_swatch)),
        (if (muted) strong(class = "text-muted", title) else strong(title))
      ),
      div(class = "text-muted", style = "font-size:0.75rem; margin-bottom:4px;", subtitle),
      (if (muted)
        h4(class = "mb-1 text-muted", "—")
      else
        h4(class = "mb-1", pct_fmt(matched_n, matched_denom))),
      (if (!is.null(matched_n) && !muted)
        div(style = "font-size:0.82rem;", comma(matched_n), " connected") else NULL),
      (if (!is.null(unmatched_n) && !muted)
        div(style = "font-size:0.82rem;color:#000000;", comma(unmatched_n), " unmatched") else NULL),
      (if (!is.null(extra_note) && !muted)
        div(class = "text-muted mt-1", style = "font-size:0.75rem;", extra_note) else NULL)
    )
  )
}

# =============================================================================
# UI
# =============================================================================

# The sidebar body is rendered dynamically (uiOutput) because the filter
# choices can only be known AFTER the parquet files are available at runtime.
sidebar_panel <- sidebar(
  width = 290,
  uiOutput("sidebar_filters")
)

navbar_css <- paste(
  ".navbar {",
  "background-color: #c3cdd7 !important;",
  "border: none !important;",
  "border-bottom: 2px solid !important;",
  "box-shadow: none !important;",
  "padding: 20px;",
  "}",
  sep = "\n")

ui <- page_navbar(
  title    = "Connected Components",
  nav_spacer(),
  theme    = bs_theme(version = 5, bootswatch = "brite") |>
    bs_add_rules(navbar_css),
  nav_spacer(),
  fillable = FALSE,
  header   = tagList(
    tags$head(
      tags$script(src = "https://cdn.jsdelivr.net/npm/d3@7.9.0/dist/d3.min.js")
    ),
  ),
  sidebar = sidebar_panel,

  nav_panel("By Source",
    uiOutput("config_warning"),

    h6(class = "text-muted", style = "font-size:2em; margin:4px 0 0 0;", "Content"),
    hr(style = "margin:16px;border:0;"),
    uiOutput("content_overview_metrics"),
    uiOutput("by_content_source_header"),
    DTOutput("tbl_by_content_source"),
    hr(style = "margin:16px;border:0;"),

    h6(class = "text-muted", style = "font-size:2em; margin:4px 0 0 0;", "Creator"),
    hr(style = "margin:16px;border:0;"),
    uiOutput("creator_overview_metrics"),
    uiOutput("by_creator_source_header"),
    DTOutput("tbl_by_creator_source")
  ),

  nav_panel("By Decade",
    h6(class = "text-muted", style = "font-size:2em; margin:4px 0 0 0;", "Content"),
    hr(style = "margin:16px;border:0;"),
    h5("Content Records by Decade"),
    DTOutput("tbl_content_decade"),

    h6(class = "text-muted", style = "font-size:2em; margin:20px 0 0 0;", "Creator"),
    hr(style = "margin:16px;border:0;"),
    h5("Creator Records by Associated Content Decade(s)"),
    DTOutput("tbl_creator_decade")
  ),

  nav_panel("By Origin",
    h6(class = "text-muted", style = "font-size:2em; margin:4px 0 0 0;", "Content"),
    hr(style = "margin:16px;border:0;"),
    h5("Content Records by Origin(s)"),
    DTOutput("tbl_content_origin"),

    h6(class = "text-muted", style = "font-size:2em; margin:20px 0 0 0;", "Creator"),
    hr(style = "margin:16px;border:0;"),
    h5("Creator Records by Associated Content Origin(s)"),
    DTOutput("tbl_creator_origin")
  )
)

# =============================================================================
# SERVER
# =============================================================================

server <- function(input, output, session) {

  # data_status: "pending" -> "ok" | "failed"
  data_status <- reactiveVal("pending")

  # One-time startup: ensure the 10 shard files exist (downloading them from
  # the deployed site when running under Shinylive/webR), then build the engine.
  observe({
    if (length(check_files()) > 0 && IS_SHINYLIVE) {
      # Files are not in the webR virtual filesystem yet; pull the missing
      # shards from the same origin as the app (or NITRATE_PARQUET_BASE_URL),
      # then fall through to engine init below.
      message("[nitrate-dash] parquet shards not found locally; fetching")
      tryCatch(
        download_parquets(parquet_base_url(session)),
        error = function(e) message("[nitrate-dash] fetch error: ", conditionMessage(e))
      )
    }

    if (length(check_files()) == 0) {
      tryCatch({
        init_engine()
        data_status("ok")
      }, error = function(e) {
        message("[nitrate-dash] engine init failed: ", conditionMessage(e))
        data_status("failed")
      })
    } else {
      data_status("failed")
    }
  })

  # Sidebar filters can only be built after FILTER_CHOICES is populated.
  output$sidebar_filters <- renderUI({
    req(data_status() == "ok")
    tagList(
      h6("Cross-Filters"),
      hr(style = "margin:6px 0"),
      selectInput("filter_decade_from", "From Decade",
                  choices = build_decade_choices(), selected = ""),
      selectInput("filter_decade_to", "To Decade",
                  choices = build_decade_choices(), selected = ""),
      selectizeInput("filter_origins", "Origin(s)",
                     choices = FILTER_CHOICES$origins, selected = NULL, multiple = TRUE,
                     options = list(plugins = list("remove_button"), placeholder = "All")),
      hr(style = "margin:6px 0"),
      h6("Source"),
      checkboxGroupInput("filter_sources", label = NULL,
                         choices = FILTER_CHOICES$sources,
                         selected = FILTER_CHOICES$source_ids)
    )
  })

  output$config_warning <- renderUI({
    status <- data_status()
    if (status == "pending") {
      return(div(class = "alert alert-info",
        tags$strong("Loading parquet data… "), "this only happens once per visit."))
    }
    if (status == "ok") return(NULL)

    if (IS_SHINYLIVE && length(FETCH_LOG) > 0) {
      base <- parquet_base_url(session)
      targets <- paste0(base, "/", basename(PARQUET_FILES()))
      return(div(class = "alert alert-danger",
        tags$strong("Parquet download failed in the browser."), tags$br(),
        "Attempted: ", tags$ul(lapply(targets, function(u) tags$li(tags$code(u)))),
        tags$strong("What the server said:"),
        tags$ul(lapply(utils::tail(FETCH_LOG, 6), tags$li)),
        tags$hr(),
        "Most likely fixes: (1) confirm all ", N_PARQUET_FILES,
        " shards (big_sight_0.parquet … big_sight_", N_PARQUET_FILES - 1,
        ".parquet) exist under ", tags$code(base)
      ))
    }

    missing <- check_files()
    div(class = "alert alert-warning",
      tags$strong("Missing parquet files: "),
      tags$code(paste(missing, collapse = ", ")), tags$br(),
      sprintf("Update DATA_DIR / N_PARQUET_FILES at the top of app.R (current DATA_DIR: '%s')", DATA_DIR))
  })

  resolve_input <- function() {
    list(
      filter_decade_from = input$filter_decade_from %||% "",
      filter_decade_to   = input$filter_decade_to %||% "",
      filter_origins     = input$filter_origins %||% NULL,
      filter_sources     = input$filter_sources %||% FILTER_CHOICES$source_ids
    )
  }

  inp_snap <- debounce(reactive({ resolve_input() }), FILTER_DEBOUNCE_MS)

  run_query <- function(qfn) {
    req(data_status() == "ok", !is.null(NC_CON))
    inp <- inp_snap()
    qfn(NC_CON, inp)
  }

  overview_data <- reactive({ run_query(query_overview) })

  output$content_overview_metrics <- renderUI({
    df <- overview_data()
    if (!nrow(df) || all(is.na(unlist(df))))
      return(p(class = "text-muted", "No data for the selected filters."))
    r <- df[1, ]
    fluidRow(
      column(4, ov_card(
        "#db3f7d", "Base", "Source Graph",
        total_val = r$total_content,
        matched_n = r$base_content,
        matched_denom = r$total_content,
        unmatched_n = r$total_content - r$base_content
      )),
      column(4, ov_card(
        "#0ec56a", "Clean", "Cleaned Graph",
        total_val = r$total_content,
        matched_n = r$clean_content,
        matched_denom = r$total_content,
        unmatched_n = r$total_content - r$clean_content
      )),
      column(4, ov_card(
        "#3885d3", "Discovery", "Proposed Graph",
        total_val = r$total_content,
        matched_n = r$disc_content,
        matched_denom = r$total_content,
        unmatched_n = r$total_content - r$disc_content
      ))
    )
  })

  output$creator_overview_metrics <- renderUI({
    df <- overview_data()
    if (!nrow(df) || all(is.na(unlist(df))))
      return(p(class = "text-muted", "No data for the selected filters."))
    r <- df[1, ]
    fluidRow(
      column(4, ov_card(
        "#db3f7d", "Base", "Source Graph",
        total_val = r$total_creator,
        matched_n = r$base_creator,
        matched_denom = r$total_creator,
        unmatched_n = r$total_creator - r$base_creator
      )),
      column(4, ov_card(
        "#0ec56a", "Clean", "Cleaned Graph",
        total_val = r$total_creator,
        matched_n = r$clean_creator,
        matched_denom = r$total_creator,
        unmatched_n = r$total_creator - r$clean_creator
      )),
      column(4, ov_card(
        "#3885d3", "Discovery", "Proposed Graph",
        total_val = r$total_creator,
        matched_n = r$disc_creator,
        matched_denom = r$total_creator,
        unmatched_n = r$total_creator - r$disc_creator
      ))
    )
  })

  by_creator_source_data <- reactive({ run_query(query_creator_by_source) })

  output$by_creator_source_header <- renderUI({
    df <- by_creator_source_data()
    if (!nrow(df)) return(NULL)
    tagList(br(), h5("By Creator ID Source"))
  })

  output$tbl_by_creator_source <- renderDT({
    df <- by_creator_source_data()
    if (!nrow(df)) return(fmt_dt(data.frame()))

    df |>
      mutate(
        pct_base  = pct_fmt(base_matched,  total_creator),
        pct_clean = pct_fmt(clean_matched, total_creator),
        pct_disc  = pct_fmt(disc_matched,  total_creator)
      ) |>
      select(source, total_creator,
             pct_base, base_matched,
             pct_clean, clean_matched,
             pct_disc, disc_matched,
             unmatched) |>
      fmt_dt(first_col_name = "Source")
  })

  by_content_source_data <- reactive({ run_query(query_content_by_source) })

  output$by_content_source_header <- renderUI({
    df <- by_content_source_data()
    if (!nrow(df)) return(NULL)
    tagList(br(), h5("By Content ID Source"))
  })

  output$tbl_by_content_source <- renderDT({
    df <- by_content_source_data()
    if (!nrow(df)) return(fmt_dt(data.frame()))

    df |>
      mutate(
        pct_base  = pct_fmt(base_matched,  total_content),
        pct_clean = pct_fmt(clean_matched, total_content),
        pct_disc  = pct_fmt(disc_matched,  total_content)
      ) |>
      select(source, total_content,
             pct_base, base_matched,
             pct_clean, clean_matched,
             pct_disc, disc_matched,
             unmatched) |>
      fmt_dt(first_col_name = "Source")
  })

  content_decade_r  <- reactive({ run_query(query_content_by_decade) })
  content_origin_r  <- reactive({ run_query(query_content_by_origin) })

  creator_decade_r  <- reactive({ run_query(query_creator_by_decade) })
  creator_origin_r  <- reactive({ run_query(query_creator_by_origin) })

  fmt_content_dim <- function(df, group_col, header_label) {
    df |>
      filter(total_content > 0) |>
      mutate(
        pct_base  = pct_fmt(base_matched,  total_content),
        pct_clean = pct_fmt(clean_matched, total_content),
        pct_disc  = pct_fmt(disc_matched,  total_content)
      ) |>
      select({{ group_col }}, total_content,
             pct_base, base_matched,
             pct_clean, clean_matched,
             pct_disc, disc_matched,
             unmatched) |>
      fmt_dt(first_col_name = header_label)
  }

  fmt_creator_dim <- function(df, group_col, header_label) {
    df |>
      filter(total_creator > 0) |>
      mutate(
        pct_base  = pct_fmt(base_matched,  total_creator),
        pct_clean = pct_fmt(clean_matched, total_creator),
        pct_disc  = pct_fmt(disc_matched,  total_creator)
      ) |>
      select({{ group_col }}, total_creator,
             pct_base, base_matched,
             pct_clean, clean_matched,
             pct_disc, disc_matched,
             unmatched) |>
      fmt_dt(first_col_name = header_label)
  }

  output$tbl_content_decade <- renderDT({
    df <- content_decade_r()
    if (!nrow(df)) return(fmt_dt(data.frame()))
    df <- df |> mutate(decade_label = sprintf("%ds", decade))
    fmt_content_dim(df, decade_label, "Decade")
  })

  output$tbl_content_origin <- renderDT({
    df <- content_origin_r()
    if (!nrow(df)) return(fmt_dt(data.frame()))
    df <- df |> rename(origin = grouping)
    fmt_content_dim(df, origin, "Origin")
  })

  output$tbl_creator_decade <- renderDT({
    df <- creator_decade_r()
    if (!nrow(df)) return(fmt_dt(data.frame()))
    df <- df |> mutate(decade_label = sprintf("%ds", decade))
    fmt_creator_dim(df, decade_label, "Decade")
  })

  output$tbl_creator_origin <- renderDT({
    df <- creator_origin_r()
    if (!nrow(df)) return(fmt_dt(data.frame()))
    df <- df |> rename(origin = grouping)
    fmt_creator_dim(df, origin, "Origin")
  })

}

# =============================================================================
# RUN
# =============================================================================
shinyApp(ui, server)
